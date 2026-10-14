/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.gaussdb.internal.wal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.huawei.gaussdb.jdbc.copy.CopyDual;
import com.huawei.gaussdb.jdbc.core.BaseConnection;
import com.huawei.gaussdb.jdbc.core.QueryExecutor;
import com.huawei.gaussdb.jdbc.core.ReplicationProtocol;
import com.huawei.gaussdb.jdbc.core.tenant.TenantConnection;
import com.huawei.gaussdb.jdbc.core.v3.replication.MultiPrimaryReplicationProtocol;
import com.huawei.gaussdb.jdbc.core.v3.replication.V3ReplicationProtocol;
import com.huawei.gaussdb.jdbc.replication.MultiPrimaryReplicationStream;
import com.huawei.gaussdb.jdbc.replication.PGReplicationConnectionImpl;
import com.huawei.gaussdb.jdbc.replication.ReplicationType;
import com.huawei.gaussdb.jdbc.replication.TenantReplicationConnectionImpl;
import com.huawei.gaussdb.jdbc.replication.fluent.logical.LogicalReplicationOptions;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

class HcsMultiPrimaryProtocolTest {
    private static final String FIRST = "0/64,0/2,0/1000";

    @Test
    @SuppressWarnings("unchecked")
    void tenantStartupOnlyIncludesReplicationWhenExplicitlyRequestedWithCompatibleAssumption()
            throws Exception {
        java.lang.reflect.Method parameters =
                com.huawei.gaussdb.jdbc.core.v3.ConnectionFactoryImpl.class.getDeclaredMethod(
                        "getParametersForStartup", String.class, String.class, Properties.class);
        parameters.setAccessible(true);
        for (String assumedVersion : new String[] {null, "9.2.4", "9.4"}) {
            for (boolean replicationRequested : new boolean[] {false, true}) {
                Properties properties = new Properties();
                if (assumedVersion != null) {
                    properties.setProperty("assumeMinServerVersion", assumedVersion);
                }
                if (replicationRequested) {
                    properties.setProperty("replication", "database");
                }
                List<String[]> startup =
                        (List<String[]>)
                                parameters.invoke(
                                        new com.huawei.gaussdb.jdbc.core.tenant
                                                .TenantConnectionFactoryImpl(),
                                        "probe_user",
                                        "probe_database",
                                        properties);
                List<String> replicationValues = new ArrayList<>();
                for (String[] entry : startup) {
                    if ("replication".equals(entry[0])) replicationValues.add(entry[1]);
                }
                if (replicationRequested && "9.4".equals(assumedVersion)) {
                    assertThat(replicationValues).containsExactly("database");
                } else {
                    assertThat(replicationValues).isEmpty();
                }
            }
        }
    }

    @Test
    void real507TenantProtocolInheritsStartLogicalAndSendsStartReplication() throws Exception {
        QueryExecutor executor = mock(QueryExecutor.class);
        TenantConnection tenant = mock(TenantConnection.class);
        when(tenant.initReplicationProtocol(executor)).thenCallRealMethod();
        ReplicationProtocol protocol = tenant.initReplicationProtocol(executor);
        assertThat(protocol).isInstanceOf(MultiPrimaryReplicationProtocol.class);
        assertThat(
                        protocol.getClass()
                                .getMethod("startLogical", LogicalReplicationOptions.class)
                                .getDeclaringClass())
                .isEqualTo(V3ReplicationProtocol.class);

        BaseConnection base = tenantConnection(true);
        when(base.getReplicationProtocol()).thenReturn(protocol);
        CopyDual copy = mock(CopyDual.class);
        when(executor.startCopy(any(String.class), org.mockito.ArgumentMatchers.eq(true)))
                .thenReturn(copy);
        com.huawei.gaussdb.jdbc.replication.PGReplicationStream stream =
                base.getReplicationAPI()
                        .replicationStream()
                        .logical()
                        .withSlotName("test")
                        .withStartPosition(ReplicationPosition.requireMultiPrimary(FIRST))
                        .withStatusInterval(0, java.util.concurrent.TimeUnit.SECONDS)
                        .start();
        try {
            verify(executor).startCopy("START_REPLICATION SLOT test LOGICAL " + FIRST, true);
            assertThat(stream).isInstanceOf(MultiPrimaryReplicationStream.class);
        } finally {
            stream.close();
        }
    }

    private static void set(Object target, String name, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static Object get(Object target, String name) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return f.get(target);
    }

    private static class Session {
        final Deque<byte[]> packets = new ArrayDeque<>();
        final List<byte[]> replies = new ArrayList<>();
        final MultiPrimaryReplicationStream nativeStream;

        Session(String start) {
            CopyDual transport =
                    (CopyDual)
                            Proxy.newProxyInstance(
                                    CopyDual.class.getClassLoader(),
                                    new Class[] {CopyDual.class},
                                    (p, m, a) -> {
                                        switch (m.getName()) {
                                            case "isActive":
                                                return true;
                                            case "readFromCopy":
                                                return packets.poll();
                                            case "writeToCopy":
                                                replies.add(
                                                        Arrays.copyOfRange(
                                                                (byte[]) a[0],
                                                                (int) a[1],
                                                                (int) a[1] + (int) a[2]));
                                                return null;
                                            case "flushCopy":
                                                return null;
                                            default:
                                                if (m.getReturnType() == int.class) {
                                                    return 0;
                                                }
                                                if (m.getReturnType() == long.class) {
                                                    return 0L;
                                                }
                                                return null;
                                        }
                                    });
            nativeStream =
                    new MultiPrimaryReplicationStream(
                            transport,
                            ReplicationPosition.requireMultiPrimary(start),
                            1000000,
                            ReplicationType.LOGICAL);
        }

        void packet(String position, String payload) {
            long[] triple = ReplicationPosition.requireMultiPrimary(position).asLongArray();
            byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
            packets.add(
                    ByteBuffer.allocate(33 + bytes.length)
                            .put((byte) 'w')
                            .putLong(triple[0])
                            .putLong(triple[1])
                            .putLong(triple[2])
                            .putLong(1234)
                            .put(bytes)
                            .array());
        }

        long[] flushed() throws Exception {
            nativeStream.forceUpdateStatus();
            byte[] bytes = replies.get(replies.size() - 1);
            assertThat(bytes).hasSize(113);
            ByteBuffer b = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            b.position(33);
            return new long[] {b.getLong(), b.getLong(), b.getLong()};
        }

        WalReplicationStream connector() throws Exception {
            WalReplicationStream wal =
                    new WalReplicationStream(
                            null,
                            "jdbc:hcs-test:test",
                            "user",
                            "db-secret",
                            "test",
                            "mppdb_decoding",
                            2,
                            "j",
                            false,
                            100);
            wal.setHcsMultiPrimary(true, new Properties());
            set(wal, "replicationStream", nativeStream);
            set(wal, "useReplicationApi", true);
            set(wal, "running", true);
            set(wal, "lastLsn", ReplicationPosition.MULTI_PRIMARY_START);
            return wal;
        }
    }

    private static BaseConnection tenantConnection(boolean slotExists) throws Exception {
        BaseConnection base = mock(BaseConnection.class);
        when(base.tacGetQueryExecutor())
                .thenReturn(mock(com.huawei.gaussdb.jdbc.core.QueryExecutor.class));
        TenantReplicationConnectionImpl tenantApi = new TenantReplicationConnectionImpl(base);
        when(base.getReplicationAPI()).thenReturn(tenantApi);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet result = mock(ResultSet.class);
        when(base.prepareStatement("SELECT 1 FROM pg_get_replication_slots() WHERE slot_name = ?"))
                .thenReturn(statement);
        when(statement.executeQuery()).thenReturn(result);
        when(result.next()).thenReturn(slotExists);
        return base;
    }

    private static WalReplicationStream connector(Connection connection, boolean hcs) {
        WalReplicationStream wal =
                new WalReplicationStream(
                        connection,
                        "jdbc:hcs-test:test",
                        "user",
                        "db-secret",
                        "test",
                        "mppdb_decoding",
                        2,
                        "j",
                        false,
                        100);
        Properties properties = new Properties();
        properties.setProperty("connectMode", "auth");
        properties.setProperty("tenantName", "tenant");
        properties.setProperty("tenantPassword", "tenant-secret");
        wal.setHcsMultiPrimary(hcs, properties);
        return wal;
    }

    private static class RecordingDriver implements Driver {
        private final Connection connection;
        String url;
        Properties properties;

        RecordingDriver(Connection connection) {
            this.connection = connection;
        }

        public boolean acceptsURL(String url) {
            return url.startsWith("jdbc:hcs-test:");
        }

        public Connection connect(String url, Properties properties) throws SQLException {
            if (!acceptsURL(url)) {
                return null;
            }
            this.url = url;
            this.properties = properties;
            if (connection == null) {
                throw new SQLException("Replication should connect port not in thread_pool.");
            }
            return connection;
        }

        public DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) {
            return new DriverPropertyInfo[0];
        }

        public int getMajorVersion() {
            return 1;
        }

        public int getMinorVersion() {
            return 0;
        }

        public boolean jdbcCompliant() {
            return false;
        }

        public Logger getParentLogger() {
            return Logger.getGlobal();
        }
    }

    @Test
    void restoredTripleUsesExistingTenantConnectionWithoutOpeningReplicationConnection()
            throws Exception {
        BaseConnection base = tenantConnection(true);
        ReplicationProtocol protocol = mock(ReplicationProtocol.class);
        MultiPrimaryReplicationStream nativeStream = spy(new Session(FIRST).nativeStream);
        when(base.getReplicationProtocol()).thenReturn(protocol);
        when(protocol.startLogical(any())).thenReturn(nativeStream);
        RecordingDriver driver = new RecordingDriver(null);
        WalReplicationStream wal = connector(base, true);
        DriverManager.registerDriver(driver);
        try {
            wal.initialize(FIRST);
            assertThat(wal.isUseReplicationApi()).isTrue();
            verify(base).getReplicationAPI();
            PreparedStatement statement =
                    base.prepareStatement(
                            "SELECT 1 FROM pg_get_replication_slots() WHERE slot_name = ?");
            verify(statement).setString(1, "test");
            verify(base, never())
                    .prepareStatement("SELECT 1 FROM pg_replication_slots WHERE slot_name = ?");
            org.mockito.ArgumentCaptor<LogicalReplicationOptions> options =
                    org.mockito.ArgumentCaptor.forClass(LogicalReplicationOptions.class);
            verify(protocol).startLogical(options.capture());
            assertThat(options.getValue().getStartLSNPosition().asString()).isEqualTo(FIRST);
            assertThat(driver.url).isNull();
            wal.close();
            wal.close();
            verify(nativeStream).close();
            verify(base, never()).close();
        } finally {
            DriverManager.deregisterDriver(driver);
            wal.close();
        }
    }

    @Test
    void ordinaryModeStillOpensAndClosesDedicatedReplicationConnection() throws Exception {
        BaseConnection sql = tenantConnection(true);
        BaseConnection dedicated = mock(BaseConnection.class);
        when(dedicated.tacGetQueryExecutor())
                .thenReturn(mock(com.huawei.gaussdb.jdbc.core.QueryExecutor.class));
        PGReplicationConnectionImpl ordinaryApi = new PGReplicationConnectionImpl(dedicated);
        when(dedicated.getReplicationAPI()).thenReturn(ordinaryApi);
        ReplicationProtocol protocol = mock(ReplicationProtocol.class);
        when(dedicated.getReplicationProtocol()).thenReturn(protocol);
        MultiPrimaryReplicationStream nativeStream = mock(MultiPrimaryReplicationStream.class);
        when(protocol.startLogical(any())).thenReturn(nativeStream);
        RecordingDriver driver = new RecordingDriver(dedicated);
        WalReplicationStream wal = connector(sql, false);
        DriverManager.registerDriver(driver);
        try {
            wal.initialize("0/0");
            assertThat(wal.isUseReplicationApi()).isTrue();
            assertThat(driver.url).contains("replication=database");
            assertThat(driver.properties.getProperty("user")).isEqualTo("user");
            assertThat(driver.properties.getProperty("password")).isEqualTo("db-secret");
            verify(sql, never()).getReplicationAPI();
            wal.close();
            wal.close();
            verify(nativeStream).close();
            verify(dedicated).close();
            verify(sql, never()).close();
        } finally {
            DriverManager.deregisterDriver(driver);
            wal.close();
        }
    }

    @Test
    void tenantStreamStartFailureDoesNotCloseBorrowedConnectionOrFallBackToSql() throws Exception {
        BaseConnection base = tenantConnection(true);
        ReplicationProtocol protocol = mock(ReplicationProtocol.class);
        when(base.getReplicationProtocol()).thenReturn(protocol);
        when(protocol.startLogical(any())).thenThrow(new SQLException("stream start failed"));
        WalReplicationStream wal = connector(base, true);
        assertThatThrownBy(() -> wal.initialize(FIRST))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("fallback is disabled")
                .hasRootCauseMessage("stream start failed");
        assertThat(wal.isRunning()).isFalse();
        wal.close();
        verify(base, never()).close();
    }

    @Test
    void tenantRejectsOrdinaryBuilderWithoutClosingBorrowedConnection() throws Exception {
        BaseConnection base = tenantConnection(true);
        PGReplicationConnectionImpl ordinaryApi = new PGReplicationConnectionImpl(base);
        when(base.getReplicationAPI()).thenReturn(ordinaryApi);
        WalReplicationStream wal = connector(base, true);
        assertThatThrownBy(() -> wal.initialize(FIRST))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("fallback is disabled")
                .hasRootCauseMessage(
                        "HCS multi-primary requested but driver returned an ordinary logical builder; verify CMS/tenant configuration and the 507 driver");
        assertThat(wal.isRunning()).isFalse();
        verify(base, never()).close();
    }

    @Test
    void missingTenantSlotFailsBeforeStartingReplication() throws Exception {
        BaseConnection base = tenantConnection(false);
        WalReplicationStream wal = connector(base, true);
        assertThatThrownBy(() -> wal.initialize(FIRST))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("requires a pre-created logical slot");
        verify(base, never()).getReplicationAPI();
        verify(base, never()).close();
    }

    @Test
    void native507BufferAndCheckpointAckPreserveTriple() throws Exception {
        Session session = new Session(ReplicationPosition.MULTI_PRIMARY_START);
        WalReplicationStream wal = session.connector();
        session.packet("0/63,0/1,0/100", "BEGIN CSN: 99");
        session.packet(
                "0/63,0/1,0/200",
                "{\"table_name\":\"public.t\",\"op_type\":\"INSERT\",\"columns_name\":[\"id\"],\"columns_type\":[\"integer\"],\"columns_val\":[\"1\"]}");
        assertThat(wal.readChanges(100)).isEmpty();
        assertThat(wal.getLastLsn()).isEqualTo(ReplicationPosition.MULTI_PRIMARY_START);
        session.packet(FIRST, "COMMIT XID: 1");
        assertThat(wal.readChanges(100)).hasSize(1);
        assertThat(wal.getLastLsn()).isEqualTo(FIRST);
        assertThat(session.flushed()).containsExactly(0, 0, 0);
        wal.acknowledgeLsn(FIRST);
        assertThat(session.flushed()).containsExactly(100, 2, 4096);
        session.packet("0/65,0/1,0/2000", "unexpected");
        assertThatThrownBy(() -> wal.readChanges(100)).isInstanceOf(SQLException.class);
        assertThat(wal.getLastLsn()).isEqualTo(FIRST);
    }
}

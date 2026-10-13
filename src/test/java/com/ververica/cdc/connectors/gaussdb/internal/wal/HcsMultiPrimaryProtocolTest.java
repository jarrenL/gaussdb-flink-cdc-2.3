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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.huawei.gaussdb.jdbc.copy.CopyDual;
import com.huawei.gaussdb.jdbc.core.BaseConnection;
import com.huawei.gaussdb.jdbc.core.ReplicationProtocol;
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

    @Test
    void restoredTripleReachesNativeBuilderAndTenantPropertiesReachConnection() throws Exception {
        Session session = new Session(FIRST);
        BaseConnection base = mock(BaseConnection.class);
        ReplicationProtocol protocol = mock(ReplicationProtocol.class);
        when(base.tacGetQueryExecutor())
                .thenReturn(mock(com.huawei.gaussdb.jdbc.core.QueryExecutor.class));
        TenantReplicationConnectionImpl tenantApi = new TenantReplicationConnectionImpl(base);
        when(base.getReplicationAPI()).thenReturn(tenantApi);
        when(base.getReplicationProtocol()).thenReturn(protocol);
        when(protocol.startLogical(any())).thenReturn(session.nativeStream);
        Properties[] captured = {null};
        Driver driver =
                new Driver() {
                    public boolean acceptsURL(String url) {
                        return url.startsWith("jdbc:hcs-test:");
                    }

                    public Connection connect(String url, Properties p) {
                        if (!acceptsURL(url)) {
                            return null;
                        }
                        captured[0] = p;
                        return base;
                    }

                    public DriverPropertyInfo[] getPropertyInfo(String url, Properties p) {
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
                };
        Connection sql = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet result = mock(ResultSet.class);
        when(sql.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(result);
        when(result.next()).thenReturn(true);
        Properties tenant = new Properties();
        tenant.setProperty("connectMode", "auth");
        tenant.setProperty("tenantName", "tenant");
        tenant.setProperty("tenantPassword", "tenant-secret");
        WalReplicationStream wal =
                new WalReplicationStream(
                        sql,
                        "jdbc:hcs-test:test",
                        "user",
                        "db-secret",
                        "test",
                        "mppdb_decoding",
                        2,
                        "j",
                        false,
                        100);
        wal.setHcsMultiPrimary(true, tenant);
        DriverManager.registerDriver(driver);
        try {
            wal.initialize(FIRST);
            org.mockito.ArgumentCaptor<LogicalReplicationOptions> options =
                    org.mockito.ArgumentCaptor.forClass(LogicalReplicationOptions.class);
            verify(protocol).startLogical(options.capture());
            assertThat(options.getValue().getStartLSNPosition().asString()).isEqualTo(FIRST);
            assertThat(captured[0].getProperty("tenantPassword")).isEqualTo("tenant-secret");
            assertThat(captured[0].getProperty("replication"))
                    .isNull(); // replication flag is carried in the JDBC URL
            wal.close();
            PGReplicationConnectionImpl ordinaryApi = new PGReplicationConnectionImpl(base);
            when(base.getReplicationAPI()).thenReturn(ordinaryApi);
            assertThatThrownBy(() -> wal.initialize(FIRST))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("fallback is disabled");
        } finally {
            DriverManager.deregisterDriver(driver);
            wal.close();
        }
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

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

package com.ververica.cdc.connectors.gaussdb;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.ververica.cdc.connectors.gaussdb.internal.GaussDBSourceTask;
import com.ververica.cdc.connectors.gaussdb.internal.snapshot.InitialSnapshot;
import com.ververica.cdc.connectors.gaussdb.internal.snapshot.JdbcSnapshotAccess;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalReplicationStream;
import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;

class InitialSnapshotConsistencyGuardTest {
    @Test
    void unverifiedInitialStartStopsBeforeConnectingOrCreatingASlot() throws Exception {
        for (String value : new String[] {null, "false"}) {
            Map<String, String> options = InitialSnapshotTest.options();
            if (value == null) options.remove("scan.snapshot.allow-unverified-consistency");
            else options.put("scan.snapshot.allow-unverified-consistency", value);
            AtomicInteger connections = new AtomicInteger();
            GaussDBSourceTask task =
                    new GaussDBSourceTask() {
                        protected Connection connect(GaussDBConfig config) {
                            connections.incrementAndGet();
                            throw new AssertionError("A rejected initial job must not connect");
                        }
                    };
            SourceTaskContext context = mock(SourceTaskContext.class);
            OffsetStorageReader reader = mock(OffsetStorageReader.class);
            when(context.offsetStorageReader()).thenReturn(reader);
            when(reader.offset(anyMap())).thenReturn(null);
            task.initialize(context);
            assertThatThrownBy(() -> task.start(options))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Initial snapshot is blocked");
            assertThat(connections.get()).isZero();
        }
    }

    @Test
    void directSnapshotEntryAlsoRejectsBeforeSqlOrWalAccess() throws Exception {
        Map<String, String> options = InitialSnapshotTest.options();
        options.remove("scan.snapshot.allow-unverified-consistency");
        JdbcSnapshotAccess jdbc = mock(JdbcSnapshotAccess.class);
        WalReplicationStream stream = mock(WalReplicationStream.class);
        assertThatThrownBy(
                        () ->
                                new InitialSnapshot(
                                                new GaussDBConfig(options),
                                                jdbc,
                                                stream,
                                                () -> true)
                                        .prepare())
                .hasMessageContaining("does not establish consistency");
        verifyNoInteractions(jdbc, stream);
    }

    @Test
    void invalidOptInIsRejectedAndOptInDoesNotClaimVerification() {
        Map<String, String> options = InitialSnapshotTest.options();
        options.put("scan.snapshot.allow-unverified-consistency", "verified");
        assertThatThrownBy(() -> new GaussDBConfig(options))
                .hasMessageContaining("must be true or false");
        options.remove("scan.snapshot.allow-unverified-consistency");
        assertThatThrownBy(() -> new GaussDBConfig(options).requireSnapshotExperimentOptIn())
                .hasMessageContaining("GTM settings are not a visibility barrier");
        options.put("scan.snapshot.allow-unverified-consistency", "true");
        new GaussDBConfig(options).requireSnapshotExperimentOptIn();
    }
}

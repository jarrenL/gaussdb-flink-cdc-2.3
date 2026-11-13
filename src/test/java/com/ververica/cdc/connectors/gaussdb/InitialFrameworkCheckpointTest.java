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

import com.ververica.cdc.connectors.gaussdb.internal.snapshot.JdbcSnapshotAccess;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalChange;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Integration with the real, unmodified CDC 2.3 Debezium engine and checkpoint callbacks. */
class InitialFrameworkCheckpointTest {
    @Test
    @Timeout(40)
    void snapshotPublicationIsAtomicAndCompletedCheckpointResumesWithoutScanningAgain()
            throws Exception {
        FrameworkCheckpointTest.session = new FrameworkCheckpointTest.Session();
        FrameworkCheckpointTest.Session session = FrameworkCheckpointTest.session;
        JdbcSnapshotAccess jdbc = mock(JdbcSnapshotAccess.class);
        session.snapshotAccess = jdbc;
        when(jdbc.discoverTables())
                .thenReturn(Collections.singletonList(InitialSnapshotTest.table()));
        doAnswer(
                        i -> {
                            String token = i.getArgument(0);
                            session.batches.add(
                                    Collections.singletonList(
                                            InitialSnapshotTest.signal(
                                                    token,
                                                    token.startsWith("flink_low_")
                                                            ? InitialSnapshotTest.LOW
                                                            : InitialSnapshotTest.HIGH)));
                            return null;
                        })
                .when(jdbc)
                .writeSignal(anyString());
        when(jdbc.isSignal(any(), anyString()))
                .thenAnswer(
                        i -> {
                            WalChange row = i.getArgument(0);
                            return "signals".equals(row.getTable())
                                    && row.getAfterColumns()
                                            .get(0)
                                            .getValue()
                                            .equals(i.getArgument(1));
                        });
        CountDownLatch scanning = new CountDownLatch(1), releaseScan = new CountDownLatch(1);
        doAnswer(
                        i -> {
                            scanning.countDown();
                            if (!releaseScan.await(10, TimeUnit.SECONDS))
                                throw new IllegalStateException("scan timed out");
                            Consumer<WalChange> output = i.getArgument(1);
                            output.accept(
                                    InitialSnapshotTest.insert(1, "one", InitialSnapshotTest.LOW));
                            output.accept(
                                    InitialSnapshotTest.insert(2, "two", InitialSnapshotTest.LOW));
                            return null;
                        })
                .when(jdbc)
                .scan(anyList(), any(), any());
        byte[] saved;
        try (FrameworkCheckpointTest.Harness h =
                new FrameworkCheckpointTest.Harness(
                        null, true, InitialSnapshotTest.config().engineProperties())) {
            assertThat(scanning.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(h.output).isEmpty();
            assertThat(session.acknowledgements).isEmpty();
            // A checkpoint while the task scans must not contain a completed snapshot offset.
            synchronized (h.lock) {
                org.apache.flink.runtime.state.FunctionSnapshotContext c =
                        mock(org.apache.flink.runtime.state.FunctionSnapshotContext.class);
                when(c.getCheckpointId()).thenReturn(10L);
                h.source.snapshotState(c);
                assertThat(h.offsets.values).isEmpty();
            }
            releaseScan.countDown();
            assertThat(h.firstRow.await(10, TimeUnit.SECONDS)).isTrue();
            Future<byte[]> checkpoint = h.executor.submit(() -> h.snapshot(11));
            Thread.sleep(100);
            assertThat(checkpoint.isDone()).isFalse();
            assertThat(session.acknowledgements).isEmpty();
            h.release.countDown();
            saved = checkpoint.get(10, TimeUnit.SECONDS);
            assertThat(h.output)
                    .hasSize(2)
                    .allSatisfy(row -> assertThat(row).contains("\"op\":\"r\""));
            assertThat(new String(saved, java.nio.charset.StandardCharsets.UTF_8))
                    .contains(InitialSnapshotTest.HIGH, "gaussdb_snapshot_completed");
            assertThat(session.acknowledgements).isEmpty();
            h.source.notifyCheckpointComplete(11);
            assertThat(session.acknowledgements).containsExactly(InitialSnapshotTest.HIGH);
            assertThat(session.slotPreparations).containsExactly("NEW");
        }
        FrameworkCheckpointTest.session = new FrameworkCheckpointTest.Session();
        FrameworkCheckpointTest.Session resumed = FrameworkCheckpointTest.session;
        // No snapshot-access instance is supplied: any attempt to scan after restore will fail.
        Map<String, String> resumedOptions = InitialSnapshotTest.options();
        resumedOptions.remove("scan.snapshot.allow-unverified-consistency");
        try (FrameworkCheckpointTest.Harness h =
                new FrameworkCheckpointTest.Harness(
                        saved, false, new GaussDBConfig(resumedOptions).engineProperties())) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (resumed.start.get() == null && System.nanoTime() < deadline) Thread.sleep(10);
            assertThat(resumed.start.get()).isEqualTo(InitialSnapshotTest.HIGH);
            assertThat(resumed.slotPreparations).containsExactly(InitialSnapshotTest.HIGH);
            resumed.batches.add(
                    Collections.singletonList(
                            InitialSnapshotTest.insert(3, "new", InitialSnapshotTest.TAIL)));
            assertThat(h.firstRow.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(h.output).singleElement().asString().contains("\"op\":\"c\"", "new");
            h.snapshot(12);
            assertThat(resumed.acknowledgements).isEmpty();
            h.source.notifyCheckpointComplete(12);
            assertThat(resumed.acknowledgements).containsExactly(InitialSnapshotTest.TAIL);
        }
        verify(jdbc, times(1)).scan(anyList(), any(), any());
    }

    @Test
    @Timeout(40)
    void checkpointCannotPassBetweenSnapshotBatches() throws Exception {
        FrameworkCheckpointTest.session = new FrameworkCheckpointTest.Session();
        FrameworkCheckpointTest.Session session = FrameworkCheckpointTest.session;
        session.pauseAfterFirstPoll = true;
        JdbcSnapshotAccess jdbc = mock(JdbcSnapshotAccess.class);
        session.snapshotAccess = jdbc;
        when(jdbc.discoverTables())
                .thenReturn(Collections.singletonList(InitialSnapshotTest.table()));
        doAnswer(
                        i -> {
                            String token = i.getArgument(0);
                            session.batches.add(
                                    Collections.singletonList(
                                            InitialSnapshotTest.signal(
                                                    token,
                                                    token.startsWith("flink_low_")
                                                            ? InitialSnapshotTest.LOW
                                                            : InitialSnapshotTest.HIGH)));
                            return null;
                        })
                .when(jdbc)
                .writeSignal(anyString());
        when(jdbc.isSignal(any(), anyString()))
                .thenAnswer(
                        i -> {
                            WalChange row = i.getArgument(0);
                            return "signals".equals(row.getTable())
                                    && row.getAfterColumns()
                                            .get(0)
                                            .getValue()
                                            .equals(i.getArgument(1));
                        });
        doAnswer(
                        i -> {
                            Consumer<WalChange> output = i.getArgument(1);
                            for (int id = 1; id <= 3; id++)
                                output.accept(
                                        InitialSnapshotTest.insert(
                                                id, "snapshot", InitialSnapshotTest.LOW));
                            return null;
                        })
                .when(jdbc)
                .scan(anyList(), any(), any());
        Map<String, String> options = InitialSnapshotTest.options();
        options.put("scan.snapshot.batch.rows", "1");
        FrameworkCheckpointTest.Harness h =
                new FrameworkCheckpointTest.Harness(
                        null, false, new GaussDBConfig(options).engineProperties());
        try {
            assertThat(h.firstRow.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(h.output).hasSize(1);
            Future<byte[]> checkpoint = h.executor.submit(() -> h.snapshot(21));
            Thread.sleep(100);
            assertThat(checkpoint.isDone()).isFalse();
            assertThat(session.acknowledgements).isEmpty();
            session.continuePoll.countDown();
            byte[] saved = checkpoint.get(10, TimeUnit.SECONDS);
            assertThat(h.output).hasSize(3);
            com.ververica.cdc.debezium.internal.DebeziumOffset offset =
                    com.ververica.cdc.debezium.internal.DebeziumOffsetSerializer.INSTANCE
                            .deserialize(saved);
            assertThat(offset.sourceOffset.get("gaussdb_snapshot_completed")).isEqualTo(true);
            assertThat(offset.sourceOffset.get("gaussdb_position"))
                    .isEqualTo(InitialSnapshotTest.HIGH);
            assertThat(session.acknowledgements).isEmpty();
            h.source.notifyCheckpointComplete(21);
            assertThat(session.acknowledgements).containsExactly(InitialSnapshotTest.HIGH);
        } finally {
            session.continuePoll.countDown();
            h.close();
        }
    }
}

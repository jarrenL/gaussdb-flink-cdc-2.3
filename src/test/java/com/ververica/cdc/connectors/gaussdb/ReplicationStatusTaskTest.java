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

import com.ververica.cdc.connectors.gaussdb.internal.CommittedBatch;
import com.ververica.cdc.connectors.gaussdb.internal.GaussDBSourceTask;
import com.ververica.cdc.connectors.gaussdb.internal.snapshot.JdbcSnapshotAccess;
import com.ververica.cdc.connectors.gaussdb.internal.wal.*;
import java.net.SocketException;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;
import java.util.stream.Stream;
import org.apache.kafka.connect.source.*;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

@Timeout(15)
class ReplicationStatusTaskTest {
    @TempDir Path scratch;

    class Harness implements AutoCloseable {
        final InitialSnapshotTest.Fixture fixture;
        final Map<String, String> options = InitialSnapshotTest.options();
        final Connection sql = mock(Connection.class);
        final ScheduledExecutorService timer;
        final GaussDBSourceTask task;
        Runnable tick;

        Harness(boolean manual) throws Exception {
            options.put("scan.snapshot.storage", "disk");
            options.put("scan.snapshot.spill-dir", scratch.toString());
            options.put("scan.snapshot.batch.rows", "1");
            options.put("replication.status.interval.ms", "5");
            fixture = new InitialSnapshotTest.Fixture(options);
            timer = manual ? mock(ScheduledExecutorService.class) : null;
            task =
                    new GaussDBSourceTask() {
                        protected Connection connect(GaussDBConfig config) {
                            return sql;
                        }

                        protected void prepareInitialSlot(
                                GaussDBConfig config, Connection connection, String restored) {}

                        protected WalReplicationStream createStream(
                                GaussDBConfig config, Connection connection) {
                            assertThat(connection).isSameAs(sql);
                            return fixture.stream;
                        }

                        protected JdbcSnapshotAccess createSnapshotAccess(
                                GaussDBConfig config, Connection connection) {
                            assertThat(connection).isSameAs(sql);
                            return fixture.jdbc;
                        }

                        protected ScheduledExecutorService createStatusExecutor() {
                            return timer == null ? super.createStatusExecutor() : timer;
                        }
                    };
            SourceTaskContext context = mock(SourceTaskContext.class);
            OffsetStorageReader offsets = mock(OffsetStorageReader.class);
            when(context.offsetStorageReader()).thenReturn(offsets);
            when(offsets.offset(anyMap())).thenReturn(null);
            task.initialize(context);
        }

        void start() {
            task.start(options);
            if (timer != null) {
                ArgumentCaptor<Runnable> action = ArgumentCaptor.forClass(Runnable.class);
                verify(timer)
                        .scheduleWithFixedDelay(
                                action.capture(), eq(5L), eq(5L), eq(TimeUnit.MILLISECONDS));
                tick = action.getValue();
            }
        }

        public void close() {
            task.stop();
        }
    }

    @Test
    void feedbackContinuesDuringScanAndPublicationAndHandoffKeepsBufferedTail() throws Exception {
        CountDownLatch scanning = new CountDownLatch(1), release = new CountDownLatch(1);
        CountDownLatch scanFeedback = new CountDownLatch(2);
        AtomicReference<Thread> worker = new AtomicReference<>();
        ExecutorService poller = Executors.newSingleThreadExecutor();
        try (Harness h = new Harness(false)) {
            doAnswer(
                            i -> {
                                worker.set(Thread.currentThread());
                                if (scanning.getCount() == 0 && release.getCount() != 0)
                                    scanFeedback.countDown();
                                return null;
                            })
                    .when(h.fixture.stream)
                    .sendStatus();
            doAnswer(
                            i -> {
                                scanning.countDown();
                                if (!release.await(5, TimeUnit.SECONDS))
                                    throw new AssertionError("Scan release timed out");
                                Consumer<WalChange> output = i.getArgument(1);
                                output.accept(
                                        InitialSnapshotTest.insert(
                                                1, "one", InitialSnapshotTest.LOW));
                                output.accept(
                                        InitialSnapshotTest.insert(
                                                2, "two", InitialSnapshotTest.LOW));
                                return null;
                            })
                    .when(h.fixture.jdbc)
                    .scan(anyList(), any(), any());
            doAnswer(
                            i -> {
                                String token = i.getArgument(0);
                                h.fixture.queue.add(
                                        token.startsWith("flink_low_")
                                                ? Collections.singletonList(
                                                        InitialSnapshotTest.signal(
                                                                token, InitialSnapshotTest.LOW))
                                                : Arrays.asList(
                                                        InitialSnapshotTest.signal(
                                                                token, InitialSnapshotTest.HIGH),
                                                        InitialSnapshotTest.insert(
                                                                3,
                                                                "buffered",
                                                                InitialSnapshotTest.TAIL)));
                                return null;
                            })
                    .when(h.fixture.jdbc)
                    .writeSignal(anyString());
            h.start();
            Future<SourceRecord> first = poller.submit(() -> h.task.poll().get(0));
            assertThat(scanning.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(scanFeedback.await(5, TimeUnit.SECONDS)).isTrue();
            verify(h.fixture.stream, never()).acknowledgeLsn(anyString());
            release.countDown();
            SourceRecord partial = first.get(5, TimeUnit.SECONDS);
            assertThat(partial.sourceOffset().get("gaussdb_snapshot_completed")).isEqualTo(false);

            CountDownLatch publicationFeedback = new CountDownLatch(2);
            doAnswer(
                            i -> {
                                publicationFeedback.countDown();
                                return null;
                            })
                    .when(h.fixture.stream)
                    .sendStatus();
            assertThat(publicationFeedback.await(5, TimeUnit.SECONDS)).isTrue();
            verify(h.fixture.stream, never()).acknowledgeLsn(anyString());
            SourceRecord complete = h.task.poll().get(0);
            assertThat(complete.sourceOffset().get(CommittedBatch.POSITION))
                    .isEqualTo(InitialSnapshotTest.HIGH);
            assertThat(complete.sourceOffset().get("gaussdb_snapshot_completed")).isEqualTo(true);
            SourceRecord tail = h.task.poll().get(0);
            assertThat(tail.sourceOffset().get(CommittedBatch.POSITION))
                    .isEqualTo(InitialSnapshotTest.TAIL);
            h.fixture.queue.add(
                    Collections.singletonList(
                            InitialSnapshotTest.insert(4, "live", "0/68,0/1,0/500")));
            SourceRecord live = h.task.poll().get(0);
            assertThat(live.sourceOffset().get(CommittedBatch.POSITION))
                    .isEqualTo("0/68,0/1,0/500");
            verify(h.fixture.stream, times(1)).initialize(ReplicationPosition.MULTI_PRIMARY_START);
            verify(h.fixture.stream, never()).close();
            verify(h.fixture.stream, never()).acknowledgeLsn(anyString());
            h.task.commitRecord(live); // Simulate the framework's completed checkpoint callback.
            verify(h.fixture.stream).acknowledgeLsn("0/68,0/1,0/500");
        } finally {
            release.countDown();
            poller.shutdownNow();
        }
        worker.get().join(2000);
        assertThat(worker.get().isAlive()).isFalse();
        assertScratchEmpty();
    }

    @Test
    void statusFailureDuringScanCleansDiskAndCannotPublishOrAck() throws Exception {
        try (Harness h = new Harness(true)) {
            doThrow(
                            new SQLException(
                                    "status failed",
                                    "08006",
                                    new SocketException("Socket is closed")))
                    .when(h.fixture.stream)
                    .sendStatus();
            doAnswer(
                            i -> {
                                Consumer<WalChange> output = i.getArgument(1);
                                output.accept(
                                        InitialSnapshotTest.insert(
                                                1, "partial", InitialSnapshotTest.LOW));
                                h.tick.run();
                                BooleanSupplier running = i.getArgument(2);
                                running.getAsBoolean();
                                return null;
                            })
                    .when(h.fixture.jdbc)
                    .scan(anyList(), any(), any());
            h.start();
            assertThatThrownBy(h.task::poll)
                    .hasMessageContaining("phase=SCAN")
                    .hasRootCauseInstanceOf(SocketException.class);
            verify(h.fixture.stream, never()).acknowledgeLsn(anyString());
            verify(h.fixture.jdbc, never()).cleanSignals(anyString(), anyString());
            assertScratchEmpty();
        }
    }

    @Test
    void readFailureWaitingForLowReportsStageAndNeverStartsBusinessScan() throws Exception {
        try (Harness h = new Harness(true)) {
            when(h.fixture.stream.readChanges(anyInt()))
                    .thenThrow(
                            new SQLException(
                                    "copy failed",
                                    "08006",
                                    new SocketException("Socket is closed")));
            h.start();
            assertThatThrownBy(h.task::poll)
                    .hasMessageContaining("phase=WAIT_LOW")
                    .hasMessageContaining("sqlState=08006")
                    .hasMessageContaining("statusFeedbackAgeMs=none")
                    .hasRootCauseInstanceOf(SocketException.class);
            verify(h.fixture.jdbc, never()).scan(anyList(), any(), any());
            verify(h.fixture.stream, never()).acknowledgeLsn(anyString());
            assertScratchEmpty();
        }
    }

    @Test
    void statusFailureBeforePollingIsPropagatedAndStopCancelsTimer() throws Exception {
        try (Harness h = new Harness(true)) {
            doThrow(new SQLException("closed", "08006")).when(h.fixture.stream).sendStatus();
            h.start();
            h.tick.run();
            assertThatThrownBy(h.task::poll)
                    .hasMessageContaining("status feedback failed")
                    .hasMessageContaining("sqlState=08006");
            verify(h.fixture.jdbc, never()).validateSignalTable();
            verify(h.fixture.stream, never()).acknowledgeLsn(anyString());
            h.task.stop();
            verify(h.timer).shutdownNow();
            clearInvocations(h.fixture.stream);
            h.tick.run();
            verify(h.fixture.stream, never()).sendStatus();
        }
    }

    @Test
    void feedbackFailureBetweenPublicationBatchesImmediatelyDiscardsScratch() throws Exception {
        try (Harness h = new Harness(true)) {
            doAnswer(
                            i -> {
                                Consumer<WalChange> output = i.getArgument(1);
                                output.accept(
                                        InitialSnapshotTest.insert(
                                                1, "first", InitialSnapshotTest.LOW));
                                output.accept(
                                        InitialSnapshotTest.insert(
                                                2, "remaining", InitialSnapshotTest.LOW));
                                return null;
                            })
                    .when(h.fixture.jdbc)
                    .scan(anyList(), any(), any());
            h.start();
            SourceRecord partial = h.task.poll().get(0);
            assertThat(partial.sourceOffset().get("gaussdb_snapshot_completed")).isEqualTo(false);
            try (Stream<Path> paths = Files.list(scratch)) {
                assertThat(paths.count()).isEqualTo(1);
            }
            doThrow(new SQLException("status failed", "08006")).when(h.fixture.stream).sendStatus();
            h.tick.run();
            assertThatThrownBy(h.task::poll).hasMessageContaining("status feedback failed");
            assertScratchEmpty();
            verify(h.fixture.stream, never()).acknowledgeLsn(anyString());
        }
    }

    private void assertScratchEmpty() throws Exception {
        try (Stream<Path> paths = Files.list(scratch)) {
            assertThat(paths.count()).isZero();
        }
    }
}

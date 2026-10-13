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
import com.ververica.cdc.connectors.gaussdb.internal.TransactionDeserializer;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalChange;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalReplicationStream;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.Validator;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Runs the unmodified official CDC 2.3 source, embedded engine and checkpoint commit callback. */
class FrameworkCheckpointTest {
    static final String FIRST = "0/64,0/2,0/1000", SECOND = "0/65,0/1,0/2000";
    static volatile Session session;

    public static class FakeConnector extends SourceConnector {
        Map<String, String> config;

        public void start(Map<String, String> c) {
            config = c;
        }

        public Class<? extends Task> taskClass() {
            return FakeTask.class;
        }

        public List<Map<String, String>> taskConfigs(int n) {
            return Collections.singletonList(config);
        }

        public void stop() {}

        public String version() {
            return "test";
        }

        public ConfigDef config() {
            return new ConfigDef();
        }
    }

    public static class FakeTask extends GaussDBSourceTask {
        protected Connection connect(GaussDBConfig config) {
            return mock(Connection.class);
        }

        protected WalReplicationStream createStream(GaussDBConfig config, Connection sql) {
            return session.stream;
        }
    }

    static class Session {
        final BlockingQueue<List<WalChange>> batches = new LinkedBlockingQueue<>();
        final List<String> acknowledgements = new CopyOnWriteArrayList<>();
        final AtomicReference<String> start = new AtomicReference<>(),
                position = new AtomicReference<>();
        final WalReplicationStream stream = mock(WalReplicationStream.class);

        Session() throws Exception {
            doAnswer(
                            i -> {
                                start.set(i.getArgument(0));
                                position.set(i.getArgument(0));
                                return null;
                            })
                    .when(stream)
                    .initialize(anyString());
            when(stream.readChanges(anyInt()))
                    .thenAnswer(
                            i -> {
                                List<WalChange> rows = batches.poll(50, TimeUnit.MILLISECONDS);
                                if (rows == null) return Collections.emptyList();
                                position.set(rows.get(rows.size() - 1).getLsn());
                                return rows;
                            });
            when(stream.getLastLsn()).thenAnswer(i -> position.get());
            doAnswer(
                            i -> {
                                acknowledgements.add(i.getArgument(0));
                                return null;
                            })
                    .when(stream)
                    .acknowledgeLsn(anyString());
        }
    }

    static WalChange row(int id, String position) {
        return WalChange.insert(
                position,
                1,
                "public",
                "t",
                Collections.singletonList(
                        new WalChange.ColumnValue("id", 23, String.valueOf(id), false)));
    }

    static class MemoryState<T> implements ListState<T> {
        final List<T> values = new ArrayList<>();

        public Iterable<T> get() {
            return new ArrayList<>(values);
        }

        public void add(T value) {
            values.add(value);
        }

        public void clear() {
            values.clear();
        }

        public void update(List<T> values) {
            clear();
            addAll(values);
        }

        public void addAll(List<T> values) {
            this.values.addAll(values);
        }
    }

    static class Harness implements AutoCloseable {
        final Object lock = new Object();
        final MemoryState<byte[]> offsets = new MemoryState<>();
        final MemoryState<String> history = new MemoryState<>();
        final List<String> output = new CopyOnWriteArrayList<>();
        final CountDownLatch firstRow = new CountDownLatch(1), release = new CountDownLatch(1);
        final ExecutorService executor = Executors.newCachedThreadPool();
        final DebeziumSourceFunction<String> source;
        Future<?> running;

        Harness(byte[] restored, boolean block) throws Exception {
            Properties p = TestConfig.config().engineProperties();
            p.setProperty("connector.class", FakeConnector.class.getName());
            source =
                    new DebeziumSourceFunction<>(
                            new TransactionDeserializer<>(
                                    new JsonDebeziumDeserializationSchema(),
                                    Collections.emptyList()),
                            p,
                            null,
                            Validator.getDefaultValidator());
            StreamingRuntimeContext runtime =
                    mock(StreamingRuntimeContext.class, RETURNS_DEEP_STUBS);
            when(runtime.getNumberOfParallelSubtasks()).thenReturn(1);
            when(runtime.isCheckpointingEnabled()).thenReturn(true);
            source.setRuntimeContext(runtime);
            OperatorStateStore store = mock(OperatorStateStore.class);
            when(store.getUnionListState(any(ListStateDescriptor.class)))
                    .thenAnswer(
                            i ->
                                    ((ListStateDescriptor<?>) i.getArgument(0))
                                                    .getName()
                                                    .equals(
                                                            DebeziumSourceFunction
                                                                    .OFFSETS_STATE_NAME)
                                            ? offsets
                                            : history);
            FunctionInitializationContext init = mock(FunctionInitializationContext.class);
            when(init.getOperatorStateStore()).thenReturn(store);
            when(init.isRestored()).thenReturn(restored != null);
            if (restored != null) offsets.add(restored);
            source.initializeState(init);
            source.open(new Configuration());
            running =
                    executor.submit(
                            () -> {
                                try {
                                    source.run(
                                            new SourceFunction.SourceContext<String>() {
                                                public void collect(String value) {
                                                    output.add(value);
                                                    firstRow.countDown();
                                                    if (block && output.size() == 1)
                                                        try {
                                                            if (!release.await(
                                                                    10, TimeUnit.SECONDS))
                                                                throw new IllegalStateException(
                                                                        "test timed out");
                                                        } catch (InterruptedException e) {
                                                            Thread.currentThread().interrupt();
                                                            throw new RuntimeException(e);
                                                        }
                                                }

                                                public void collectWithTimestamp(String v, long t) {
                                                    collect(v);
                                                }

                                                public void emitWatermark(Watermark w) {}

                                                public void markAsTemporarilyIdle() {}

                                                public Object getCheckpointLock() {
                                                    return lock;
                                                }

                                                public void close() {}
                                            });
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
        }

        byte[] snapshot(long id) throws Exception {
            synchronized (lock) {
                FunctionSnapshotContext c = mock(FunctionSnapshotContext.class);
                when(c.getCheckpointId()).thenReturn(id);
                source.snapshotState(c);
                return offsets.values.get(0).clone();
            }
        }

        public void close() throws Exception {
            release.countDown();
            source.cancel();
            running.get(10, TimeUnit.SECONDS);
            source.close();
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(40)
    void checkpointIsAtomicAndOnlyCompletionAcknowledgesThenRestoreUsesFullTriple()
            throws Exception {
        session = new Session();
        Session first = session;
        byte[] saved;
        try (Harness h = new Harness(null, true)) {
            first.batches.add(Arrays.asList(row(1, FIRST), row(2, FIRST)));
            assertThat(h.firstRow.await(10, TimeUnit.SECONDS)).isTrue();
            Future<byte[]> checkpoint = h.executor.submit(() -> h.snapshot(1));
            Thread.sleep(100);
            assertThat(checkpoint.isDone()).isFalse();
            assertThat(first.acknowledgements).isEmpty();
            h.release.countDown();
            saved = checkpoint.get(10, TimeUnit.SECONDS);
            assertThat(h.output).hasSize(2);
            assertThat(new String(saved, java.nio.charset.StandardCharsets.UTF_8)).contains(FIRST);
            assertThat(first.acknowledgements).isEmpty();
            h.source.notifyCheckpointComplete(1);
            assertThat(first.acknowledgements).containsExactly(FIRST);
            first.batches.add(Collections.singletonList(row(3, SECOND)));
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (h.output.size() < 3 && System.nanoTime() < end) Thread.sleep(10);
            assertThat(h.output).hasSize(3);
            h.snapshot(2);
            h.source.notifyCheckpointAborted(2);
            assertThat(first.acknowledgements).containsExactly(FIRST);
        }
        session = new Session();
        Session restored = session;
        try (Harness h = new Harness(saved, false)) {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (restored.start.get() == null && System.nanoTime() < end) Thread.sleep(10);
            assertThat(restored.start.get()).isEqualTo(FIRST);
            restored.batches.add(Collections.singletonList(row(3, SECOND)));
            assertThat(h.firstRow.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(h.output).hasSize(1);
            h.snapshot(3);
            h.source.notifyCheckpointComplete(3);
            assertThat(restored.acknowledgements).containsExactly(SECOND);
        }
    }
}

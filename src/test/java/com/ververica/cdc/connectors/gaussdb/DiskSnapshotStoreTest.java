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
import com.ververica.cdc.connectors.gaussdb.internal.snapshot.*;
import com.ververica.cdc.connectors.gaussdb.internal.wal.*;
import java.nio.file.*;
import java.sql.Connection;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DiskSnapshotStoreTest {
    @TempDir Path parent;

    Map<String, String> diskOptions() {
        Map<String, String> options = InitialSnapshotTest.options();
        options.put("scan.snapshot.storage", "disk");
        options.put("scan.snapshot.spill-dir", parent.toString());
        options.put("scan.snapshot.disk.cache.mb", "2");
        options.put("scan.snapshot.disk.write-buffer.bytes", "4096");
        // These are MEMORY backend limits, not total row limits for the disk backend.
        options.put("scan.snapshot.max.rows", "1");
        options.put("scan.snapshot.max.bytes", "1");
        return options;
    }

    long children() throws Exception {
        try (Stream<Path> paths = Files.list(parent)) {
            return paths.count();
        }
    }

    static Map<String, List<String>> image(List<WalChange> rows) {
        Map<String, List<String>> result = new HashMap<>();
        for (WalChange row : rows) {
            List<String> values = new ArrayList<>();
            for (WalChange.ColumnValue c : row.getAfterColumns()) values.add(c.getValue());
            result.put(row.getSchema() + "." + row.getTable() + ":" + values.get(0), values);
        }
        return result;
    }

    @Test
    void diskAndMemoryProduceSameMergedImageWithKeyChangesNullsAndLargeUnicode() throws Exception {
        Map<String, String> options = diskOptions();
        options.put("schema-name", "public|other");
        SnapshotTable a = InitialSnapshotTest.table();
        SnapshotTable b = new SnapshotTable("other", "t", a.columns, a.primaryKey);
        List<SnapshotTable> tables = Arrays.asList(a, b);
        Map<String, String> memoryOptions = new HashMap<>(options);
        memoryOptions.put("scan.snapshot.storage", "memory");
        memoryOptions.put("scan.snapshot.max.rows", "10000");
        memoryOptions.put("scan.snapshot.max.bytes", "67108864");
        char[] chars = new char[70000];
        Arrays.fill(chars, '中');
        String large = new String(chars) + "😀\0";
        try (SnapshotBackfillBuffer disk =
                        new SnapshotBackfillBuffer(new GaussDBConfig(options), tables);
                SnapshotBackfillBuffer memory =
                        new SnapshotBackfillBuffer(new GaussDBConfig(memoryOptions), tables)) {
            for (int id = 1; id <= 1000; id++) {
                WalChange row =
                        InitialSnapshotTest.insert(
                                id, id == 10 ? large : "row" + id, InitialSnapshotTest.LOW);
                disk.addSnapshotRow(row);
                memory.addSnapshotRow(row);
            }
            WalChange other =
                    WalChange.insert(
                            InitialSnapshotTest.LOW,
                            0,
                            "other",
                            "t",
                            InitialSnapshotTest.image(1, "other table"));
            disk.addSnapshotRow(other);
            memory.addSnapshotRow(other);
            List<WalChange> changes =
                    Arrays.asList(
                            WalChange.update(
                                    InitialSnapshotTest.UPDATE,
                                    1,
                                    "public",
                                    "t",
                                    InitialSnapshotTest.image(1, "row1"),
                                    InitialSnapshotTest.image(1001, "new key")),
                            WalChange.delete(
                                    InitialSnapshotTest.UPDATE,
                                    1,
                                    "public",
                                    "t",
                                    InitialSnapshotTest.image(2, "row2")),
                            WalChange.update(
                                    InitialSnapshotTest.UPDATE,
                                    1,
                                    "public",
                                    "t",
                                    InitialSnapshotTest.image(3, "row3"),
                                    InitialSnapshotTest.image(3, null)),
                            InitialSnapshotTest.insert(4, "overlap", InitialSnapshotTest.UPDATE));
            for (WalChange change : changes) {
                disk.apply(change);
                memory.apply(change);
            }
            disk.seal();
            Map<String, List<String>> actual = new HashMap<>();
            while (!disk.isEmpty()) {
                List<WalChange> batch = disk.drain(InitialSnapshotTest.HIGH, 31, 10000);
                assertThat(batch).isNotEmpty().hasSizeLessThanOrEqualTo(31);
                actual.putAll(image(batch));
            }
            assertThat(actual).isEqualTo(image(memory.finish(InitialSnapshotTest.HIGH)));
            assertThat(actual.get("public.t:10").get(1)).isEqualTo(large);
            assertThat(actual.get("public.t:3").get(1)).isNull();
            assertThat(actual).doesNotContainKeys("public.t:1", "public.t:2");
        }
        assertThat(children()).isZero();
    }

    @Test
    void quotaAndOversizedRowsFailAndRemoveOnlyOwnedScratchFiles() throws Exception {
        Path keep = Files.write(parent.resolve("keep.txt"), new byte[] {1});
        Map<String, String> options = diskOptions();
        options.put("scan.snapshot.disk.max.bytes", "65536");
        char[] chars = new char[10000];
        Arrays.fill(chars, 'x');
        String value = new String(chars);
        try (SnapshotBackfillBuffer disk =
                new SnapshotBackfillBuffer(
                        new GaussDBConfig(options),
                        Collections.singletonList(InitialSnapshotTest.table()))) {
            assertThatThrownBy(
                            () -> {
                                for (int id = 1; id < 100; id++)
                                    disk.addSnapshotRow(
                                            InitialSnapshotTest.insert(
                                                    id, value, InitialSnapshotTest.LOW));
                            })
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("quota");
        }
        options.put("scan.snapshot.disk.max.bytes", "1");
        assertThatThrownBy(
                        () ->
                                new SnapshotBackfillBuffer(
                                        new GaussDBConfig(options),
                                        Collections.singletonList(InitialSnapshotTest.table())))
                .hasStackTraceContaining("quota");
        options.put("scan.snapshot.disk.max.bytes", "1048576");
        options.put("scan.snapshot.disk.max.row.bytes", "100");
        try (SnapshotBackfillBuffer disk =
                new SnapshotBackfillBuffer(
                        new GaussDBConfig(options),
                        Collections.singletonList(InitialSnapshotTest.table()))) {
            assertThatThrownBy(
                            () ->
                                    disk.addSnapshotRow(
                                            InitialSnapshotTest.insert(
                                                    1, "too big", InitialSnapshotTest.LOW)))
                    .hasMessageContaining("max.row.bytes");
        }
        assertThat(Files.exists(keep)).isTrue();
        assertThat(children()).isEqualTo(1);
    }

    @Test
    void timeoutBeforeHighCleansScratchWithoutAck() throws Exception {
        Map<String, String> options = diskOptions();
        options.put("scan.snapshot.timeout.seconds", "1");
        InitialSnapshotTest.Fixture fixture = new InitialSnapshotTest.Fixture(options);
        fixture.deliverHigh = false;
        assertThatThrownBy(fixture::execute).hasMessageContaining("timed out");
        assertThat(children()).isZero();
        verify(fixture.stream, never()).acknowledgeLsn(anyString());
    }

    @Test
    void stoppingPartialPublicationCleansDiskAndCannotAckIncompleteSnapshot() throws Exception {
        Map<String, String> options = diskOptions();
        options.put("scan.snapshot.batch.rows", "1");
        InitialSnapshotTest.Fixture fixture = new InitialSnapshotTest.Fixture(options);
        doAnswer(
                        i -> {
                            Consumer<WalChange> output = i.getArgument(1);
                            for (int id = 1; id <= 3; id++)
                                output.accept(
                                        InitialSnapshotTest.insert(
                                                id, "row", InitialSnapshotTest.LOW));
                            return null;
                        })
                .when(fixture.jdbc)
                .scan(anyList(), any(), any());
        GaussDBSourceTask task =
                new GaussDBSourceTask() {
                    protected Connection connect(GaussDBConfig config) {
                        return mock(Connection.class);
                    }

                    protected void prepareInitialSlot(
                            GaussDBConfig config, Connection sql, String restored) {}

                    protected WalReplicationStream createStream(
                            GaussDBConfig config, Connection sql) {
                        return fixture.stream;
                    }

                    protected JdbcSnapshotAccess createSnapshotAccess(
                            GaussDBConfig config, Connection sql) {
                        return fixture.jdbc;
                    }
                };
        SourceTaskContext context = mock(SourceTaskContext.class);
        OffsetStorageReader offsets = mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(offsets);
        when(offsets.offset(anyMap())).thenReturn(null);
        task.initialize(context);
        task.start(options);
        SourceRecord first = task.poll().get(0);
        assertThat(first.sourceOffset().get("gaussdb_snapshot_completed")).isEqualTo(false);
        assertThat(children()).isEqualTo(1);
        assertThatThrownBy(() -> task.commitRecord(first))
                .hasMessageContaining("completed snapshot checkpoint");
        task.stop();
        assertThat(children()).isZero();
        verify(fixture.stream, never()).acknowledgeLsn(anyString());
    }

    @Test
    void successfulPublicationAndDuplicateKeyFailureBothCleanDisk() throws Exception {
        InitialSnapshotTest.Fixture fixture = new InitialSnapshotTest.Fixture(diskOptions());
        doAnswer(
                        i -> {
                            Consumer<WalChange> out = i.getArgument(1);
                            out.accept(
                                    InitialSnapshotTest.insert(1, "ok", InitialSnapshotTest.LOW));
                            return null;
                        })
                .when(fixture.jdbc)
                .scan(anyList(), any(), any());
        assertThat(fixture.execute()).hasSize(1);
        assertThat(children()).isZero();
        InitialSnapshotTest.Fixture duplicate = new InitialSnapshotTest.Fixture(diskOptions());
        doAnswer(
                        i -> {
                            Consumer<WalChange> out = i.getArgument(1);
                            out.accept(InitialSnapshotTest.insert(1, "a", InitialSnapshotTest.LOW));
                            out.accept(InitialSnapshotTest.insert(1, "b", InitialSnapshotTest.LOW));
                            return null;
                        })
                .when(duplicate.jdbc)
                .scan(anyList(), any(), any());
        assertThatThrownBy(duplicate::execute).hasMessageContaining("Duplicate primary key");
        assertThat(children()).isZero();
    }
}

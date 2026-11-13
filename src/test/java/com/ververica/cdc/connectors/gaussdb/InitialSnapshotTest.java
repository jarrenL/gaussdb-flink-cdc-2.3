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
import com.ververica.cdc.connectors.gaussdb.internal.TransactionDeserializer;
import com.ververica.cdc.connectors.gaussdb.internal.snapshot.*;
import com.ververica.cdc.connectors.gaussdb.internal.wal.*;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class InitialSnapshotTest {
    static final String LOW = "0/64,0/1,0/100",
            UPDATE = "0/65,0/2,0/200",
            HIGH = "0/66,0/1,0/300",
            TAIL = "0/67,0/1,0/400";

    static Map<String, String> options() {
        Map<String, String> settings = TestConfig.options();
        settings.put("scan.startup.mode", "initial");
        settings.put("scan.snapshot.signal-table", "cdc_meta.signals");
        return settings;
    }

    static GaussDBConfig config() {
        return new GaussDBConfig(options());
    }

    static SnapshotTable table() {
        Map<String, Integer> columns = new LinkedHashMap<>();
        columns.put("id", 23);
        columns.put("name", 25);
        return new SnapshotTable("public", "t", columns, Collections.singletonList("id"));
    }

    static List<WalChange.ColumnValue> image(int id, String name) {
        return Arrays.asList(
                new WalChange.ColumnValue("id", 23, String.valueOf(id), false),
                new WalChange.ColumnValue("name", 25, name, name == null));
    }

    static WalChange insert(int id, String name, String pos) {
        return WalChange.insert(pos, 1, "public", "t", image(id, name));
    }

    static WalChange signal(String token, String pos) {
        return WalChange.insert(
                pos,
                1,
                "cdc_meta",
                "signals",
                Collections.singletonList(
                        new WalChange.ColumnValue("signal_id", 1043, token, false)));
    }

    static List<String> json(SourceRecord record) throws Exception {
        List<String> output = new ArrayList<>();
        new TransactionDeserializer<>(
                        new JsonDebeziumDeserializationSchema(), Collections.emptyList())
                .deserialize(record, ConfigurationAndRowTest.collector(output));
        return output;
    }

    @Test
    void mergesOverlappingInsertsDeletesUpdatesAndPrimaryKeyChanges() throws Exception {
        SnapshotBackfillBuffer buffer =
                new SnapshotBackfillBuffer(config(), Collections.singletonList(table()));
        buffer.addSnapshotRow(insert(1, "already-new", LOW));
        buffer.addSnapshotRow(insert(2, "already-deleted-later", LOW));
        buffer.addSnapshotRow(insert(3, "insert-already-visible", LOW));
        buffer.addSnapshotRow(insert(4, "kept", LOW));
        buffer.apply(
                WalChange.update(
                        UPDATE, 1, "public", "t", image(1, "old"), image(1, "already-new")));
        buffer.apply(WalChange.delete(UPDATE, 1, "public", "t", image(2, "old")));
        buffer.apply(insert(3, "insert-already-visible", UPDATE));
        buffer.apply(
                WalChange.update(UPDATE, 1, "public", "t", image(3, "old"), image(30, "主键变更")));
        // Deleted before SELECT reached it: removal of an absent row is idempotent.
        buffer.apply(WalChange.delete(UPDATE, 1, "public", "t", image(9, "gone")));
        List<String> rows = json(CommittedBatch.snapshot(config(), buffer.finish(HIGH), HIGH));
        assertThat(rows).hasSize(3);
        assertThat(rows)
                .allSatisfy(
                        row ->
                                assertThat(row)
                                        .contains("\"op\":\"r\"", "\"snapshot\":\"true\"", HIGH));
        assertThat(String.join("", rows))
                .contains("already-new", "kept", "主键变更", "\"id\":30")
                .doesNotContain("already-deleted-later", "\"id\":2,", "\"id\":3,");
    }

    @Test
    void wholeMarkerIntervalIsMergedAndPostHighSuffixIsPreserved() throws Exception {
        GaussDBConfig config = config();
        JdbcSnapshotAccess jdbc = mock(JdbcSnapshotAccess.class);
        WalReplicationStream stream = mock(WalReplicationStream.class);
        when(jdbc.discoverTables()).thenReturn(Collections.singletonList(table()));
        List<List<WalChange>> reads = new ArrayList<>();
        doAnswer(
                        i -> {
                            String token = i.getArgument(0);
                            if (token.startsWith("flink_low_"))
                                reads.add(
                                        Arrays.asList(
                                                signal(token, LOW),
                                                insert(3, "during-low-read", UPDATE)));
                            else
                                reads.add(
                                        Arrays.asList(
                                                WalChange.delete(
                                                        UPDATE, 1, "public", "t", image(2, "old")),
                                                signal(token, HIGH),
                                                insert(4, "after-high", TAIL)));
                            return null;
                        })
                .when(jdbc)
                .writeSignal(anyString());
        doAnswer(
                        i -> {
                            Consumer<WalChange> output = i.getArgument(1);
                            output.accept(insert(1, "unchanged", LOW));
                            output.accept(insert(2, "to-delete", LOW));
                            // This INSERT is already visible to the JDBC scan; it must appear only
                            // once.
                            output.accept(insert(3, "during-low-read", UPDATE));
                            return null;
                        })
                .when(jdbc)
                .scan(anyList(), any(), any());
        AtomicInteger index = new AtomicInteger();
        when(stream.readChanges(anyInt())).thenAnswer(i -> reads.get(index.getAndIncrement()));
        when(stream.getLastLsn())
                .thenAnswer(
                        i -> {
                            List<WalChange> list = reads.get(index.get() - 1);
                            return list.get(list.size() - 1).getLsn();
                        });
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
        List<SourceRecord> result = new InitialSnapshot(config, jdbc, stream, () -> true).execute();
        assertThat(result).hasSize(2);
        assertThat(json(result.get(0)))
                .hasSize(2)
                .allSatisfy(row -> assertThat(row).doesNotContain("after-high", "to-delete"));
        assertThat(json(result.get(1)))
                .singleElement()
                .asString()
                .contains("after-high", "\"op\":\"c\"");
        assertThat(result.get(0).sourceOffset().get(CommittedBatch.POSITION)).isEqualTo(HIGH);
        assertThat(result.get(0).sourceOffset().get("gaussdb_snapshot_completed")).isEqualTo(true);
        assertThat(result.get(1).sourceOffset().get(CommittedBatch.POSITION)).isEqualTo(TAIL);
        verify(jdbc).cleanSignals(startsWith("flink_low_"), startsWith("flink_high_"));
        verify(stream, never()).acknowledgeLsn(anyString());
    }

    @Test
    void compositePrimaryKeysAreCanonicalAndNamespacedByTable() {
        Map<String, String> options = options();
        options.put("table-name", "t|other");
        options.put("schema-name", "public|other_schema");
        Map<String, Integer> columns = new LinkedHashMap<>();
        columns.put("id", 23);
        columns.put("name", 25);
        SnapshotTable a = new SnapshotTable("public", "t", columns, Arrays.asList("id", "name"));
        SnapshotTable b =
                new SnapshotTable("other_schema", "t", columns, Arrays.asList("id", "name"));
        SnapshotBackfillBuffer buffer =
                new SnapshotBackfillBuffer(new GaussDBConfig(options), Arrays.asList(a, b));
        buffer.addSnapshotRow(insert(1, "a|b", LOW));
        buffer.addSnapshotRow(WalChange.insert(LOW, 1, "other_schema", "t", image(1, "a|b")));
        buffer.apply(
                WalChange.insert(
                        UPDATE,
                        1,
                        "public",
                        "t",
                        Arrays.asList(
                                new WalChange.ColumnValue("id", 23, "01", false),
                                new WalChange.ColumnValue("name", 25, "a|b", false))));
        assertThat(buffer.finish(HIGH)).hasSize(2);
    }

    @Test
    void rejectsMissingImagesUnsupportedKeysAndBufferOverflow() {
        SnapshotBackfillBuffer buffer =
                new SnapshotBackfillBuffer(config(), Collections.singletonList(table()));
        assertThatThrownBy(
                        () ->
                                buffer.apply(
                                        WalChange.update(
                                                UPDATE,
                                                1,
                                                "public",
                                                "t",
                                                Collections.emptyList(),
                                                image(1, "x"))))
                .hasMessageContaining("primary key");
        assertThatThrownBy(
                        () ->
                                buffer.apply(
                                        WalChange.insert(
                                                UPDATE,
                                                1,
                                                "public",
                                                "t",
                                                Collections.singletonList(
                                                        new WalChange.ColumnValue(
                                                                "id", 23, "1", false)))))
                .hasMessageContaining("complete after image");
        Map<String, String> small = options();
        small.put("scan.snapshot.max.rows", "1");
        SnapshotBackfillBuffer bounded =
                new SnapshotBackfillBuffer(
                        new GaussDBConfig(small), Collections.singletonList(table()));
        bounded.addSnapshotRow(insert(1, "x", LOW));
        assertThatThrownBy(() -> bounded.addSnapshotRow(insert(2, "y", LOW)))
                .hasMessageContaining("buffer limit");
        assertThatThrownBy(
                        () ->
                                new SnapshotTable(
                                        "public",
                                        "bad",
                                        Collections.singletonMap("amount", 1700),
                                        Collections.singletonList("amount")))
                .hasMessageContaining("primary-key type");
        assertThatThrownBy(
                        () ->
                                new SnapshotTable(
                                        "public",
                                        "bad",
                                        Collections.singletonMap("id", 23),
                                        Collections.emptyList()))
                .hasMessageContaining("primary key");
    }

    @Test
    void configurationRequiresSignalAndBindsCompletedCheckpointToSelection() {
        Map<String, String> missing = options();
        missing.remove("scan.snapshot.signal-table");
        assertThatThrownBy(() -> new GaussDBConfig(missing)).hasMessageContaining("signal-table");
        assertThatThrownBy(() -> SnapshotTable.signalTable("meta.signal; DROP TABLE t"))
                .hasMessageContaining("schema.table");
        assertThat(config().matches("cdc_meta", "signals")).isFalse();
        SourceRecord snapshot = CommittedBatch.snapshot(config(), Collections.emptyList(), HIGH);
        config().validateSnapshotOffset(snapshot.sourceOffset());
        assertThatThrownBy(
                        () ->
                                config().validateSnapshotOffset(
                                                Collections.singletonMap(
                                                        CommittedBatch.POSITION, HIGH)))
                .hasMessageContaining("completed snapshot checkpoint");
        Map<String, String> changed = options();
        changed.put("table-name", "another");
        assertThatThrownBy(
                        () ->
                                new GaussDBConfig(changed)
                                        .validateSnapshotOffset(snapshot.sourceOffset()))
                .hasMessageContaining("same table selection");
    }

    @Test
    void cancellationNeverPublishesOrAcknowledges() throws Exception {
        JdbcSnapshotAccess jdbc = mock(JdbcSnapshotAccess.class);
        when(jdbc.discoverTables()).thenReturn(Collections.singletonList(table()));
        WalReplicationStream stream = mock(WalReplicationStream.class);
        assertThatThrownBy(() -> new InitialSnapshot(config(), jdbc, stream, () -> false).execute())
                .isInstanceOf(InterruptedException.class);
        verify(jdbc, never()).writeSignal(anyString());
        verify(stream, never()).acknowledgeLsn(anyString());
    }

    static class Fixture {
        final GaussDBConfig config;
        final JdbcSnapshotAccess jdbc = mock(JdbcSnapshotAccess.class);
        final WalReplicationStream stream = mock(WalReplicationStream.class);
        final java.util.Deque<List<WalChange>> queue = new java.util.ArrayDeque<>();
        String position = ReplicationPosition.MULTI_PRIMARY_START;
        String highPosition = HIGH;
        boolean deliverHigh = true;

        Fixture(Map<String, String> options) throws Exception {
            config = new GaussDBConfig(options);
            when(jdbc.discoverTables()).thenReturn(Collections.singletonList(table()));
            doAnswer(
                            i -> {
                                String token = i.getArgument(0);
                                if (token.startsWith("flink_low_"))
                                    queue.add(Collections.singletonList(signal(token, LOW)));
                                else if (deliverHigh)
                                    queue.add(
                                            Collections.singletonList(signal(token, highPosition)));
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
            when(stream.readChanges(anyInt()))
                    .thenAnswer(
                            i -> {
                                List<WalChange> rows = queue.poll();
                                if (rows == null) return Collections.emptyList();
                                position = rows.get(rows.size() - 1).getLsn();
                                return rows;
                            });
            when(stream.getLastLsn()).thenAnswer(i -> position);
        }

        List<SourceRecord> execute() throws Exception {
            return new InitialSnapshot(config, jdbc, stream, () -> true).execute();
        }
    }

    @Test
    void emptyDatabaseStillHasACompletedSnapshotOffsetWithoutBusinessTraffic() throws Exception {
        Fixture fixture = new Fixture(options());
        List<SourceRecord> records = fixture.execute();
        assertThat(records).hasSize(1);
        assertThat(json(records.get(0))).isEmpty();
        assertThat(records.get(0).sourceOffset().get(CommittedBatch.POSITION)).isEqualTo(HIGH);
        fixture.config.validateSnapshotOffset(records.get(0).sourceOffset());
        verify(fixture.stream, never()).acknowledgeLsn(anyString());
    }

    @Test
    void missingHighMarkerTimesOutWithoutAcknowledgeOrPublication() throws Exception {
        Map<String, String> options = options();
        options.put("scan.snapshot.timeout.seconds", "1");
        Fixture fixture = new Fixture(options);
        fixture.deliverHigh = false;
        assertThatThrownBy(fixture::execute).hasMessageContaining("timed out");
        verify(fixture.jdbc, never()).cleanSignals(anyString(), anyString());
        verify(fixture.stream, never()).acknowledgeLsn(anyString());
    }

    @Test
    void unorderedCommitOrTableRecreationIsRejectedBeforePublication() throws Exception {
        Fixture unordered = new Fixture(options());
        unordered.highPosition = "0/63,0/1,0/100";
        assertThatThrownBy(unordered::execute).hasMessageContaining("not ordered");
        Fixture recreated = new Fixture(options());
        SnapshotTable old = table();
        SnapshotTable replacement =
                new SnapshotTable(old.schema, old.table, old.columns, old.primaryKey, 99);
        when(recreated.jdbc.discoverTables())
                .thenReturn(Collections.singletonList(old), Collections.singletonList(replacement));
        assertThatThrownBy(recreated::execute).hasMessageContaining("schema changed");
        verify(recreated.jdbc, never()).cleanSignals(anyString(), anyString());
        verify(recreated.stream, never()).acknowledgeLsn(anyString());
    }

    @Test
    void snapshotReadOperationUsesOfficialRowDeserializerAsAnInsert() throws Exception {
        org.apache.flink.table.types.logical.RowType type =
                (org.apache.flink.table.types.logical.RowType)
                        org.apache.flink.table.api.DataTypes.ROW(
                                        org.apache.flink.table.api.DataTypes.FIELD(
                                                "id", org.apache.flink.table.api.DataTypes.INT()),
                                        org.apache.flink.table.api.DataTypes.FIELD(
                                                "name",
                                                org.apache.flink.table.api.DataTypes.STRING()))
                                .getLogicalType();
        List<org.apache.flink.table.data.RowData> rows = new ArrayList<>();
        new TransactionDeserializer<>(
                        com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema
                                .newBuilder()
                                .setPhysicalRowType(type)
                                .setResultTypeInfo(
                                        org.apache.flink.table.runtime.typeutils.InternalTypeInfo
                                                .of(type))
                                .build(),
                        type.getFieldNames())
                .deserialize(
                        CommittedBatch.snapshot(
                                config(), Collections.singletonList(insert(1, "全量", HIGH)), HIGH),
                        ConfigurationAndRowTest.collector(rows));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getRowKind()).isEqualTo(org.apache.flink.types.RowKind.INSERT);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("全量");
    }
}

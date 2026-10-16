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

package com.ververica.cdc.connectors.gaussdb.examples;

import com.ververica.cdc.connectors.gaussdb.GaussDBConfig;
import com.ververica.cdc.connectors.gaussdb.internal.wal.ReplicationPosition;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalChange;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalReplicationStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Bounded, opt-in HCS integration experiment. It owns only a uniquely named table and a newly
 * created disposable slot. It never acknowledges a replication position.
 *
 * <p>This tests whether an exported JDBC snapshot can be joined to the native three-part stream; it
 * does not enable a production snapshot mode in the connector.
 */
public final class HcsSnapshotHandoffVerification {
    private static final int EXPECTED_CHANGES = 9;
    private static final long STREAM_TIMEOUT_MS = 45_000L;

    private HcsSnapshotHandoffVerification() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "Usage: HcsSnapshotHandoffVerification /path/to/probe.properties");
        }
        Properties source = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get(args[0]))) {
            source.load(input);
        }
        if (!"true".equals(source.getProperty("probe.allow-mutations"))) {
            throw new IllegalArgumentException(
                    "Set probe.allow-mutations=true for this isolated-table integration test");
        }
        Map<String, String> options = new HashMap<>();
        for (String key : source.stringPropertyNames()) {
            options.put(key, source.getProperty(key));
        }
        GaussDBConfig config = new GaussDBConfig(options);
        String schema = identifier(source.getProperty("probe.schema", "public"));
        String slot = identifier(source.getProperty("probe.slot-name"));
        if (slot.equals(config.required("slot.name"))) {
            throw new IllegalArgumentException("The probe must never use the connector's slot");
        }
        String table =
                "hcs_handoff_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        String qualified = schema + "." + table;
        List<String> failures = new ArrayList<>();
        List<String> parallelFailures = new ArrayList<>();
        boolean slotCreated = false;
        boolean tableCreated = false;
        Class.forName("com.huawei.gaussdb.jdbc.Driver");

        try (Connection admin = open(config);
                Connection exporter = open(config);
                Connection importer = open(config);
                Connection writer = open(config);
                Connection longWriter = open(config)) {
            try {
                requireSlotAbsent(admin, slot);
                execute(
                        admin,
                        "CREATE TABLE " + qualified + " (id INTEGER PRIMARY KEY, payload TEXT)");
                tableCreated = true;
                String slotPosition = createSlot(admin, slot);
                slotCreated = true;
                report(
                        "SETUP",
                        true,
                        "table=" + qualified + ", slot=" + slot + ", xlog_position=" + slotPosition,
                        failures);

                execute(
                        admin,
                        "INSERT INTO "
                                + qualified
                                + " VALUES (1,'old'),(2,'delete-me'),(3,'keep')");
                longWriter.setAutoCommit(false);
                execute(longWriter, "INSERT INTO " + qualified + " VALUES (5,'long-transaction')");

                exporter.setAutoCommit(false);
                setRepeatableRead(exporter);
                SnapshotBoundary boundary = exportBoundary(exporter);
                report(
                        "SNAPSHOT_AND_CSN_EXPORT",
                        boundary.csn != null,
                        "snapshot=" + boundary.snapshotId + ", csn=" + boundary.csnText,
                        failures);
                Map<Integer, String> original = readRows(exporter, qualified);
                report(
                        "SNAPSHOT_EXCLUDES_OPEN_TRANSACTION",
                        !original.containsKey(5),
                        original.toString(),
                        failures);

                importer.setAutoCommit(false);
                setRepeatableRead(importer);
                boolean imported = importSnapshot(importer, boundary.snapshotId);
                report(
                        "SNAPSHOT_IMPORT",
                        imported,
                        imported
                                ? "SET TRANSACTION SNAPSHOT succeeded"
                                : "not accepted by this PDB",
                        parallelFailures);
                if (imported) {
                    report(
                            "IMPORTED_SNAPSHOT_EQUALS_EXPORTER",
                            original.equals(readRows(importer, qualified)),
                            "same rows before concurrent writes",
                            parallelFailures);
                }

                execute(writer, "INSERT INTO " + qualified + " VALUES (4,'inserted')");
                execute(writer, "UPDATE " + qualified + " SET payload='updated' WHERE id=1");
                execute(writer, "DELETE FROM " + qualified + " WHERE id=2");
                execute(writer, "UPDATE " + qualified + " SET payload=NULL WHERE id=3");
                execute(writer, "INSERT INTO " + qualified + " VALUES (6,'中文测试')");
                writer.setAutoCommit(false);
                execute(writer, "INSERT INTO " + qualified + " VALUES (7,'rolled-back')");
                writer.rollback();
                writer.setAutoCommit(true);
                longWriter.commit();
                longWriter.setAutoCommit(true);

                report(
                        "EXPORTER_STABLE_AFTER_DML",
                        original.equals(readRows(exporter, qualified)),
                        "INSERT/UPDATE/DELETE/NULL/Unicode and late commit did not change snapshot",
                        failures);
                if (imported) {
                    report(
                            "IMPORTER_STABLE_AFTER_DML",
                            original.equals(readRows(importer, qualified)),
                            "imported reader remained on the same snapshot",
                            parallelFailures);
                }
                Map<Integer, String> current = readRows(admin, qualified);
                Map<Integer, String> expected = new LinkedHashMap<>();
                expected.put(1, "updated");
                expected.put(3, null);
                expected.put(4, "inserted");
                expected.put(5, "long-transaction");
                expected.put(6, "中文测试");
                report(
                        "FRESH_READER_SEES_COMMITS",
                        expected.equals(current),
                        current.toString(),
                        failures);

                List<WalChange> changes =
                        collectStream(
                                config,
                                admin,
                                slot,
                                schema,
                                table,
                                ReplicationPosition.MULTI_PRIMARY_START,
                                EXPECTED_CHANGES);
                assessChanges(changes, boundary.csn, original, expected, failures);
                String resumeFrom = firstPostSnapshotPosition(changes, boundary.csn);
                if (resumeFrom != null) {
                    int laterCount = 0;
                    for (WalChange change : changes) {
                        if (ReplicationPosition.compare(change.getLsn(), resumeFrom) > 0) {
                            laterCount++;
                        }
                    }
                    List<WalChange> resumed =
                            collectStream(
                                    config, admin, slot, schema, table, resumeFrom, laterCount);
                    boolean exactResume = resumed.size() == laterCount;
                    for (WalChange change : resumed) {
                        exactResume &= ReplicationPosition.compare(change.getLsn(), resumeFrom) > 0;
                    }
                    report(
                            "THREE_PART_RESUME",
                            exactResume,
                            "from="
                                    + resumeFrom
                                    + ", expected-later="
                                    + laterCount
                                    + ", actual="
                                    + resumed.size(),
                            failures);
                } else {
                    report(
                            "THREE_PART_RESUME",
                            false,
                            "no post-snapshot commit position found",
                            failures);
                }
            } finally {
                rollbackQuietly(importer);
                rollbackQuietly(exporter);
                rollbackQuietly(longWriter);
                rollbackQuietly(writer);
                if (slotCreated) {
                    try {
                        execute(admin, "SELECT pg_drop_replication_slot('" + slot + "')");
                        System.out.println("CLEANUP slot=" + slot + " dropped");
                    } catch (SQLException e) {
                        failures.add("CLEANUP_SLOT: " + e.getMessage());
                        System.out.println("CLEANUP_SLOT FAILED: " + e.getMessage());
                    }
                }
                if (tableCreated) {
                    try {
                        execute(admin, "DROP TABLE " + qualified);
                        System.out.println("CLEANUP table=" + qualified + " dropped");
                    } catch (SQLException e) {
                        failures.add("CLEANUP_TABLE: " + e.getMessage());
                        System.out.println("CLEANUP_TABLE FAILED: " + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            failures.add(
                    "INTEGRATION_ABORT: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            System.out.println("CASE INTEGRATION_ABORT FAIL " + e.getMessage());
        }
        String verdict =
                failures.isEmpty()
                        ? (parallelFailures.isEmpty()
                                ? "PASS_SINGLE_AND_IMPORTED_EXPERIMENT"
                                : "PASS_SINGLE_READER_EXPERIMENT")
                        : "NOT_PROVEN";
        System.out.println("VERDICT=" + verdict);
        for (String failure : failures) {
            System.out.println("REASON=" + failure);
        }
        for (String failure : parallelFailures) {
            System.out.println("PARALLEL_SNAPSHOT_LIMITATION=" + failure);
        }
        if (!failures.isEmpty()) {
            System.exit(2);
        }
    }

    private static Connection open(GaussDBConfig config) throws SQLException {
        return DriverManager.getConnection(config.jdbcUrl(), config.authentication());
    }

    private static String identifier(String value) {
        if (value == null || !value.matches("[a-z][a-z0-9_]{0,62}")) {
            throw new IllegalArgumentException("Invalid or missing probe identifier: " + value);
        }
        return value;
    }

    private static void requireSlotAbsent(Connection connection, String slot) throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "SELECT 1 FROM pg_get_replication_slots() WHERE slot_name=?")) {
            statement.setString(1, slot);
            try (ResultSet rows = statement.executeQuery()) {
                if (rows.next()) {
                    throw new IllegalArgumentException(
                            "Probe slot already exists; choose a NEW disposable name: " + slot);
                }
            }
        }
    }

    private static String createSlot(Connection connection, String slot) throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "SELECT * FROM pg_create_logical_replication_slot(?, ?)")) {
            statement.setString(1, slot);
            statement.setString(2, "parallel_json_decoding");
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new SQLException("Slot creation returned no row");
                }
                return rows.getString(2);
            }
        }
    }

    private static void setRepeatableRead(Connection connection) throws SQLException {
        execute(connection, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    }

    private static SnapshotBoundary exportBoundary(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet rows =
                        statement.executeQuery(
                                "SELECT * FROM pg_catalog.pg_export_snapshot_and_csn()")) {
            if (!rows.next() || rows.getMetaData().getColumnCount() < 2) {
                throw new SQLException("Snapshot export did not return snapshot ID and CSN");
            }
            String snapshotId = rows.getString(1);
            String csnText = rows.getString(2);
            if (snapshotId == null || !snapshotId.matches("[0-9A-Fa-f-]+")) {
                throw new SQLException("Invalid exported snapshot ID: " + snapshotId);
            }
            return new SnapshotBoundary(snapshotId, csnText, parseHexCsn(csnText));
        }
    }

    static Long parseHexCsn(String text) throws SQLException {
        if (text == null || !text.matches("(?:0[xX])?[0-9A-Fa-f]+")) {
            throw new SQLException("Unrecognized snapshot CSN format: " + text);
        }
        String digits = text.startsWith("0x") || text.startsWith("0X") ? text.substring(2) : text;
        try {
            return Long.parseUnsignedLong(digits, 16);
        } catch (NumberFormatException e) {
            throw new SQLException("Snapshot CSN does not fit 64 bits: " + text, e);
        }
    }

    private static boolean importSnapshot(Connection connection, String snapshotId) {
        try {
            execute(connection, "SET TRANSACTION SNAPSHOT '" + snapshotId + "'");
            return true;
        } catch (SQLException e) {
            System.out.println(
                    "SNAPSHOT_IMPORT_ERROR sqlstate="
                            + e.getSQLState()
                            + " code="
                            + e.getErrorCode()
                            + " message="
                            + e.getMessage());
            rollbackQuietly(connection);
            return false;
        }
    }

    private static Map<Integer, String> readRows(Connection connection, String qualified)
            throws SQLException {
        Map<Integer, String> result = new LinkedHashMap<>();
        try (Statement statement = connection.createStatement();
                ResultSet rows =
                        statement.executeQuery(
                                "SELECT id,payload FROM " + qualified + " ORDER BY id")) {
            while (rows.next()) {
                result.put(rows.getInt(1), rows.getString(2));
            }
        }
        return result;
    }

    private static List<WalChange> collectStream(
            GaussDBConfig config,
            Connection sql,
            String slot,
            String schema,
            String table,
            String startPosition,
            int expectedCount)
            throws Exception {
        WalReplicationStream stream =
                new WalReplicationStream(
                        sql,
                        config.jdbcUrl(),
                        config.required("username"),
                        config.required("password"),
                        slot,
                        "parallel_json_decoding",
                        2,
                        "j",
                        false,
                        100);
        stream.setHcsMultiPrimary(true, config.replicationAuthentication());
        List<WalChange> changes = new ArrayList<>();
        try {
            stream.initialize(startPosition);
            long deadline = System.currentTimeMillis() + STREAM_TIMEOUT_MS;
            while (System.currentTimeMillis() < deadline && changes.size() < expectedCount) {
                for (WalChange change : stream.readChanges(100)) {
                    if (schema.equals(change.getSchema()) && table.equals(change.getTable())) {
                        changes.add(change);
                        System.out.println(
                                "STREAM type="
                                        + change.getType()
                                        + " csn="
                                        + Long.toUnsignedString(change.getCsn())
                                        + " position="
                                        + change.getLsn()
                                        + " before="
                                        + change.getBeforeColumns()
                                        + " after="
                                        + change.getAfterColumns());
                    }
                }
                if (changes.size() < expectedCount) {
                    Thread.sleep(100);
                }
            }
        } finally {
            // Never call acknowledgeLsn(): this slot is disposable and belongs to the probe.
            stream.close();
        }
        return changes;
    }

    private static String firstPostSnapshotPosition(List<WalChange> changes, Long snapshotCsn) {
        if (snapshotCsn == null) return null;
        for (WalChange change : changes) {
            if (Long.compareUnsigned(change.getCsn(), snapshotCsn) > 0) {
                return change.getLsn();
            }
        }
        return null;
    }

    static void assessChanges(
            List<WalChange> changes,
            Long snapshotCsn,
            Map<Integer, String> snapshot,
            Map<Integer, String> expected,
            List<String> failures) {
        report(
                "STREAM_EXPECTED_COUNT",
                changes.size() == EXPECTED_CHANGES,
                "expected=" + EXPECTED_CHANGES + ", actual=" + changes.size(),
                failures);
        Map<Integer, String> replay = new LinkedHashMap<>(snapshot);
        Map<Integer, Integer> counts = new HashMap<>();
        boolean positionsAreTriples = true;
        boolean boundaryMatches = true;
        boolean rollbackExcluded = true;
        boolean unicodeDecoded = false;
        for (WalChange change : changes) {
            String position = change.getLsn();
            try {
                ReplicationPosition.requireMultiPrimary(position);
            } catch (RuntimeException e) {
                positionsAreTriples = false;
            }
            Integer id = idOf(change);
            if (id == null) {
                boundaryMatches = false;
                continue;
            }
            counts.put(id, counts.getOrDefault(id, 0) + 1);
            if (id == 7) rollbackExcluded = false;
            boolean inSnapshot =
                    id >= 1 && id <= 3 && change.getType() == WalChange.ChangeType.INSERT;
            boolean csnBeforeOrEqual =
                    snapshotCsn != null && Long.compareUnsigned(change.getCsn(), snapshotCsn) <= 0;
            if (inSnapshot != csnBeforeOrEqual) boundaryMatches = false;
            if (!csnBeforeOrEqual) {
                if (change.getType() == WalChange.ChangeType.DELETE) {
                    replay.remove(id);
                } else {
                    replay.put(id, payloadOf(change));
                }
            }
            if (id == 6) unicodeDecoded = "中文测试".equals(payloadOf(change));
        }
        report(
                "THREE_PART_POSITIONS",
                positionsAreTriples,
                "all stream positions parsed",
                failures);
        report(
                "SNAPSHOT_CSN_PARTITION",
                boundaryMatches,
                "baseline INSERTs <= snapshot CSN; later commits > snapshot CSN",
                failures);
        report("ROLLBACK_HAS_NO_EVENT", rollbackExcluded, "id=7 absent", failures);
        report("UNICODE_EVENT", unicodeDecoded, "id=6 UTF-8 payload", failures);
        Map<Integer, Integer> expectedCounts = new HashMap<>();
        expectedCounts.put(1, 2);
        expectedCounts.put(2, 2);
        expectedCounts.put(3, 2);
        expectedCounts.put(4, 1);
        expectedCounts.put(5, 1);
        expectedCounts.put(6, 1);
        report("EACH_CHANGE_ONCE", expectedCounts.equals(counts), counts.toString(), failures);
        report(
                "SNAPSHOT_PLUS_WAL_EQUALS_CURRENT",
                expected.equals(replay),
                "replay=" + replay + ", database=" + expected,
                failures);
    }

    private static Integer idOf(WalChange change) {
        List<WalChange.ColumnValue> columns =
                change.getType() == WalChange.ChangeType.DELETE
                        ? change.getBeforeColumns()
                        : change.getAfterColumns();
        for (WalChange.ColumnValue column : columns) {
            if ("id".equals(column.getColumnName())) {
                try {
                    return Integer.parseInt(column.getValue());
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        }
        return null;
    }

    private static String payloadOf(WalChange change) {
        for (WalChange.ColumnValue column : change.getAfterColumns()) {
            if ("payload".equals(column.getColumnName())) {
                return column.isNull() ? null : column.getValue();
            }
        }
        return null;
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static void rollbackQuietly(Connection connection) {
        try {
            if (!connection.getAutoCommit()) {
                connection.rollback();
                connection.setAutoCommit(true);
            }
        } catch (SQLException ignored) {
            // Preserve the original verification error and continue fixture cleanup.
        }
    }

    private static void report(
            String caseName, boolean passed, String detail, List<String> failures) {
        System.out.println("CASE " + caseName + " " + (passed ? "PASS" : "FAIL") + " " + detail);
        if (!passed) failures.add(caseName + ": " + detail);
    }

    private static final class SnapshotBoundary {
        final String snapshotId;
        final String csnText;
        final Long csn;

        SnapshotBoundary(String snapshotId, String csnText, Long csn) {
            this.snapshotId = snapshotId;
            this.csnText = csnText;
            this.csn = csn;
        }
    }
}

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

package com.ververica.cdc.connectors.gaussdb.internal.snapshot;

import com.ververica.cdc.connectors.gaussdb.GaussDBConfig;
import com.ververica.cdc.connectors.gaussdb.internal.wal.ReplicationPosition;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalChange;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalChange.ColumnValue;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * JDBC access used by the initial snapshot; only the explicitly configured signal table is written.
 */
public class JdbcSnapshotAccess {
    private final Connection connection;
    private final GaussDBConfig config;
    private final String[] signal;

    public JdbcSnapshotAccess(Connection connection, GaussDBConfig config) {
        this.connection = connection;
        this.config = config;
        signal = SnapshotTable.signalTable(config.required("scan.snapshot.signal-table"));
    }

    public List<SnapshotTable> discoverTables() throws SQLException {
        List<String[]> names = new ArrayList<>();
        // Capture ordinary user tables only. DDL and partitioned/inherited tables are outside
        // this first implementation; a schema recheck is performed before publishing the image.
        try (PreparedStatement s =
                        prepare(
                                "SELECT n.nspname, c.relname FROM pg_catalog.pg_class c "
                                        + "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                                        + "WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema') "
                                        + "ORDER BY n.nspname, c.relname");
                ResultSet r = s.executeQuery()) {
            while (r.next())
                if (config.matches(r.getString(1), r.getString(2)))
                    names.add(new String[] {r.getString(1), r.getString(2)});
        }
        List<SnapshotTable> result = new ArrayList<>();
        for (String[] name : names) {
            SnapshotTable table = definition(name[0], name[1]);
            for (String key : table.primaryKey) {
                int oid = table.columns.get(key);
                if (oid == 25 || oid == 1043)
                    throw new SQLException(
                            "Initial snapshot business primary keys currently require integer or UUID columns; text collation equality is not verified");
            }
            result.add(table);
        }
        return result;
    }

    public void validateSignalTable() throws SQLException {
        SnapshotTable table = definition(signal[0], signal[1]);
        if (table.columns.size() != 1
                || !table.columns.containsKey("signal_id")
                || !(table.columns.get("signal_id") == 1043 || table.columns.get("signal_id") == 25)
                || !table.primaryKey.equals(java.util.Collections.singletonList("signal_id")))
            throw new SQLException(
                    "Signal table must have exactly one VARCHAR/TEXT primary key column named signal_id");
    }

    private SnapshotTable definition(String schema, String table) throws SQLException {
        Map<String, Integer> columns = new LinkedHashMap<>();
        long relationOid = 0;
        try (PreparedStatement s =
                prepare(
                        "SELECT a.attname, a.atttypid, c.oid, c.parttype, c.relpersistence, "
                                + "EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid = c.oid OR i.inhparent = c.oid) "
                                + "FROM pg_catalog.pg_attribute a "
                                + "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
                                + "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                                + "WHERE n.nspname = ? AND c.relname = ? AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum")) {
            s.setString(1, schema);
            s.setString(2, table);
            try (ResultSet r = s.executeQuery()) {
                while (r.next()) {
                    if (!"n".equals(r.getString(4))
                            || !"p".equals(r.getString(5))
                            || r.getBoolean(6))
                        throw new SQLException(
                                "Initial snapshot supports permanent ordinary tables without partitions or inheritance only");
                    relationOid = r.getLong(3);
                    columns.put(r.getString(1), r.getInt(2));
                }
            }
        }
        Map<Integer, String> keys = new TreeMap<>();
        try (ResultSet r = connection.getMetaData().getPrimaryKeys(null, schema, table)) {
            while (r.next()) keys.put(r.getInt("KEY_SEQ"), r.getString("COLUMN_NAME"));
        }
        return new SnapshotTable(
                schema, table, columns, new ArrayList<>(keys.values()), relationOid);
    }

    public void writeSignal(String token) throws SQLException {
        if (!connection.getAutoCommit())
            throw new SQLException("Signal must be committed outside the scan transaction");
        try (PreparedStatement s =
                prepare("INSERT INTO " + signalName() + " (signal_id) VALUES (?)")) {
            s.setString(1, token);
            if (s.executeUpdate() != 1)
                throw new SQLException("Snapshot signal INSERT did not affect one row");
        }
    }

    public void cleanSignals(String low, String high) throws SQLException {
        try (PreparedStatement s =
                prepare("DELETE FROM " + signalName() + " WHERE signal_id IN (?, ?)")) {
            s.setString(1, low);
            s.setString(2, high);
            s.executeUpdate();
        }
    }

    public boolean isSignal(WalChange row, String token) {
        return row.getType() == WalChange.ChangeType.INSERT
                && config.isSignalTable(row.getSchema(), row.getTable())
                && row.getAfterColumns().stream()
                        .anyMatch(
                                c ->
                                        "signal_id".equals(c.getColumnName())
                                                && !c.isNull()
                                                && token.equals(c.getValue()));
    }

    public void scan(
            List<SnapshotTable> tables, Consumer<WalChange> output, BooleanSupplier running)
            throws SQLException, InterruptedException {
        connection.setAutoCommit(false);
        try {
            for (SnapshotTable table : tables) {
                String columns =
                        table.columns.keySet().stream()
                                .map(SnapshotTable::quote)
                                .collect(java.util.stream.Collectors.joining(", "));
                try (PreparedStatement s =
                        prepare("SELECT " + columns + " FROM " + table.qualified())) {
                    s.setFetchSize(config.positiveInt("scan.snapshot.fetch.size", 1000));
                    try (ResultSet r = s.executeQuery()) {
                        while (r.next()) {
                            if (!running.getAsBoolean() || Thread.currentThread().isInterrupted())
                                throw new InterruptedException("Initial snapshot cancelled");
                            List<ColumnValue> values = new ArrayList<>();
                            int i = 1;
                            for (Map.Entry<String, Integer> column : table.columns.entrySet()) {
                                String value;
                                if (column.getValue() == 17) {
                                    byte[] bytes = r.getBytes(i++);
                                    value = bytes == null ? null : hexBytea(bytes);
                                } else value = r.getString(i++);
                                values.add(
                                        new ColumnValue(
                                                column.getKey(),
                                                column.getValue(),
                                                value,
                                                value == null));
                            }
                            output.accept(
                                    WalChange.insert(
                                            ReplicationPosition.MULTI_PRIMARY_START,
                                            0,
                                            table.schema,
                                            table.table,
                                            values));
                        }
                    }
                }
            }
            connection.commit();
        } finally {
            // Rollback is also safe after successful commit; no open scan transaction survives.
            connection.rollback();
            connection.setAutoCommit(true);
        }
    }

    private static String hexBytea(byte[] bytes) {
        char[] hex = "0123456789abcdef".toCharArray();
        StringBuilder result = new StringBuilder("\\x");
        for (byte value : bytes) result.append(hex[(value & 0xff) >>> 4]).append(hex[value & 0xf]);
        return result.toString();
    }

    private String signalName() {
        return SnapshotTable.quote(signal[0]) + "." + SnapshotTable.quote(signal[1]);
    }

    private PreparedStatement prepare(String sql) throws SQLException {
        PreparedStatement s = connection.prepareStatement(sql);
        s.setQueryTimeout(config.positiveInt("scan.snapshot.timeout.seconds", 300));
        return s;
    }
}

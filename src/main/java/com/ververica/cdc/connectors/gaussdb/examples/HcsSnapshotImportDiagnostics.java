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
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Read-only, two-session capability probe. It does not certify cross-node snapshot/WAL semantics.
 */
public final class HcsSnapshotImportDiagnostics {
    private HcsSnapshotImportDiagnostics() {}

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && "--help".equals(args[0])) {
            System.out.println(
                    "Usage: HcsSnapshotImportDiagnostics <exporter.properties> [importer.properties]");
            System.out.println(
                    "Two fresh SQL sessions in the same tenant/database; read-only transactions, no business reads, slots, streams or ACKs.");
            System.out.println(
                    "Success checks import capability only, not cross-node visibility or CSN/WAL boundary correctness.");
            return;
        }
        if (args.length < 1 || args.length > 2)
            throw new IllegalArgumentException(
                    "Use --help for the read-only snapshot import probe");
        GaussDBConfig exporter = load(args[0]);
        GaussDBConfig importer = args.length == 2 ? load(args[1]) : exporter;
        requireSameDatabase(exporter, importer);
        Class.forName("com.huawei.gaussdb.jdbc.Driver");
        boolean accepted;
        try (Connection a = open(exporter);
                Connection b = open(importer)) {
            accepted = probe(a, b, System.out);
        }
        if (!accepted) System.exit(2);
    }

    private static GaussDBConfig load(String file) throws Exception {
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get(file))) {
            properties.load(input);
        }
        Map<String, String> options = new HashMap<>();
        for (String key : properties.stringPropertyNames())
            options.put(key, properties.getProperty(key));
        return new GaussDBConfig(options);
    }

    static void requireSameDatabase(GaussDBConfig a, GaussDBConfig b) {
        if (!a.required("database").equals(b.required("database"))
                || !a.required("hcs.tenant-name").equals(b.required("hcs.tenant-name")))
            throw new IllegalArgumentException(
                    "Exporter and importer must target the same tenant and database");
    }

    private static Connection open(GaussDBConfig config) throws SQLException {
        Properties properties = config.authentication();
        properties.setProperty("socketTimeout", "30");
        return DriverManager.getConnection(config.jdbcUrl(), properties);
    }

    static boolean probe(Connection exporter, Connection importer, PrintStream out) {
        boolean exporterStarted = false, importerStarted = false, success = false, clean = true;
        String stage = "fresh_sessions";
        try {
            if (exporter == importer || !exporter.getAutoCommit() || !importer.getAutoCommit())
                throw new SQLException("Probe requires two fresh autocommit SQL sessions", "P0001");
            stage = "exporter_begin";
            exporter.setAutoCommit(false);
            exporterStarted = true;
            execute(exporter, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
            requireRepeatableRead(exporter);
            stage = "snapshot_export";
            String id;
            String csn;
            try (PreparedStatement s =
                            prepare(
                                    exporter,
                                    "SELECT * FROM pg_catalog.pg_export_snapshot_and_csn()");
                    ResultSet rows = s.executeQuery()) {
                if (!rows.next() || rows.getMetaData().getColumnCount() < 2)
                    throw new SQLException("Snapshot export must return ID and CSN", "P0001");
                id = rows.getString(1);
                csn = rows.getString(2);
                if (id == null
                        || id.length() > 128
                        || !id.matches("[0-9A-Fa-f-]+")
                        || csn == null
                        || csn.trim().isEmpty())
                    throw new SQLException("Invalid exported snapshot metadata", "P0001");
            }
            out.println("snapshot.export=accepted");
            out.println("snapshot.csn.raw=" + csn);
            stage = "importer_begin";
            importer.setAutoCommit(false);
            importerStarted = true;
            execute(importer, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
            stage = "snapshot_import";
            // Import MUST precede every importer SELECT, including SHOW transaction_isolation.
            // Only a strictly validated server snapshot identifier is interpolated into this SET.
            execute(importer, "SET TRANSACTION SNAPSHOT '" + id + "'");
            stage = "importer_isolation";
            requireRepeatableRead(importer);
            out.println("snapshot.import=accepted");
            success = true;
        } catch (SQLException e) {
            out.println("probe.failed_stage=" + stage);
            out.println("probe.sqlstate=" + e.getSQLState());
            out.println("probe.error_code=" + e.getErrorCode());
        } finally {
            // Keep the exporter alive until all importer checks finish. Never commit either side.
            if (importerStarted) clean = rollback(importer, "importer", out);
            if (exporterStarted) clean = rollback(exporter, "exporter", out) && clean;
            out.println("snapshot.cross_node_visibility=NOT_VERIFIED");
            out.println("snapshot.csn_wal_boundary=NOT_VERIFIED");
            out.println("PROBE_COMPLETE_READ_ONLY_NO_SLOT_MUTATION_NO_STREAM_ACK");
        }
        return success && clean;
    }

    private static void requireRepeatableRead(Connection connection) throws SQLException {
        try (PreparedStatement s = prepare(connection, "SHOW transaction_isolation");
                ResultSet r = s.executeQuery()) {
            if (!r.next() || !"repeatable read".equalsIgnoreCase(r.getString(1)))
                throw new SQLException("Requested REPEATABLE READ was not active", "P0001");
        }
    }

    private static PreparedStatement prepare(Connection connection, String sql)
            throws SQLException {
        PreparedStatement s = connection.prepareStatement(sql);
        s.setQueryTimeout(15);
        return s;
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (PreparedStatement s = prepare(connection, sql)) {
            s.execute();
        }
    }

    private static boolean rollback(Connection connection, String name, PrintStream out) {
        try {
            connection.rollback();
            return true;
        } catch (SQLException e) {
            out.println(
                    name
                            + ".rollback=failed sqlstate="
                            + e.getSQLState()
                            + " code="
                            + e.getErrorCode());
            return false;
        }
    }
}

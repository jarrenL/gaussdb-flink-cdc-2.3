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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Read-only probe for the HCS snapshot/replication handoff; it never starts or ACKs a stream. */
public final class HcsSnapshotBoundaryDiagnostics {
    private HcsSnapshotBoundaryDiagnostics() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "Usage: HcsSnapshotBoundaryDiagnostics /path/to/hcs.properties");
        }
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get(args[0]))) {
            properties.load(input);
        }
        Map<String, String> options = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            options.put(key, properties.getProperty(key));
        }
        GaussDBConfig config = new GaussDBConfig(options);
        Class.forName("com.huawei.gaussdb.jdbc.Driver");
        try (Connection connection =
                DriverManager.getConnection(config.jdbcUrl(), config.authentication())) {
            printSlot(connection, config.required("slot.name"));
            try {
                connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                System.out.println("repeatable_read.request=accepted");
            } catch (SQLException e) {
                // Some GaussDB deployments document only READ COMMITTED. The export function may
                // still be inspectable, but that does not make the following SELECT consistent.
                System.out.println("repeatable_read.request=rejected");
                System.out.println("repeatable_read.sqlstate=" + e.getSQLState());
                System.out.println("repeatable_read.error_code=" + e.getErrorCode());
            }
            connection.setAutoCommit(false);
            try {
                try (Statement statement = connection.createStatement();
                        ResultSet isolation =
                                statement.executeQuery("SHOW transaction_isolation")) {
                    printRows("isolation", isolation);
                }
                try (Statement statement = connection.createStatement();
                        ResultSet snapshot =
                                statement.executeQuery("SELECT pg_export_snapshot_and_csn()")) {
                    printRows("snapshot", snapshot);
                    System.out.println("snapshot.function=available");
                } catch (SQLException e) {
                    System.out.println("snapshot.function=unavailable");
                    System.out.println("snapshot.sqlstate=" + e.getSQLState());
                    System.out.println("snapshot.error_code=" + e.getErrorCode());
                    System.out.println("snapshot.error=" + e.getMessage());
                }
            } finally {
                connection.rollback();
            }
        }
        System.out.println("PROBE_COMPLETE_NO_SLOT_MUTATION_NO_STREAM_ACK");
    }

    private static void printSlot(Connection connection, String slotName) throws SQLException {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "SELECT * FROM pg_get_replication_slots() WHERE slot_name = ?")) {
            statement.setString(1, slotName);
            try (ResultSet slot = statement.executeQuery()) {
                printRows("slot", slot);
            }
        }
    }

    private static void printRows(String prefix, ResultSet rows) throws SQLException {
        ResultSetMetaData metadata = rows.getMetaData();
        int count = 0;
        while (rows.next()) {
            count++;
            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                System.out.println(
                        prefix
                                + "["
                                + count
                                + "]."
                                + metadata.getColumnLabel(i)
                                + "="
                                + rows.getString(i));
            }
        }
        System.out.println(prefix + ".rows=" + count);
    }
}

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

import com.huawei.gaussdb.jdbc.replication.LogSequenceNumber;
import com.ververica.cdc.connectors.gaussdb.GaussDBConfig;
import com.ververica.cdc.connectors.gaussdb.internal.wal.ReplicationPosition;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

/** Initial-mode startup barrier. A restored job must NEVER replace a missing slot. */
public final class HcsSlotLifecycle {
    private HcsSlotLifecycle() {}

    public static void prepare(Connection sql, GaussDBConfig config, String restored)
            throws SQLException {
        boolean exists;
        try (PreparedStatement s =
                sql.prepareStatement("SELECT 1 FROM gs_logical_replication_slot WHERE name = ?")) {
            s.setQueryTimeout(config.positiveInt("connect.timeout.seconds", 15));
            s.setString(1, config.required("slot.name"));
            try (ResultSet r = s.executeQuery()) {
                exists = r.next();
            }
        }
        if (exists) {
            try (PreparedStatement s =
                    sql.prepareStatement(
                            "SELECT plugin, active FROM pg_get_replication_slots() WHERE slot_name = ?")) {
                s.setQueryTimeout(config.positiveInt("connect.timeout.seconds", 15));
                s.setString(1, config.required("slot.name"));
                try (ResultSet r = s.executeQuery()) {
                    if (!r.next())
                        throw new SQLException("Cannot verify HCS slot plugin/active metadata");
                    if (!Arrays.asList(
                                    "parallel_json_decoding",
                                    "parallel_binary_decoding",
                                    "mppdb_decoding")
                            .contains(r.getString(1)))
                        throw new SQLException(
                                "Initial snapshot slot has an unsupported output plugin");
                    boolean active = r.getBoolean(2);
                    if (r.wasNull()) throw new SQLException("Cannot verify HCS slot active state");
                    if (active)
                        throw new SQLException(
                                "Initial snapshot requires an inactive dedicated slot");
                }
            }
        }
        if (!exists) {
            if (restored != null)
                throw new SQLException(
                        "Restored HCS slot is missing; refusing to create a replacement and lose retained WAL");
            if (!"true".equals(config.get("scan.slot.create", "true")))
                throw new SQLException(
                        "Initial snapshot slot is missing and scan.slot.create=false");
            try (PreparedStatement s =
                    sql.prepareStatement(
                            "SELECT * FROM pg_create_logical_replication_slot(?, ?)")) {
                s.setQueryTimeout(config.positiveInt("scan.snapshot.timeout.seconds", 300));
                s.setString(1, config.required("slot.name"));
                s.setString(2, "parallel_json_decoding");
                try (ResultSet r = s.executeQuery()) {
                    if (!r.next()) throw new SQLException("HCS slot creation returned no row");
                    // The returned single xlog_position is NOT a fabricated three-part snapshot
                    // token.
                }
            }
        }
        if (restored != null) validateRestart(sql, config, restored);
    }

    private static void validateRestart(Connection sql, GaussDBConfig config, String restored)
            throws SQLException {
        String query =
                "SELECT restart_csn, wal_id, restart_lsn FROM gs_logical_replication_slot_stream gst "
                        + "LEFT JOIN gs_logical_replication_slot gsl ON gsl.slot_id = gst.slot_id "
                        + "WHERE gsl.name = ? ORDER BY 1 ASC, 2 ASC, 3 ASC LIMIT 1";
        try (PreparedStatement s = sql.prepareStatement(query)) {
            s.setQueryTimeout(config.positiveInt("connect.timeout.seconds", 15));
            s.setString(1, config.required("slot.name"));
            try (ResultSet r = s.executeQuery()) {
                if (!r.next())
                    throw new SQLException(
                            "Cannot verify HCS slot restart boundary for checkpoint recovery");
                String restart =
                        component(r.getString(1))
                                + ","
                                + component(r.getString(2))
                                + ","
                                + component(r.getString(3));
                if (ReplicationPosition.compare(restored, restart) < 0)
                    throw new SQLException(
                            "HCS checkpoint precedes the slot restart boundary; retained WAL cannot cover recovery. Use a new slot and redo the initial snapshot");
            }
        }
    }

    private static String component(String value) throws SQLException {
        try {
            if (value == null || value.contains(",")) throw new IllegalArgumentException();
            if (value.contains("/")) return ReplicationPosition.parse(value).asString();
            return LogSequenceNumber.valueOf(Long.parseUnsignedLong(value)).asString();
        } catch (IllegalArgumentException e) {
            throw new SQLException("Invalid HCS slot restart metadata", e);
        }
    }
}

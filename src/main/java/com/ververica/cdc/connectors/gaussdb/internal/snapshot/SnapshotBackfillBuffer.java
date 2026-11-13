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
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalChange;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalChange.ColumnValue;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Bounded first implementation: merge the entire initial image before publishing any row. */
public final class SnapshotBackfillBuffer {
    private final GaussDBConfig config;
    private final Map<String, SnapshotTable> tables = new LinkedHashMap<>();
    private final Map<List<String>, WalChange> rows = new LinkedHashMap<>();
    private long estimatedBytes;

    public SnapshotBackfillBuffer(GaussDBConfig config, List<SnapshotTable> definitions) {
        this.config = config;
        for (SnapshotTable table : definitions)
            if (tables.put(table.id(), table) != null)
                throw new IllegalArgumentException("Duplicate snapshot table");
        if (tables.isEmpty())
            throw new IllegalArgumentException("No tables matched the initial snapshot selection");
    }

    public void addSnapshotRow(WalChange row) {
        SnapshotTable table = requireTable(row);
        table.requireFullImage(row.getAfterColumns());
        List<String> key = table.key(row.getAfterColumns());
        if (rows.containsKey(key))
            throw new IllegalArgumentException("Duplicate primary key in JDBC snapshot");
        put(key, row);
    }

    public void apply(WalChange change) {
        if (!config.matches(change.getSchema(), change.getTable())) return;
        SnapshotTable table = requireTable(change);
        switch (change.getType()) {
            case INSERT:
                table.requireFullImage(change.getAfterColumns());
                put(table.key(change.getAfterColumns()), change);
                break;
            case UPDATE:
                table.requireFullImage(change.getAfterColumns());
                // Remove the OLD key, including when a transaction changes the primary key.
                remove(table.key(change.getBeforeColumns()));
                put(table.key(change.getAfterColumns()), change);
                break;
            case DELETE:
                remove(table.key(change.getBeforeColumns()));
                break;
            default:
                throw new IllegalArgumentException(
                        "Uncommitted or unsupported change in snapshot backfill");
        }
    }

    public List<WalChange> finish(String highPosition) {
        List<WalChange> output = new ArrayList<>();
        for (WalChange row : rows.values())
            output.add(
                    WalChange.insert(
                            highPosition,
                            0,
                            row.getSchema(),
                            row.getTable(),
                            row.getAfterColumns()));
        return output;
    }

    private SnapshotTable requireTable(WalChange row) {
        SnapshotTable table = tables.get(row.getSchema() + "\0" + row.getTable());
        if (table == null)
            throw new IllegalArgumentException(
                    "Captured table set changed during initial snapshot");
        return table;
    }

    private void remove(List<String> key) {
        WalChange previous = rows.remove(key);
        if (previous != null) estimatedBytes -= size(previous);
    }

    private void put(List<String> key, WalChange value) {
        // Store only the final after image; do not retain the WAL before image/raw payload.
        WalChange row =
                WalChange.insert(
                        value.getLsn(),
                        0,
                        value.getSchema(),
                        value.getTable(),
                        new ArrayList<>(value.getAfterColumns()));
        remove(key);
        rows.put(key, row);
        estimatedBytes += size(row);
        if (rows.size() > config.positiveInt("scan.snapshot.max.rows", 100000)
                || estimatedBytes > config.positiveInt("scan.snapshot.max.bytes", 67108864))
            throw new IllegalStateException(
                    "Initial snapshot buffer limit exceeded; no snapshot rows were published. Reduce the table selection or raise the explicit snapshot limits");
    }

    private static long size(WalChange row) {
        long size = 256L + 2L * (row.getSchema().length() + row.getTable().length());
        for (ColumnValue column : row.getAfterColumns())
            size +=
                    96L
                            + 2L
                                    * (column.getColumnName().length()
                                            + (column.getValue() == null
                                                    ? 0
                                                    : column.getValue().length()));
        return size;
    }
}

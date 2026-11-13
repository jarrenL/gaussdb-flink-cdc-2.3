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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Merge the initial image before publication, using bounded memory or disposable disk storage. */
public final class SnapshotBackfillBuffer implements AutoCloseable {
    private final GaussDBConfig config;
    private final Map<String, SnapshotTable> tables = new LinkedHashMap<>();
    private final Map<List<String>, WalChange> rows = new LinkedHashMap<>();
    private long estimatedBytes;
    private final DiskSnapshotStore disk;
    private boolean closed;

    public SnapshotBackfillBuffer(GaussDBConfig config, List<SnapshotTable> definitions) {
        this.config = config;
        for (SnapshotTable table : definitions)
            if (tables.put(table.id(), table) != null)
                throw new IllegalArgumentException("Duplicate snapshot table");
        if (tables.isEmpty())
            throw new IllegalArgumentException("No tables matched the initial snapshot selection");
        disk =
                "disk".equals(config.get("scan.snapshot.storage", "disk"))
                        ? new DiskSnapshotStore(config)
                        : null;
    }

    public synchronized void addSnapshotRow(WalChange row) {
        requireOpen();
        SnapshotTable table = requireTable(row);
        table.requireFullImage(row.getAfterColumns());
        List<String> key = table.key(row.getAfterColumns());
        if (disk == null ? rows.containsKey(key) : disk.contains(key))
            throw new IllegalArgumentException("Duplicate primary key in JDBC snapshot");
        put(key, row);
    }

    public synchronized void apply(WalChange change) {
        requireOpen();
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

    public synchronized List<WalChange> finish(String highPosition) {
        requireOpen();
        if (disk != null)
            throw new IllegalStateException("Disk snapshots must be drained in bounded batches");
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

    /** Drain the merged image after HIGH. No scan/backfill row is published before that point. */
    public synchronized List<WalChange> drain(String highPosition, int maxRows, int maxBytes) {
        requireOpen();
        if (maxRows <= 0 || maxBytes <= 0)
            throw new IllegalArgumentException("Invalid snapshot batch limits");
        if (disk != null) return disk.drain(highPosition, maxRows, maxBytes);
        List<WalChange> output = new ArrayList<>();
        long bytes = 0;
        Iterator<WalChange> iterator = rows.values().iterator();
        while (iterator.hasNext() && output.size() < maxRows) {
            WalChange row = iterator.next();
            long rowBytes = size(row);
            // A row is indivisible; allow one row larger than the batch byte target.
            if (!output.isEmpty() && bytes + rowBytes > maxBytes) break;
            output.add(
                    WalChange.insert(
                            highPosition,
                            0,
                            row.getSchema(),
                            row.getTable(),
                            row.getAfterColumns()));
            bytes += rowBytes;
            estimatedBytes -= rowBytes;
            iterator.remove();
        }
        return output;
    }

    public synchronized boolean isEmpty() {
        requireOpen();
        return disk == null ? rows.isEmpty() : disk.isEmpty();
    }

    public synchronized void seal() {
        requireOpen();
        if (disk != null) disk.flush();
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        rows.clear();
        if (disk != null) disk.close();
    }

    private void requireOpen() {
        if (closed) throw new IllegalStateException("Snapshot buffer is closed");
    }

    private SnapshotTable requireTable(WalChange row) {
        SnapshotTable table = tables.get(row.getSchema() + "\0" + row.getTable());
        if (table == null)
            throw new IllegalArgumentException(
                    "Captured table set changed during initial snapshot");
        return table;
    }

    private void remove(List<String> key) {
        if (disk != null) {
            disk.remove(key);
            return;
        }
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
        if (disk != null) {
            disk.put(key, row);
            return;
        }
        remove(key);
        rows.put(key, row);
        estimatedBytes += size(row);
        if (rows.size() > config.positiveInt("scan.snapshot.max.rows", 100000)
                || estimatedBytes > config.positiveInt("scan.snapshot.max.bytes", 67108864))
            throw new IllegalStateException(
                    "Initial snapshot buffer limit exceeded; no snapshot rows were published. Reduce the table selection or raise the explicit snapshot limits");
    }

    static long size(WalChange row) {
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

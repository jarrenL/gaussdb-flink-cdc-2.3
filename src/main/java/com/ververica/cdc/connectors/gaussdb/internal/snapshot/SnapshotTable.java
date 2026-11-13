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

import com.ververica.cdc.connectors.gaussdb.internal.wal.WalChange.ColumnValue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** A fixed table shape. Initial snapshots reject absent keys and schema drift. */
public final class SnapshotTable {
    public final String schema;
    public final String table;
    public final long relationOid;
    public final Map<String, Integer> columns;
    public final List<String> primaryKey;

    public SnapshotTable(
            String schema, String table, Map<String, Integer> columns, List<String> primaryKey) {
        this(schema, table, columns, primaryKey, 0);
    }

    public SnapshotTable(
            String schema,
            String table,
            Map<String, Integer> columns,
            List<String> primaryKey,
            long relationOid) {
        this.schema = Objects.requireNonNull(schema);
        this.table = Objects.requireNonNull(table);
        this.relationOid = relationOid;
        this.columns = Collections.unmodifiableMap(new LinkedHashMap<>(columns));
        this.primaryKey = Collections.unmodifiableList(new ArrayList<>(primaryKey));
        if (columns.isEmpty() || primaryKey.isEmpty())
            throw new IllegalArgumentException(
                    "Initial snapshot requires columns and a primary key: " + qualified());
        for (String key : primaryKey) {
            Integer oid = columns.get(key);
            // Canonical equality must agree between JDBC text and WAL text. Other key types
            // (numeric scale, bpchar padding, locale-sensitive types) need their own codecs.
            if (oid == null
                    || !(oid == 20
                            || oid == 21
                            || oid == 23
                            || oid == 25
                            || oid == 1043
                            || oid == 2950))
                throw new IllegalArgumentException(
                        "Unsupported initial snapshot primary-key type: "
                                + qualified()
                                + "."
                                + key);
        }
    }

    public String id() {
        return schema + "\0" + table;
    }

    public String qualified() {
        return quote(schema) + "." + quote(table);
    }

    public static String quote(String identifier) {
        if (identifier == null || identifier.isEmpty() || identifier.indexOf('\0') >= 0)
            throw new IllegalArgumentException("Invalid SQL identifier");
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /** Signal-table configuration deliberately accepts only simple schema.table identifiers. */
    public static String[] signalTable(String value) {
        if (value == null || !value.matches("[A-Za-z_][A-Za-z0-9_]*\\.[A-Za-z_][A-Za-z0-9_]*"))
            throw new IllegalArgumentException(
                    "scan.snapshot.signal-table must be a simple schema.table identifier");
        return value.split("\\.", -1);
    }

    public List<String> key(List<ColumnValue> image) {
        Map<String, ColumnValue> values = index(image);
        List<String> key = new ArrayList<>();
        key.add(id());
        for (String name : primaryKey) {
            ColumnValue col = values.get(name);
            if (col == null
                    || col.isNull()
                    || col.getValue() == null
                    || col.getTypeOid() != columns.get(name))
                throw new IllegalArgumentException(
                        "Missing/null/changed primary key in snapshot or WAL: "
                                + qualified()
                                + "."
                                + name);
            String value = col.getValue();
            int oid = col.getTypeOid();
            if (oid == 20 || oid == 21 || oid == 23) value = Long.toString(Long.parseLong(value));
            if (oid == 2950) value = java.util.UUID.fromString(value).toString();
            key.add(value);
        }
        return key;
    }

    public void requireFullImage(List<ColumnValue> image) {
        Map<String, ColumnValue> values = index(image);
        if (!values.keySet().equals(columns.keySet()))
            throw new IllegalArgumentException(
                    "Initial snapshot backfill requires a complete after image and stable schema: "
                            + qualified());
        for (ColumnValue col : image) {
            if (col.getTypeOid() != columns.get(col.getColumnName())
                    || (!col.isNull() && col.getValue() == null))
                throw new IllegalArgumentException(
                        "Column type/value changed during initial snapshot: " + qualified());
        }
    }

    private static Map<String, ColumnValue> index(List<ColumnValue> image) {
        Map<String, ColumnValue> values = new LinkedHashMap<>();
        for (ColumnValue col : image)
            if (values.put(col.getColumnName(), col) != null)
                throw new IllegalArgumentException("Duplicate column in snapshot/WAL image");
        return values;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SnapshotTable)) return false;
        SnapshotTable t = (SnapshotTable) other;
        return id().equals(t.id())
                && relationOid == t.relationOid
                && columns.equals(t.columns)
                && primaryKey.equals(t.primaryKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id(), relationOid, columns, primaryKey);
    }
}

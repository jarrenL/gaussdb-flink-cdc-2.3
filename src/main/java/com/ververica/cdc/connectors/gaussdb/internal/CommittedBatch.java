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

package com.ververica.cdc.connectors.gaussdb.internal;

import com.ververica.cdc.connectors.gaussdb.GaussDBConfig;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalChange;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/** One framework event contains complete committed transactions and one safe resume token. */
public final class CommittedBatch {
    public static final String POSITION = "gaussdb_position";
    private static final Schema COLUMN =
            SchemaBuilder.struct()
                    .name("gaussdb.internal.Column")
                    .field("name", Schema.STRING_SCHEMA)
                    .field("oid", Schema.INT32_SCHEMA)
                    .field("value", Schema.OPTIONAL_STRING_SCHEMA)
                    .field("is_null", Schema.BOOLEAN_SCHEMA)
                    .build();
    private static final Schema CHANGE =
            SchemaBuilder.struct()
                    .name("gaussdb.internal.Change")
                    .field("schema", Schema.STRING_SCHEMA)
                    .field("table", Schema.STRING_SCHEMA)
                    .field("op", Schema.STRING_SCHEMA)
                    .field("position", Schema.STRING_SCHEMA)
                    .field("xid", Schema.INT64_SCHEMA)
                    .field("before", SchemaBuilder.array(COLUMN).build())
                    .field("after", SchemaBuilder.array(COLUMN).build())
                    .build();
    private static final Schema SOURCE =
            SchemaBuilder.struct()
                    .name("gaussdb.internal.Source")
                    .field("snapshot", Schema.STRING_SCHEMA)
                    .field("ts_ms", Schema.INT64_SCHEMA)
                    .build();
    private static final Schema BATCH =
            SchemaBuilder.struct()
                    .name("gaussdb.internal.CommittedBatch")
                    .field("source", SOURCE)
                    .field("rows", SchemaBuilder.array(CHANGE).build())
                    .build();

    private CommittedBatch() {}

    public static SourceRecord create(
            GaussDBConfig config, List<WalChange> changes, String position) {
        return create(config, changes, position, false);
    }

    public static SourceRecord snapshot(
            GaussDBConfig config, List<WalChange> changes, String position) {
        return create(config, changes, position, true);
    }

    private static SourceRecord create(
            GaussDBConfig config, List<WalChange> changes, String position, boolean snapshot) {
        List<Struct> rows = new ArrayList<>();
        for (WalChange change : changes) {
            if (!config.matches(change.getSchema(), change.getTable())) continue;
            rows.add(
                    new Struct(CHANGE)
                            .put("schema", change.getSchema())
                            .put("table", change.getTable())
                            .put("op", change.getType().name())
                            .put("position", change.getLsn())
                            .put("xid", change.getXid())
                            .put("before", columns(change.getBeforeColumns()))
                            .put("after", columns(change.getAfterColumns())));
        }
        Struct batch =
                new Struct(BATCH)
                        .put(
                                "source",
                                new Struct(SOURCE)
                                        .put("snapshot", snapshot ? "last" : "false")
                                        .put("ts_ms", System.currentTimeMillis()))
                        .put("rows", rows);
        Map<String, Object> offset = new HashMap<>();
        offset.put(POSITION, position);
        if (config.initialSnapshot()) {
            offset.put("gaussdb_snapshot_completed", true);
            offset.put("gaussdb_snapshot_scope", config.snapshotScope());
        }
        return new SourceRecord(config.partition(), offset, "gaussdb.committed", BATCH, batch);
    }

    private static List<Struct> columns(List<WalChange.ColumnValue> columns) {
        List<Struct> result = new ArrayList<>();
        for (WalChange.ColumnValue c : columns)
            result.add(
                    new Struct(COLUMN)
                            .put("name", c.getColumnName())
                            .put("oid", c.getTypeOid())
                            .put("value", c.getValue())
                            .put("is_null", c.isNull()));
        return result;
    }
}

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

import com.ververica.cdc.connectors.gaussdb.internal.wal.ReplicationPosition;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/** Expands a committed batch within one CDC fetcher emission/offset critical section. */
public final class TransactionDeserializer<T> implements DebeziumDeserializationSchema<T> {
    private static final long serialVersionUID = 1L;
    private final DebeziumDeserializationSchema<T> delegate;
    private final List<String> requiredFields;
    private static final Schema SOURCE =
            SchemaBuilder.struct()
                    .name("gaussdb.Source")
                    .field("version", Schema.STRING_SCHEMA)
                    .field("connector", Schema.STRING_SCHEMA)
                    .field("name", Schema.STRING_SCHEMA)
                    .field("db", Schema.STRING_SCHEMA)
                    .field("schema", Schema.STRING_SCHEMA)
                    .field("table", Schema.STRING_SCHEMA)
                    .field("snapshot", Schema.STRING_SCHEMA)
                    .field("ts_ms", Schema.INT64_SCHEMA)
                    .field("position", Schema.STRING_SCHEMA)
                    .build();

    public TransactionDeserializer(
            DebeziumDeserializationSchema<T> delegate, List<String> requiredFields) {
        this.delegate = delegate;
        this.requiredFields = requiredFields;
    }

    @Override
    public void deserialize(SourceRecord batch, Collector<T> out) throws Exception {
        Struct value = (Struct) batch.value();
        for (Struct row : value.<Struct>getArray("rows")) {
            SourceRecord record = rowRecord(batch, row);
            for (String image : new String[] {"before", "after"}) {
                Struct data = ((Struct) record.value()).getStruct(image);
                if (data != null)
                    for (String field : requiredFields) {
                        List<Struct> columns = row.getArray(image);
                        if (columns.stream().noneMatch(c -> c.getString("name").equals(field))) {
                            throw new IllegalArgumentException(
                                    "Missing WAL column "
                                            + field
                                            + " in "
                                            + image
                                            + "; require full row images (REPLICA IDENTITY FULL) or use JSON output for partial images");
                        }
                    }
            }
            delegate.deserialize(record, out);
        }
    }

    public static SourceRecord rowRecord(SourceRecord batch, Struct row) {
        String position =
                ReplicationPosition.requireMultiPrimary(row.getString("position")).asString();
        Map<String, Integer> columns = new LinkedHashMap<>();
        for (String image : new String[] {"before", "after"})
            for (Struct col : row.<Struct>getArray(image)) {
                Integer previous = columns.put(col.getString("name"), col.getInt32("oid"));
                if (previous != null && !previous.equals(col.getInt32("oid")))
                    throw new IllegalArgumentException("Column type changed inside WAL event");
            }
        SchemaBuilder dataBuilder = SchemaBuilder.struct().name("gaussdb.Row").optional();
        columns.forEach((name, oid) -> dataBuilder.field(name, schema(oid)));
        Schema data = dataBuilder.build();
        Schema envelope =
                SchemaBuilder.struct()
                        .name("gaussdb.Envelope")
                        .field("before", data)
                        .field("after", data)
                        .field("source", SOURCE)
                        .field("op", Schema.STRING_SCHEMA)
                        .field("ts_ms", Schema.INT64_SCHEMA)
                        .build();
        String op;
        switch (row.getString("op")) {
            case "INSERT":
                op = "c";
                break;
            case "UPDATE":
                op = "u";
                break;
            case "DELETE":
                op = "d";
                break;
            default:
                throw new IllegalArgumentException("Unsupported WAL operation");
        }
        long now = System.currentTimeMillis();
        Struct source =
                new Struct(SOURCE)
                        .put("version", "0.1.0")
                        .put("connector", "gaussdb")
                        .put("name", "gaussdb")
                        .put("db", batch.sourcePartition().get("database"))
                        .put("schema", row.getString("schema"))
                        .put("table", row.getString("table"))
                        .put("snapshot", "false")
                        .put("ts_ms", now)
                        .put("position", position);
        Struct value =
                new Struct(envelope)
                        .put("before", "c".equals(op) ? null : image(data, row.getArray("before")))
                        .put("after", "d".equals(op) ? null : image(data, row.getArray("after")))
                        .put("source", source)
                        .put("op", op)
                        .put("ts_ms", now);
        return new SourceRecord(
                batch.sourcePartition(),
                Collections.singletonMap(CommittedBatch.POSITION, position),
                "gaussdb." + row.getString("schema") + "." + row.getString("table"),
                envelope,
                value);
    }

    private static Schema schema(int oid) {
        switch (oid) {
            case 16:
                return Schema.OPTIONAL_BOOLEAN_SCHEMA;
            case 21:
                return Schema.OPTIONAL_INT16_SCHEMA;
            case 23:
            case 26:
                return Schema.OPTIONAL_INT32_SCHEMA;
            case 20:
                return Schema.OPTIONAL_INT64_SCHEMA;
            case 700:
                return Schema.OPTIONAL_FLOAT32_SCHEMA;
            case 701:
                return Schema.OPTIONAL_FLOAT64_SCHEMA;
            case 17:
                return Schema.OPTIONAL_BYTES_SCHEMA;
            case 1082:
                return SchemaBuilder.int32().name("io.debezium.time.Date").optional().build();
            case 1083:
                return SchemaBuilder.int64().name("io.debezium.time.MicroTime").optional().build();
            case 1114:
                return SchemaBuilder.int64()
                        .name("io.debezium.time.MicroTimestamp")
                        .optional()
                        .build();
            default:
                return Schema.OPTIONAL_STRING_SCHEMA;
        }
    }

    private static Struct image(Schema schema, List<Struct> columns) {
        if (columns.isEmpty())
            throw new IllegalArgumentException("WAL row image is absent; enable full row images");
        Struct row = new Struct(schema);
        for (Struct col : columns)
            row.put(
                    col.getString("name"),
                    col.getBoolean("is_null")
                            ? null
                            : convert(col.getInt32("oid"), col.getString("value")));
        return row;
    }

    private static Object convert(int oid, String value) {
        if (value == null) throw new IllegalArgumentException("Non-null WAL column has no value");
        switch (oid) {
            case 16:
                if ("t".equalsIgnoreCase(value)
                        || "true".equalsIgnoreCase(value)
                        || "1".equals(value)) return true;
                if ("f".equalsIgnoreCase(value)
                        || "false".equalsIgnoreCase(value)
                        || "0".equals(value)) return false;
                throw new IllegalArgumentException("Invalid WAL boolean");
            case 21:
                return Short.valueOf(value);
            case 23:
            case 26:
                return Integer.valueOf(value);
            case 20:
                return Long.valueOf(value);
            case 700:
                return Float.valueOf(value);
            case 701:
                return Double.valueOf(value);
            case 1082:
                return Math.toIntExact(LocalDate.parse(value).toEpochDay());
            case 1083:
                return LocalTime.parse(value).toNanoOfDay() / 1000;
            case 1114:
                LocalDateTime dt = LocalDateTime.parse(value.replace(' ', 'T'));
                return Math.addExact(
                        Math.multiplyExact(dt.toEpochSecond(ZoneOffset.UTC), 1000000L),
                        dt.getNano() / 1000);
            case 1184:
                return OffsetDateTime.parse(value.replace(' ', 'T')).toInstant().toString();
            case 17:
                if (!value.startsWith("\\x") || (value.length() - 2) % 2 != 0)
                    throw new IllegalArgumentException("Only hex bytea is supported");
                byte[] bytes = new byte[(value.length() - 2) / 2];
                for (int i = 0; i < bytes.length; i++)
                    bytes[i] = (byte) Integer.parseInt(value.substring(2 + i * 2, 4 + i * 2), 16);
                return bytes;
            default:
                return value;
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return delegate.getProducedType();
    }
}

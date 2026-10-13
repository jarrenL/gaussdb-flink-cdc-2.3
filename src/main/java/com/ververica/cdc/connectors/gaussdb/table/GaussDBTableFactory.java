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

package com.ververica.cdc.connectors.gaussdb.table;

import com.ververica.cdc.connectors.gaussdb.GaussDBConfig;
import com.ververica.cdc.connectors.gaussdb.GaussDBSource;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;

/** Flink SQL factory for HCS transparent multi-primary incremental capture. */
public final class GaussDBTableFactory implements DynamicTableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "gaussdb-cdc";
    }

    private Set<ConfigOption<?>> keys(String... names) {
        Set<ConfigOption<?>> keys = new HashSet<>();
        for (String name : names) keys.add(ConfigOptions.key(name).stringType().noDefaultValue());
        return keys;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return keys(
                "hcs.cms-endpoints",
                "database",
                "username",
                "password",
                "hcs.tenant-name",
                "hcs.tenant-password",
                "slot.name",
                "table-name");
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return keys(
                "schema-name",
                "sslmode",
                "connect.timeout.seconds",
                "poll.interval.ms",
                "batch.size",
                "scan.startup.mode",
                "output.format",
                "server-time-zone");
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.createTableFactoryHelper(this, context).validate();
        Map<String, String> options = context.getCatalogTable().getOptions();
        GaussDBConfig config;
        try {
            config = new GaussDBConfig(options);
        } catch (IllegalArgumentException e) {
            throw new ValidationException(e.getMessage(), e);
        }
        String format = config.get("output.format", "row");
        if (!Arrays.asList("row", "json").contains(format))
            throw new ValidationException("output.format must be row or json");
        DataType physical = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        RowType rowType = (RowType) physical.getLogicalType();
        if ("json".equals(format)
                && (rowType.getFieldCount() != 1
                        || rowType.getTypeAt(0).getTypeRoot() != LogicalTypeRoot.VARCHAR))
            throw new ValidationException("JSON output requires one STRING column");
        java.time.ZoneId.of(config.get("server-time-zone", "UTC"));
        return new TableSource(config, physical, format);
    }

    private static final class TableSource implements ScanTableSource {
        private final GaussDBConfig config;
        private final DataType physical;
        private final String format;

        private TableSource(GaussDBConfig config, DataType physical, String format) {
            this.config = config;
            this.physical = physical;
            this.format = format;
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return "json".equals(format) ? ChangelogMode.insertOnly() : ChangelogMode.all();
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
            RowType type = (RowType) physical.getLogicalType();
            DebeziumDeserializationSchema<RowData> deserializer =
                    "json".equals(format)
                            ? new JsonRows(type)
                            : RowDataDebeziumDeserializeSchema.newBuilder()
                                    .setPhysicalRowType(type)
                                    .setResultTypeInfo(InternalTypeInfo.of(type))
                                    .setServerTimeZone(
                                            java.time.ZoneId.of(
                                                    config.get("server-time-zone", "UTC")))
                                    .build();
            return SourceFunctionProvider.of(
                    new GaussDBSource<>(
                            config,
                            deserializer,
                            "json".equals(format) ? Collections.emptyList() : type.getFieldNames()),
                    false);
        }

        @Override
        public DynamicTableSource copy() {
            return new TableSource(config, physical, format);
        }

        @Override
        public String asSummaryString() {
            return "GaussDB HCS CDC 2.3 (experimental incremental)";
        }
    }

    private static final class JsonRows implements DebeziumDeserializationSchema<RowData> {
        private static final long serialVersionUID = 1L;
        private final JsonDebeziumDeserializationSchema delegate =
                new JsonDebeziumDeserializationSchema();
        private final RowType rowType;

        private JsonRows(RowType rowType) {
            this.rowType = rowType;
        }

        @Override
        public void deserialize(SourceRecord record, Collector<RowData> out) throws Exception {
            delegate.deserialize(
                    record,
                    new Collector<String>() {
                        @Override
                        public void collect(String value) {
                            out.collect(GenericRowData.of(StringData.fromString(value)));
                        }

                        @Override
                        public void close() {}
                    });
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return InternalTypeInfo.of(rowType);
        }
    }
}

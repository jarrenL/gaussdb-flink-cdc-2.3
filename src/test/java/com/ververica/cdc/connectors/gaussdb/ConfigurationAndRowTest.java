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

package com.ververica.cdc.connectors.gaussdb;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.ververica.cdc.connectors.gaussdb.internal.CommittedBatch;
import com.ververica.cdc.connectors.gaussdb.internal.TransactionDeserializer;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalChange;
import com.ververica.cdc.connectors.gaussdb.table.GaussDBTableFactory;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import java.util.*;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class ConfigurationAndRowTest {
    static final String POS = "0/64,0/2,0/1000";

    static WalChange.ColumnValue col(String name, int oid, String value) {
        return new WalChange.ColumnValue(name, oid, value, value == null);
    }

    static <T> Collector<T> collector(List<T> output) {
        return new Collector<T>() {
            public void collect(T v) {
                output.add(v);
            }

            public void close() {}
        };
    }

    @Test
    void native507ParsesAllCmsEndpointsAndKeepsCredentialsInProperties() {
        Map<String, String> options = TestConfig.options();
        options.put("hcs.cms-endpoints", " cms-a:8000,192.0.2.1:8001,[2001:db8::1]:9000 ");
        GaussDBConfig config = new GaussDBConfig(options);
        Properties parsed =
                com.huawei.gaussdb.jdbc.Driver.parseURL(config.jdbcUrl(), config.authentication());
        com.huawei.gaussdb.jdbc.util.HostSpec[] hosts =
                com.huawei.gaussdb.jdbc.Driver.getURLHostSpecs(parsed);
        assertThat(hosts).hasSize(3);
        assertThat(hosts[0].getHost()).isEqualTo("cms-a");
        assertThat(hosts[1].getPort()).isEqualTo(8001);
        assertThat(hosts[2].getPort()).isEqualTo(9000);
        assertThat(config.jdbcUrl()).doesNotContain("password");
        assertThat(parsed.getProperty("connectMode")).isEqualTo("auth");
        assertThat(config.authentication().getProperty("replication")).isNull();
        assertThat(config.authentication().getProperty("sslmode")).isEqualTo("require");
        assertThat(config.replicationAuthentication().getProperty("replication"))
                .isEqualTo("database");
        assertThat(config.replicationAuthentication().getProperty("preferQueryMode"))
                .isEqualTo("simple");
        assertThat(config.replicationAuthentication().getProperty("allowConnThreadPool"))
                .isEqualTo("false");
    }

    @Test
    void rejectsBadCmsAndUnsupportedSnapshot() {
        for (String endpoints :
                Arrays.asList(
                        "",
                        "cms",
                        "cms:0",
                        "cms:65536",
                        "cms:8000,",
                        "cms:8000?password=secret",
                        "2001:db8::1:8000")) {
            Map<String, String> o = TestConfig.options();
            o.put("hcs.cms-endpoints", endpoints);
            assertThatThrownBy(() -> new GaussDBConfig(o))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageNotContaining("secret");
        }
        Map<String, String> o = TestConfig.options();
        o.put("scan.startup.mode", "initial");
        assertThatThrownBy(() -> new GaussDBConfig(o)).hasMessageContaining("incremental");
    }

    @Test
    void officialRowDeserializerEmitsInsertUpdatePairDeleteWithTypes() throws Exception {
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.INT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD("active", DataTypes.BOOLEAN()),
                                        DataTypes.FIELD("amount", DataTypes.DECIMAL(10, 2)))
                                .getLogicalType();
        List<WalChange.ColumnValue> before =
                Arrays.asList(
                        col("id", 23, "1"),
                        col("name", 25, "old"),
                        col("active", 16, "t"),
                        col("amount", 1700, "12.30"));
        List<WalChange.ColumnValue> after =
                Arrays.asList(
                        col("id", 23, "1"),
                        col("name", 25, "new"),
                        col("active", 16, "f"),
                        col("amount", 1700, "13.40"));
        List<WalChange> changes =
                Arrays.asList(
                        WalChange.insert(POS, 1, "public", "t", before),
                        WalChange.update(POS, 1, "public", "t", before, after),
                        WalChange.delete(POS, 1, "public", "t", after));
        TransactionDeserializer<RowData> serde =
                new TransactionDeserializer<>(
                        RowDataDebeziumDeserializeSchema.newBuilder()
                                .setPhysicalRowType(type)
                                .setResultTypeInfo(InternalTypeInfo.of(type))
                                .build(),
                        type.getFieldNames());
        List<RowData> rows = new ArrayList<>();
        serde.deserialize(
                CommittedBatch.create(TestConfig.config(), changes, POS), collector(rows));
        assertThat(rows)
                .extracting(RowData::getRowKind)
                .containsExactly(
                        RowKind.INSERT,
                        RowKind.UPDATE_BEFORE,
                        RowKind.UPDATE_AFTER,
                        RowKind.DELETE);
        assertThat(rows.get(0).getBoolean(2)).isTrue();
        assertThat(rows.get(2).getBoolean(2)).isFalse();
        assertThat(rows.get(2).getDecimal(3, 10, 2).toBigDecimal()).isEqualByComparingTo("13.40");
    }

    @Test
    void jsonUsesOfficialCdc23SerializerAndRetainsPosition() throws Exception {
        List<String> rows = new ArrayList<>();
        new TransactionDeserializer<>(
                        new JsonDebeziumDeserializationSchema(), Collections.emptyList())
                .deserialize(
                        CommittedBatch.create(
                                TestConfig.config(),
                                Collections.singletonList(FrameworkCheckpointTest.row(1, POS)),
                                POS),
                        collector(rows));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0)).contains("\"op\":\"c\"", POS, "\"id\":1");
    }

    @Test
    void jsonBeforeDistinguishesMissingOldColumnFromOldNull() throws Exception {
        WalChange missingOldName =
                WalChange.update(
                        POS,
                        1,
                        "public",
                        "t",
                        Collections.singletonList(col("id", 23, "8889")),
                        Arrays.asList(col("id", 23, "8889"), col("name", 25, null)));
        WalChange oldNameWasNull =
                WalChange.update(
                        POS,
                        2,
                        "public",
                        "t",
                        Arrays.asList(col("id", 23, "8890"), col("name", 25, null)),
                        Arrays.asList(col("id", 23, "8890"), col("name", 25, "new")));
        List<String> rows = new ArrayList<>();
        new TransactionDeserializer<>(
                        new JsonDebeziumDeserializationSchema(), Collections.emptyList())
                .deserialize(
                        CommittedBatch.create(
                                TestConfig.config(),
                                Arrays.asList(missingOldName, oldNameWasNull),
                                POS),
                        collector(rows));

        assertThat(rows).hasSize(2);
        assertThat(rows.get(0)).contains("\"before\":{\"id\":8889}");
        assertThat(rows.get(0)).contains("\"after\":{\"id\":8889,\"name\":null}");
        assertThat(rows.get(1)).contains("\"before\":{\"id\":8890,\"name\":null}");
    }

    @Test
    void jsonDeserializerPreservesChineseColumnValue() throws Exception {
        String value = "中文测试_特殊字符!@#$(%^&*()";
        WalChange change =
                WalChange.insert(
                        POS,
                        1,
                        "public",
                        "t",
                        Arrays.asList(col("id", 23, "8890"), col("name", 25, value)));
        List<String> rows = new ArrayList<>();
        new TransactionDeserializer<>(
                        new JsonDebeziumDeserializationSchema(), Collections.emptyList())
                .deserialize(
                        CommittedBatch.create(
                                TestConfig.config(), Collections.singletonList(change), POS),
                        collector(rows));
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0)).contains(value);
    }

    @Test
    void missingTypedColumnsFailInsteadOfFabricatingNulls() {
        RowType type =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.INT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()))
                                .getLogicalType();
        TransactionDeserializer<RowData> serde =
                new TransactionDeserializer<>(
                        RowDataDebeziumDeserializeSchema.newBuilder()
                                .setPhysicalRowType(type)
                                .setResultTypeInfo(InternalTypeInfo.of(type))
                                .build(),
                        type.getFieldNames());
        assertThatThrownBy(
                        () ->
                                serde.deserialize(
                                        CommittedBatch.create(
                                                TestConfig.config(),
                                                Collections.singletonList(
                                                        FrameworkCheckpointTest.row(1, POS)),
                                                POS),
                                        collector(new ArrayList<>())))
                .hasMessageContaining("Missing WAL column name");
    }

    @Test
    void sqlFactoryBuildsOfficialFrameworkSourceAfterCopy() {
        Map<String, String> options = TestConfig.options();
        options.put("connector", "gaussdb-cdc");
        options.put("output.format", "json");
        ResolvedSchema schema = ResolvedSchema.of(Column.physical("event", DataTypes.STRING()));
        CatalogTable table =
                CatalogTable.of(
                        org.apache.flink.table.api.Schema.newBuilder()
                                .fromResolvedSchema(schema)
                                .build(),
                        "test",
                        Collections.emptyList(),
                        options);
        DynamicTableFactory.Context context = mock(DynamicTableFactory.Context.class);
        when(context.getCatalogTable()).thenReturn(new ResolvedCatalogTable(table, schema));
        when(context.getObjectIdentifier()).thenReturn(ObjectIdentifier.of("c", "d", "t"));
        ScanTableSource source =
                (ScanTableSource)
                        new GaussDBTableFactory().createDynamicTableSource(context).copy();
        Object function =
                ((SourceFunctionProvider) source.getScanRuntimeProvider(null))
                        .createSourceFunction();
        assertThat(function)
                .isInstanceOf(GaussDBSource.class)
                .isInstanceOf(DebeziumSourceFunction.class);
    }
}

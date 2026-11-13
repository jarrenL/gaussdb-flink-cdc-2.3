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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.ververica.cdc.connectors.gaussdb.GaussDBConfig;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class HcsSnapshotImportDiagnosticsTest {
    static final class Session {
        final Connection connection = mock(Connection.class);
        String snapshot = "000A-001", isolation = "repeatable read";
        final String name;

        Session(String name, List<String> trace) throws Exception {
            this.name = name;
            when(connection.getAutoCommit()).thenReturn(true);
            when(connection.prepareStatement(anyString()))
                    .thenAnswer(
                            i -> {
                                String sql = i.getArgument(0);
                                PreparedStatement s = mock(PreparedStatement.class);
                                when(s.execute())
                                        .thenAnswer(
                                                call -> {
                                                    trace.add(name + ":" + sql);
                                                    return false;
                                                });
                                when(s.executeQuery())
                                        .thenAnswer(
                                                call -> {
                                                    trace.add(name + ":" + sql);
                                                    ResultSet rows = mock(ResultSet.class);
                                                    when(rows.next()).thenReturn(true, false);
                                                    if (sql.startsWith("SELECT *")) {
                                                        ResultSetMetaData metadata =
                                                                mock(ResultSetMetaData.class);
                                                        when(metadata.getColumnCount())
                                                                .thenReturn(2);
                                                        when(rows.getMetaData())
                                                                .thenReturn(metadata);
                                                        when(rows.getString(1))
                                                                .thenReturn(snapshot);
                                                        when(rows.getString(2))
                                                                .thenReturn("ABC123");
                                                    } else
                                                        when(rows.getString(1))
                                                                .thenReturn(isolation);
                                                    return rows;
                                                });
                                return s;
                            });
            doAnswer(
                            i -> {
                                trace.add(name + ":ROLLBACK");
                                return null;
                            })
                    .when(connection)
                    .rollback();
        }
    }

    @Test
    void importsBeforeImporterQueriesAndKeepsExporterOpenUntilBothChecksFinish() throws Exception {
        List<String> trace = new ArrayList<>();
        Session a = new Session("exporter", trace), b = new Session("importer", trace);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        assertThat(
                        HcsSnapshotImportDiagnostics.probe(
                                a.connection, b.connection, new PrintStream(bytes)))
                .isTrue();
        assertThat(trace)
                .containsExactly(
                        "exporter:SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
                        "exporter:SHOW transaction_isolation",
                        "exporter:SELECT * FROM pg_catalog.pg_export_snapshot_and_csn()",
                        "importer:SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
                        "importer:SET TRANSACTION SNAPSHOT '000A-001'",
                        "importer:SHOW transaction_isolation",
                        "importer:ROLLBACK",
                        "exporter:ROLLBACK");
        assertThat(bytes.toString("UTF-8"))
                .contains(
                        "snapshot.import=accepted",
                        "snapshot.csn.raw=ABC123",
                        "snapshot.cross_node_visibility=NOT_VERIFIED",
                        "snapshot.csn_wal_boundary=NOT_VERIFIED");
        verify(a.connection, never()).commit();
        verify(b.connection, never()).commit();
    }

    @Test
    void failedImportRollsBackBothWithoutFallbackOrPrintingServerExceptionText() throws Exception {
        List<String> trace = new ArrayList<>();
        Session a = new Session("exporter", trace), b = new Session("importer", trace);
        when(b.connection.prepareStatement(startsWith("SET TRANSACTION SNAPSHOT")))
                .thenThrow(new SQLException("private-server-text", "0A000", 99));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        assertThat(
                        HcsSnapshotImportDiagnostics.probe(
                                a.connection, b.connection, new PrintStream(bytes)))
                .isFalse();
        assertThat(bytes.toString("UTF-8"))
                .contains("probe.failed_stage=snapshot_import", "probe.sqlstate=0A000")
                .doesNotContain("private-server-text", "snapshot.import=accepted");
        verify(a.connection).rollback();
        verify(b.connection).rollback();
        verify(b.connection, never()).prepareStatement("SHOW transaction_isolation");
    }

    @Test
    void downgradedIsolationAndUnsafeSnapshotIdsStopBeforeImport() throws Exception {
        List<String> trace = new ArrayList<>();
        Session a = new Session("exporter", trace), b = new Session("importer", trace);
        a.isolation = "read committed";
        assertThat(HcsSnapshotImportDiagnostics.probe(a.connection, b.connection, System.out))
                .isFalse();
        verify(b.connection, never()).setAutoCommit(false);
        Session bad = new Session("exporter", new ArrayList<>()),
                other = new Session("importer", new ArrayList<>());
        bad.snapshot = "snapshot'; SELECT 1; --";
        assertThat(HcsSnapshotImportDiagnostics.probe(bad.connection, other.connection, System.out))
                .isFalse();
        verify(other.connection, never()).setAutoCommit(false);
    }

    @Test
    void existingTransactionsAreNotRolledBackAndDifferentTenantsAreRejected() throws Exception {
        List<String> trace = new ArrayList<>();
        Session a = new Session("exporter", trace), b = new Session("importer", trace);
        when(a.connection.getAutoCommit()).thenReturn(false);
        assertThat(HcsSnapshotImportDiagnostics.probe(a.connection, b.connection, System.out))
                .isFalse();
        verify(a.connection, never()).rollback();
        verify(b.connection, never()).rollback();
        Map<String, String> options = new HashMap<>();
        options.put("hcs.cms-endpoints", "cms:8000");
        options.put("database", "pdb");
        options.put("username", "test");
        options.put("password", "test");
        options.put("hcs.tenant-name", "tenant");
        options.put("hcs.tenant-password", "test");
        options.put("slot.name", "test");
        options.put("table-name", "t");
        GaussDBConfig first = new GaussDBConfig(options);
        options.put("hcs.tenant-name", "other");
        assertThatThrownBy(
                        () ->
                                HcsSnapshotImportDiagnostics.requireSameDatabase(
                                        first, new GaussDBConfig(options)))
                .hasMessageContaining("same tenant and database");
    }
}

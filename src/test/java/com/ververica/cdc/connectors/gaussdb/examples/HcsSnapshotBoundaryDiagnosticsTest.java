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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import org.junit.jupiter.api.Test;

class HcsSnapshotBoundaryDiagnosticsTest {
    private String output(Connection connection) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream original = System.out;
        try (PrintStream capture = new PrintStream(bytes, true, "UTF-8")) {
            System.setOut(capture);
            HcsSnapshotBoundaryDiagnostics.printGtmSettings(connection);
        } finally {
            System.setOut(original);
        }
        return new String(bytes.toByteArray(), StandardCharsets.UTF_8);
    }

    @Test
    void settingsAreReadOnlyLocalEvidenceAndOffDoesNotCertifyConsistency() throws Exception {
        Connection sql = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet rows = mock(ResultSet.class);
        when(sql.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(rows);
        when(rows.next()).thenReturn(true, true, false);
        when(rows.getString(1)).thenReturn("enable_gtm_free", "gtm_option");
        when(rows.getString(2)).thenReturn("off", "2");
        assertThat(output(sql))
                .contains(
                        "gtm.local.enable_gtm_free=off",
                        "gtm.local.gtm_option=2",
                        "not a cluster-wide check",
                        "gtm.read_visibility=NOT_VERIFIED");
        verify(sql)
                .prepareStatement(startsWith("SELECT name, setting FROM pg_catalog.pg_settings"));
        verify(statement).setQueryTimeout(15);
        verify(statement, never()).executeUpdate();
    }

    @Test
    void deniedOrMissingSettingsRemainUnverifiedAndDoNotExposeServerExceptionText()
            throws Exception {
        Connection sql = mock(Connection.class);
        when(sql.prepareStatement(anyString()))
                .thenThrow(new SQLException("sensitive-server-text", "42501", 7));
        assertThat(output(sql))
                .contains(
                        "gtm.settings=unavailable",
                        "gtm.settings.sqlstate=42501",
                        "gtm.read_visibility=NOT_VERIFIED")
                .doesNotContain("sensitive-server-text");
        Connection empty = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(empty.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(mock(ResultSet.class));
        assertThat(output(empty))
                .contains("gtm.settings.rows=0", "gtm.read_visibility=NOT_VERIFIED");
    }
}

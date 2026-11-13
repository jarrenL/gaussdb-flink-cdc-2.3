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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.ververica.cdc.connectors.gaussdb.internal.snapshot.*;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalChange;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class JdbcInitialSnapshotTest {
    @Test
    void scanUsesQuotedIdentifiersCursorAndPreservesNullUnicodeAndBinary() throws Exception {
        Connection sql = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet rows = mock(ResultSet.class);
        when(sql.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(rows);
        when(rows.next()).thenReturn(true, false);
        when(rows.getString(1)).thenReturn("1");
        when(rows.getString(2)).thenReturn("中文");
        when(rows.getBytes(3)).thenReturn(new byte[] {0, (byte) 255});
        Map<String, Integer> columns = new LinkedHashMap<>();
        columns.put("id", 23);
        columns.put("name", 25);
        columns.put("payload", 17);
        SnapshotTable table =
                new SnapshotTable(
                        "schema\"name", "order", columns, Collections.singletonList("id"));
        List<WalChange> image = new ArrayList<>();
        new JdbcSnapshotAccess(sql, InitialSnapshotTest.config())
                .scan(Collections.singletonList(table), image::add, () -> true);
        verify(sql)
                .prepareStatement(
                        "SELECT \"id\", \"name\", \"payload\" FROM \"schema\"\"name\".\"order\"");
        verify(statement).setFetchSize(1000);
        verify(sql).setAutoCommit(false);
        verify(sql).commit();
        verify(sql).rollback();
        verify(sql).setAutoCommit(true);
        verify(sql, never()).setTransactionIsolation(anyInt());
        assertThat(image).hasSize(1);
        assertThat(image.get(0).getAfterColumns())
                .extracting(WalChange.ColumnValue::getValue)
                .containsExactly("1", "中文", "\\x00ff");
    }

    @Test
    void cancellationRollsBackScanAndSignalsMustBeAutocommitted() throws Exception {
        Connection sql = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet rows = mock(ResultSet.class);
        when(sql.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(rows);
        when(rows.next()).thenReturn(true);
        JdbcSnapshotAccess jdbc = new JdbcSnapshotAccess(sql, InitialSnapshotTest.config());
        assertThatThrownBy(
                        () ->
                                jdbc.scan(
                                        Collections.singletonList(InitialSnapshotTest.table()),
                                        row -> {},
                                        () -> false))
                .isInstanceOf(InterruptedException.class);
        verify(sql).rollback();
        verify(sql, never()).commit();
        assertThatThrownBy(() -> jdbc.writeSignal("token"))
                .hasMessageContaining("outside the scan transaction");
        when(sql.getAutoCommit()).thenReturn(true);
        when(statement.executeUpdate()).thenReturn(1);
        jdbc.writeSignal("token");
        verify(sql).prepareStatement("INSERT INTO \"cdc_meta\".\"signals\" (signal_id) VALUES (?)");
        verify(statement).setString(1, "token");
    }

    @Test
    void partitionedTableIsRejectedBeforeAnyScanOrSignalWrite() throws Exception {
        Connection sql = mock(Connection.class);
        PreparedStatement definition = mock(PreparedStatement.class);
        ResultSet columns = mock(ResultSet.class);
        when(sql.prepareStatement(startsWith("SELECT a.attname"))).thenReturn(definition);
        when(definition.executeQuery()).thenReturn(columns);
        when(columns.next()).thenReturn(true);
        when(columns.getString(4)).thenReturn("p");
        assertThatThrownBy(
                        () ->
                                new JdbcSnapshotAccess(sql, InitialSnapshotTest.config())
                                        .validateSignalTable())
                .hasMessageContaining("without partitions");
        verify(sql, never()).prepareStatement(startsWith("SELECT \""));
        verify(sql, never()).prepareStatement(startsWith("INSERT"));
    }
}

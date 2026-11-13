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

import com.ververica.cdc.connectors.gaussdb.internal.snapshot.HcsSlotLifecycle;
import java.sql.*;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class HcsInitialSlotTest {
    static class Database {
        Connection sql = mock(Connection.class);
        PreparedStatement lookup = mock(PreparedStatement.class),
                metadata = mock(PreparedStatement.class),
                create = mock(PreparedStatement.class),
                restart = mock(PreparedStatement.class);
        ResultSet slots = mock(ResultSet.class),
                created = mock(ResultSet.class),
                bounds = mock(ResultSet.class);

        Database(boolean exists) throws Exception {
            when(sql.prepareStatement(contains("SELECT 1 FROM gs_logical_replication_slot WHERE")))
                    .thenReturn(lookup);
            when(sql.prepareStatement(contains("SELECT plugin, active"))).thenReturn(metadata);
            when(sql.prepareStatement(contains("pg_create_logical_replication_slot")))
                    .thenReturn(create);
            when(sql.prepareStatement(contains("SELECT restart_csn"))).thenReturn(restart);
            ResultSet existence = mock(ResultSet.class);
            when(lookup.executeQuery()).thenReturn(existence);
            when(existence.next()).thenReturn(exists);
            when(metadata.executeQuery()).thenReturn(slots);
            when(create.executeQuery()).thenReturn(created);
            when(restart.executeQuery()).thenReturn(bounds);
            when(slots.next()).thenReturn(true);
            when(slots.getString(1)).thenReturn("parallel_json_decoding");
            when(created.next()).thenReturn(true);
            when(bounds.next()).thenReturn(true);
            when(bounds.getString(1)).thenReturn("100");
            when(bounds.getString(2)).thenReturn("1");
            when(bounds.getString(3)).thenReturn("0/100");
        }
    }

    @Test
    void newJobCreatesMissingSlotAndNeverFabricatesSnapshotOffset() throws Exception {
        Database db = new Database(false);
        HcsSlotLifecycle.prepare(db.sql, InitialSnapshotTest.config(), null);
        InOrder order = inOrder(db.lookup, db.create);
        order.verify(db.lookup).executeQuery();
        order.verify(db.create).setQueryTimeout(anyInt());
        order.verify(db.create).setString(1, "cdc_test");
        order.verify(db.create).setString(2, "parallel_json_decoding");
        order.verify(db.create).executeQuery();
        verify(db.created, never()).getString(2);
        verify(db.sql, never()).prepareStatement(contains("drop"));
    }

    @Test
    void restoredJobRefusesToReplaceMissingSlot() throws Exception {
        Database db = new Database(false);
        assertThatThrownBy(
                        () ->
                                HcsSlotLifecycle.prepare(
                                        db.sql,
                                        InitialSnapshotTest.config(),
                                        InitialSnapshotTest.HIGH))
                .hasMessageContaining("refusing to create a replacement");
        verify(db.create, never()).executeQuery();
    }

    @Test
    void activeSlotOrUnknownActiveStateStopsStartup() throws Exception {
        Database db = new Database(true);
        when(db.slots.getBoolean(2)).thenReturn(true);
        assertThatThrownBy(
                        () -> HcsSlotLifecycle.prepare(db.sql, InitialSnapshotTest.config(), null))
                .hasMessageContaining("inactive dedicated slot");
        when(db.slots.getBoolean(2)).thenReturn(false);
        when(db.slots.wasNull()).thenReturn(true);
        assertThatThrownBy(
                        () -> HcsSlotLifecycle.prepare(db.sql, InitialSnapshotTest.config(), null))
                .hasMessageContaining("active state");
        verify(db.create, never()).executeQuery();
    }

    @Test
    void checkpointBeforeRetentionBoundaryIsRejectedAndValidFullTripleIsAccepted()
            throws Exception {
        Database db = new Database(true);
        assertThatThrownBy(
                        () ->
                                HcsSlotLifecycle.prepare(
                                        db.sql, InitialSnapshotTest.config(), "0/63,0/1,0/100"))
                .hasMessageContaining("precedes the slot restart boundary");
        HcsSlotLifecycle.prepare(db.sql, InitialSnapshotTest.config(), InitialSnapshotTest.HIGH);
        verify(db.restart, times(2)).setString(1, "cdc_test");
        verify(db.create, never()).executeQuery();
    }

    @Test
    void absentRestartMetadataOrDisallowedCreationFailsClosed() throws Exception {
        Database db = new Database(true);
        when(db.bounds.next()).thenReturn(false);
        assertThatThrownBy(
                        () ->
                                HcsSlotLifecycle.prepare(
                                        db.sql,
                                        InitialSnapshotTest.config(),
                                        InitialSnapshotTest.HIGH))
                .hasMessageContaining("Cannot verify HCS slot restart boundary");
        Database missing = new Database(false);
        Map<String, String> settings = InitialSnapshotTest.options();
        settings.put("scan.slot.create", "false");
        assertThatThrownBy(
                        () ->
                                HcsSlotLifecycle.prepare(
                                        missing.sql, new GaussDBConfig(settings), null))
                .hasMessageContaining("scan.slot.create=false");
        verify(missing.create, never()).executeQuery();
    }
}

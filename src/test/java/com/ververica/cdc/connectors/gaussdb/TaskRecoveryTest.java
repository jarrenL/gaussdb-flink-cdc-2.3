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

import com.ververica.cdc.connectors.gaussdb.internal.CommittedBatch;
import com.ververica.cdc.connectors.gaussdb.internal.GaussDBSourceTask;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalReplicationStream;
import java.sql.Connection;
import java.util.*;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;

class TaskRecoveryTest {
    @Test
    void replayIsFilteredAndAckRequiresSafeMatchingCheckpoint() throws Exception {
        String first = FrameworkCheckpointTest.FIRST, second = FrameworkCheckpointTest.SECOND;
        WalReplicationStream stream = mock(WalReplicationStream.class);
        GaussDBSourceTask task =
                new GaussDBSourceTask() {
                    protected Connection connect(GaussDBConfig c) {
                        return mock(Connection.class);
                    }

                    protected WalReplicationStream createStream(GaussDBConfig c, Connection sql) {
                        return stream;
                    }
                };
        SourceTaskContext context = mock(SourceTaskContext.class);
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(reader);
        when(reader.offset(anyMap()))
                .thenReturn(Collections.singletonMap(CommittedBatch.POSITION, first));
        task.initialize(context);
        task.start(TestConfig.options());
        verify(stream).initialize(first);
        task.poll();
        when(stream.readChanges(anyInt()))
                .thenReturn(Collections.singletonList(FrameworkCheckpointTest.row(1, first)));
        when(stream.getLastLsn()).thenReturn(first);
        assertThat(task.poll()).isEmpty();
        when(stream.readChanges(anyInt()))
                .thenReturn(
                        Arrays.asList(
                                FrameworkCheckpointTest.row(1, first),
                                FrameworkCheckpointTest.row(2, second)));
        when(stream.getLastLsn()).thenReturn(second);
        SourceRecord event = task.poll().get(0);
        assertThat(((Struct) event.value()).getArray("rows")).hasSize(1);
        verify(stream, never()).acknowledgeLsn(anyString());
        task.commitRecord(event);
        verify(stream).acknowledgeLsn(second);
        task.commitRecord(event);
        verify(stream, times(1)).acknowledgeLsn(second);
        SourceRecord wrong =
                new SourceRecord(
                        Collections.singletonMap("tenant", "other"),
                        event.sourceOffset(),
                        "t",
                        event.valueSchema(),
                        event.value());
        assertThatThrownBy(() -> task.commitRecord(wrong)).hasMessageContaining("another");
        task.stop();
    }

    @Test
    void ordinaryOffsetIsRejectedBeforeConnection() {
        GaussDBSourceTask task = new GaussDBSourceTask();
        SourceTaskContext context = mock(SourceTaskContext.class);
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(reader);
        when(reader.offset(anyMap()))
                .thenReturn(Collections.singletonMap(CommittedBatch.POSITION, "0/10"));
        task.initialize(context);
        assertThatThrownBy(() -> task.start(TestConfig.options()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sourceRejectsCheckpointFromDifferentTenantBeforeStartingEngine() throws Exception {
        com.ververica.cdc.debezium.internal.DebeziumOffset offset =
                new com.ververica.cdc.debezium.internal.DebeziumOffset();
        Map<String, String> partition = new HashMap<>(TestConfig.config().partition());
        partition.put("tenant", "other");
        offset.setSourcePartition(partition);
        offset.setSourceOffset(
                Collections.singletonMap(CommittedBatch.POSITION, FrameworkCheckpointTest.FIRST));
        byte[] bytes =
                com.ververica.cdc.debezium.internal.DebeziumOffsetSerializer.INSTANCE.serialize(
                        offset);
        FrameworkCheckpointTest.MemoryState<byte[]> state =
                new FrameworkCheckpointTest.MemoryState<>();
        state.add(bytes);
        org.apache.flink.runtime.state.FunctionInitializationContext context =
                mock(org.apache.flink.runtime.state.FunctionInitializationContext.class);
        org.apache.flink.api.common.state.OperatorStateStore store =
                mock(org.apache.flink.api.common.state.OperatorStateStore.class);
        when(context.isRestored()).thenReturn(true);
        when(context.getOperatorStateStore()).thenReturn(store);
        when(store.getUnionListState(
                        any(org.apache.flink.api.common.state.ListStateDescriptor.class)))
                .thenReturn(state);
        GaussDBSource<String> source =
                new GaussDBSource<>(
                        TestConfig.config(),
                        new com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema());
        assertThatThrownBy(() -> source.initializeState(context))
                .hasMessageContaining("another GaussDB");
    }
}

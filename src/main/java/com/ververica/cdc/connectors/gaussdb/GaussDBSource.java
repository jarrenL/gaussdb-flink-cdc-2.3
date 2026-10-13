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

import com.ververica.cdc.connectors.gaussdb.internal.TransactionDeserializer;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.Validator;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/** Official CDC 2.3 source function with the GaussDB 507 HCS transport adapter. */
public final class GaussDBSource<T> extends DebeziumSourceFunction<T> {
    private static final long serialVersionUID = 1L;
    private final GaussDBConfig config;

    public GaussDBSource(GaussDBConfig config, DebeziumDeserializationSchema<T> deserializer) {
        this(config, deserializer, Collections.emptyList());
    }

    public GaussDBSource(
            GaussDBConfig config,
            DebeziumDeserializationSchema<T> deserializer,
            List<String> requiredFields) {
        super(
                new TransactionDeserializer<>(Objects.requireNonNull(deserializer), requiredFields),
                config.engineProperties(),
                null,
                Validator.getDefaultValidator());
        this.config = config;
    }

    @Override
    public void initializeState(
            org.apache.flink.runtime.state.FunctionInitializationContext context) throws Exception {
        if (context.isRestored()) {
            org.apache.flink.api.common.state.ListState<byte[]> state =
                    context.getOperatorStateStore()
                            .getUnionListState(
                                    new org.apache.flink.api.common.state.ListStateDescriptor<>(
                                            OFFSETS_STATE_NAME,
                                            org.apache.flink.api.common.typeinfo
                                                    .PrimitiveArrayTypeInfo
                                                    .BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
            for (byte[] bytes : state.get()) {
                com.ververica.cdc.debezium.internal.DebeziumOffset offset =
                        com.ververica.cdc.debezium.internal.DebeziumOffsetSerializer.INSTANCE
                                .deserialize(bytes);
                if (!config.partition().equals(offset.sourcePartition))
                    throw new IllegalArgumentException(
                            "Checkpoint belongs to another GaussDB tenant/database/slot");
                Object position =
                        offset.sourceOffset.get(
                                com.ververica.cdc.connectors.gaussdb.internal.CommittedBatch
                                        .POSITION);
                if (!(position instanceof String))
                    throw new IllegalArgumentException("Checkpoint has no HCS three-part position");
                com.ververica.cdc.connectors.gaussdb.internal.wal.ReplicationPosition
                        .requireMultiPrimary((String) position);
            }
        }
        super.initializeState(context);
    }

    @Override
    public void run(SourceFunction.SourceContext<T> context) throws Exception {
        if (getRuntimeContext().getNumberOfParallelSubtasks() != 1)
            throw new IllegalArgumentException("GaussDB HCS source parallelism must be 1");
        super.run(context);
    }
}

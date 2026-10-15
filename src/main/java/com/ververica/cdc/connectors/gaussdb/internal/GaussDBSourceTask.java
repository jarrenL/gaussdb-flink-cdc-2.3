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
import com.ververica.cdc.connectors.gaussdb.internal.wal.ReplicationPosition;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalChange;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalReplicationStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

/** Reads native HCS transactions; only CDC's completed checkpoints acknowledge the slot. */
public class GaussDBSourceTask extends SourceTask {
    private GaussDBConfig config;
    private Connection connection;
    private WalReplicationStream stream;
    private volatile boolean running;
    private boolean initialProgress;
    private volatile String lastRead;
    private String acknowledged;

    @Override
    public String version() {
        return "0.1.0";
    }

    @Override
    public void start(Map<String, String> properties) {
        config = new GaussDBConfig(properties);
        Map<String, Object> offset = context.offsetStorageReader().offset(config.partition());
        lastRead =
                offset == null ? ReplicationPosition.MULTI_PRIMARY_START : requirePosition(offset);
        acknowledged = lastRead;
        try {
            connection = connect(config);
            stream = createStream(config, connection);
            stream.setHcsMultiPrimary(true, config.replicationAuthentication());
            stream.initialize(lastRead);
            running = true;
            initialProgress = true;
        } catch (Exception e) {
            stop();
            throw new ConnectException("Could not start GaussDB HCS native replication", e);
        }
    }

    protected Connection connect(GaussDBConfig settings) throws Exception {
        Class.forName("com.huawei.gaussdb.jdbc.Driver");
        return DriverManager.getConnection(settings.jdbcUrl(), settings.authentication());
    }

    protected WalReplicationStream createStream(GaussDBConfig settings, Connection sql) {
        return new WalReplicationStream(
                sql,
                settings.jdbcUrl(),
                settings.required("username"),
                settings.required("password"),
                settings.required("slot.name"),
                "parallel_json_decoding",
                2,
                "j",
                false,
                settings.positiveInt("batch.size", 1000));
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (!running) return null;
        // Exit the framework's initial snapshot phase even on an idle database.
        if (initialProgress) {
            initialProgress = false;
            return Collections.singletonList(
                    CommittedBatch.create(config, Collections.emptyList(), lastRead));
        }
        try {
            List<WalChange> changes = stream.readChanges(config.positiveInt("batch.size", 1000));
            String position =
                    ReplicationPosition.requireMultiPrimary(stream.getLastLsn()).asString();
            if (WalReplicationStream.isLsnNewer(position, lastRead)) {
                List<WalChange> fresh =
                        changes.stream()
                                .filter(
                                        change ->
                                                WalReplicationStream.isLsnNewer(
                                                        change.getLsn(), lastRead))
                                .collect(Collectors.toList());
                SourceRecord record = CommittedBatch.create(config, fresh, position);
                lastRead = position;
                return Collections.singletonList(record);
            }
        } catch (Exception e) {
            throw new ConnectException(
                    "GaussDB stream failed; restart from the completed Flink checkpoint", e);
        }
        Thread.sleep(config.positiveInt("poll.interval.ms", 100));
        return Collections.emptyList();
    }

    @Override
    public synchronized void commitRecord(SourceRecord record) {
        if (!running) return;
        if (!config.partition().equals(record.sourcePartition()))
            throw new ConnectException(
                    "Checkpoint belongs to another GaussDB tenant/database/slot");
        String position = requirePosition(record.sourceOffset());
        if (WalReplicationStream.isLsnNewer(position, lastRead))
            throw new ConnectException("Checkpoint is ahead of the delivered GaussDB stream");
        if (!WalReplicationStream.isLsnNewer(position, acknowledged)) return;
        try {
            stream.acknowledgeLsn(position);
            acknowledged = position;
        } catch (Exception e) {
            throw new ConnectException("Failed to acknowledge completed checkpoint", e);
        }
    }

    private static String requirePosition(Map<String, ?> offset) {
        Object position = offset.get(CommittedBatch.POSITION);
        if (!(position instanceof String))
            throw new ConnectException("Missing HCS three-part position in CDC checkpoint");
        return ReplicationPosition.requireMultiPrimary((String) position).asString();
    }

    @Override
    public synchronized void stop() {
        running = false;
        if (stream != null) stream.close();
        if (connection != null)
            try {
                connection.close();
            } catch (Exception ignored) {
                /* closing */
            }
    }
}

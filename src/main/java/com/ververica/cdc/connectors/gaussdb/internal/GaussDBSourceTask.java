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
import com.ververica.cdc.connectors.gaussdb.internal.snapshot.HcsSlotLifecycle;
import com.ververica.cdc.connectors.gaussdb.internal.snapshot.InitialSnapshot;
import com.ververica.cdc.connectors.gaussdb.internal.snapshot.JdbcSnapshotAccess;
import com.ververica.cdc.connectors.gaussdb.internal.snapshot.SnapshotPublication;
import com.ververica.cdc.connectors.gaussdb.internal.wal.ReplicationPosition;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalChange;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalReplicationStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reads native HCS transactions; only CDC's completed checkpoints acknowledge the slot. */
public class GaussDBSourceTask extends SourceTask {
    private static final Logger LOG = LoggerFactory.getLogger(GaussDBSourceTask.class);
    private GaussDBConfig config;
    // Ordinary SQL session only. WalReplicationStream owns a separate COPY connection.
    private Connection sqlConnection;
    private WalReplicationStream stream;
    private ScheduledExecutorService statusExecutor;
    private volatile Exception statusFailure;
    private volatile long lastStatusFeedbackNanos;
    private volatile boolean running;
    private boolean initialProgress;
    private boolean snapshotPending;
    private volatile SnapshotPublication publication;
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
        if (offset != null) config.validateSnapshotOffset(offset);
        if (config.initialSnapshot() && offset == null) config.requireSnapshotExperimentOptIn();
        lastRead =
                offset == null ? ReplicationPosition.MULTI_PRIMARY_START : requirePosition(offset);
        acknowledged = lastRead;
        try {
            sqlConnection = connect(config);
            if (config.initialSnapshot())
                prepareInitialSlot(config, sqlConnection, offset == null ? null : lastRead);
            stream = createStream(config, sqlConnection);
            stream.setHcsMultiPrimary(true, config.replicationAuthentication());
            stream.initialize(lastRead);
            running = true;
            publication = null;
            snapshotPending = config.initialSnapshot() && offset == null;
            initialProgress = true;
            startStatusUpdates();
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

    protected void prepareInitialSlot(GaussDBConfig settings, Connection sql, String restored)
            throws Exception {
        HcsSlotLifecycle.prepare(sql, settings, restored);
    }

    protected JdbcSnapshotAccess createSnapshotAccess(GaussDBConfig settings, Connection sql) {
        return new JdbcSnapshotAccess(sql, settings);
    }

    private void startStatusUpdates() {
        statusFailure = null;
        lastStatusFeedbackNanos = 0;
        statusExecutor = createStatusExecutor();
        int interval = config.positiveInt("replication.status.interval.ms", 1000);
        statusExecutor.scheduleWithFixedDelay(
                () -> {
                    if (!running || statusFailure != null) return;
                    try {
                        // Serialized with native reads and checkpoint ACK inside the stream.
                        // Never set applied/flushed here, and never consume business changes.
                        stream.sendStatus();
                        lastStatusFeedbackNanos = System.nanoTime();
                    } catch (Exception failure) {
                        if (running) {
                            statusFailure = failure;
                            LOG.warn(
                                    "HCS replication status feedback failed ("
                                            + statusDiagnostics(failure)
                                            + ")",
                                    failure);
                        }
                    }
                },
                interval,
                interval,
                TimeUnit.MILLISECONDS);
        LOG.info("HCS replication status feedback scheduled every {} ms", interval);
    }

    protected ScheduledExecutorService createStatusExecutor() {
        return Executors.newSingleThreadScheduledExecutor(
                runnable -> {
                    Thread thread = new Thread(runnable, "gaussdb-cdc-replication-status");
                    thread.setDaemon(true);
                    return thread;
                });
    }

    private boolean isRunningAndHealthy() {
        if (!running) return false;
        Exception failure = statusFailure;
        if (failure != null)
            throw new ConnectException(
                    "HCS replication status feedback failed ("
                            + statusDiagnostics(failure)
                            + "); restart from the completed Flink checkpoint",
                    failure);
        return true;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (!running) return null;
        try {
            isRunningAndHealthy();
        } catch (ConnectException failure) {
            discardPublication(failure);
            throw failure;
        }
        if (snapshotPending || publication != null) {
            InitialSnapshot snapshot = null;
            try {
                SnapshotPublication current = publication;
                if (current == null) {
                    snapshot =
                            new InitialSnapshot(
                                    config,
                                    createSnapshotAccess(config, sqlConnection),
                                    stream,
                                    this::isRunningAndHealthy);
                    current = snapshot.prepare();
                    publication = current;
                    snapshotPending = false;
                    initialProgress = false;
                }
                if (!isRunningAndHealthy() || Thread.currentThread().isInterrupted())
                    throw new InterruptedException("Initial snapshot publication cancelled");
                SourceRecord record = current.next();
                isRunningAndHealthy();
                if (Boolean.TRUE.equals(record.sourceOffset().get("gaussdb_snapshot_completed")))
                    lastRead = requirePosition(record.sourceOffset());
                if (!current.hasNext()) {
                    current.close();
                    publication = null;
                }
                return Collections.singletonList(record);
            } catch (InterruptedException e) {
                discardPublication(e);
                throw e;
            } catch (Exception e) {
                discardPublication(e);
                throw new ConnectException(
                        "HCS initial snapshot failed (phase="
                                + (snapshot == null ? "PUBLICATION" : snapshot.getPhase())
                                + ", "
                                + statusDiagnostics(e)
                                + "); restart from the completed Flink checkpoint",
                        e);
            }
        }
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
                    "GaussDB stream failed ("
                            + statusDiagnostics(e)
                            + "); restart from the completed Flink checkpoint",
                    e);
        }
        Thread.sleep(config.positiveInt("poll.interval.ms", 100));
        return Collections.emptyList();
    }

    private String statusDiagnostics(Throwable failure) {
        long last = lastStatusFeedbackNanos;
        return sqlErrorCode(failure)
                + ", statusFeedbackAgeMs="
                + (last == 0 ? "none" : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - last));
    }

    private static String sqlErrorCode(Throwable failure) {
        // Exception.toString()/printStackTrace() do not normally include SQLException metadata.
        // Preserve the original cause and expose codes without copying SQL/server message text.
        for (int depth = 0; failure != null && depth < 16; depth++, failure = failure.getCause())
            if (failure instanceof SQLException) {
                SQLException sql = (SQLException) failure;
                return "sqlState=" + sql.getSQLState() + ", errorCode=" + sql.getErrorCode();
            }
        return "sqlState=unavailable";
    }

    @Override
    public synchronized void commitRecord(SourceRecord record) {
        if (!isRunningAndHealthy()) return;
        if (!config.partition().equals(record.sourcePartition()))
            throw new ConnectException(
                    "Checkpoint belongs to another GaussDB tenant/database/slot");
        config.validateSnapshotOffset(record.sourceOffset());
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

    private void discardPublication(Throwable failure) {
        SnapshotPublication pending = publication;
        publication = null;
        if (pending != null)
            try {
                pending.close();
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
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
        if (statusExecutor != null) {
            statusExecutor.shutdownNow();
            statusExecutor = null;
        }
        SnapshotPublication pending = publication;
        publication = null;
        if (pending != null)
            try {
                pending.close();
            } catch (RuntimeException e) {
                LOG.warn("Could not clean snapshot scratch state", e);
            }
        if (stream != null) stream.close();
        if (sqlConnection != null)
            try {
                sqlConnection.close();
            } catch (Exception ignored) {
                /* closing */
            }
    }
}

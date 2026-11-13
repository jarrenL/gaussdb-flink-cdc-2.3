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

package com.ververica.cdc.connectors.gaussdb.internal.snapshot;

import com.ververica.cdc.connectors.gaussdb.GaussDBConfig;
import com.ververica.cdc.connectors.gaussdb.internal.wal.ReplicationPosition;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalChange;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalReplicationStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LOW signal -> bounded JDBC image -> HIGH signal -> source-side WAL backfill -> publish. The two
 * signals are observed COMMITTED events in the same native multi-primary stream. No
 * current-CSN/local-LSN guess, snapshot import or sink conflict feedback is used.
 */
public final class InitialSnapshot {
    private static final Logger LOG = LoggerFactory.getLogger(InitialSnapshot.class);
    private final GaussDBConfig config;
    private final JdbcSnapshotAccess jdbc;
    private final WalReplicationStream stream;
    private final BooleanSupplier running;
    private long deadline;
    private String observed = ReplicationPosition.MULTI_PRIMARY_START;
    private String phase = "VALIDATING_TABLES";

    public InitialSnapshot(
            GaussDBConfig config,
            JdbcSnapshotAccess jdbc,
            WalReplicationStream stream,
            BooleanSupplier running) {
        this.config = config;
        this.jdbc = jdbc;
        this.stream = stream;
        this.running = running;
    }

    public SnapshotPublication prepare() throws Exception {
        config.requireSnapshotExperimentOptIn();
        deadline =
                System.nanoTime()
                        + TimeUnit.SECONDS.toNanos(
                                config.positiveInt("scan.snapshot.timeout.seconds", 300));
        jdbc.validateSignalTable();
        List<SnapshotTable> tables = jdbc.discoverTables();
        SnapshotBackfillBuffer buffer = new SnapshotBackfillBuffer(config, tables);
        try {
            LOG.info(
                    "Starting HCS initial snapshot for {} table(s); waiting for LOW signal",
                    tables.size());
            String low = "flink_low_" + UUID.randomUUID();
            String high = "flink_high_" + UUID.randomUUID();
            phase = "WRITE_LOW";
            checkRunning();
            jdbc.writeSignal(low);
            phase = "WAIT_LOW";
            Boundary before = awaitSignal(low, null, Collections.emptyList());
            LOG.info(
                    "HCS initial snapshot LOW committed at {}; starting JDBC scan",
                    before.position);
            // Do not apply interval WAL before all scan rows are loaded: an older JDBC image could
            // otherwise overwrite a newer WAL value. The slot is not acknowledged during the scan.
            phase = "SCAN";
            jdbc.scan(
                    tables,
                    row -> {
                        checkDeadline();
                        buffer.addSnapshotRow(row);
                    },
                    running);
            checkRunning();
            phase = "WRITE_HIGH";
            jdbc.writeSignal(high);
            LOG.info("HCS JDBC scan completed; waiting for HIGH and backfilling interval WAL");
            phase = "WAIT_HIGH";
            Boundary after = awaitSignal(high, buffer, before.trailing);
            if (ReplicationPosition.compare(after.position, before.position) <= 0)
                throw new IllegalStateException(
                        "Snapshot HIGH marker did not follow LOW in the native stream");
            phase = "VERIFY_TABLES";
            if (!tables.equals(jdbc.discoverTables()))
                throw new IllegalStateException(
                        "Captured tables or schema changed during initial snapshot; restart with stable DDL");
            checkRunning();
            phase = "CLEANUP";
            jdbc.cleanSignals(low, high);
            buffer.seal();
            LOG.info(
                    "HCS initial snapshot backfill completed at {}; publishing bounded snapshot batches",
                    after.position);
            phase = "PUBLICATION";
            return new SnapshotPublication(
                    config, buffer, after.position, observed, after.trailing);
        } catch (Exception | Error failure) {
            try {
                buffer.close();
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /** Included in task errors even when INFO logging is disabled. Contains no credentials. */
    public String getPhase() {
        return phase;
    }

    private Boundary awaitSignal(
            String token, SnapshotBackfillBuffer buffer, List<WalChange> pending) throws Exception {
        List<WalChange> changes = pending;
        while (true) {
            checkRunning();
            String boundary = null;
            for (WalChange change : changes)
                if (jdbc.isSignal(change, token)) {
                    if (boundary != null && !boundary.equals(change.getLsn()))
                        throw new IllegalStateException(
                                "Duplicate snapshot signal in different transactions");
                    boundary = change.getLsn();
                }
            if (boundary != null) {
                List<WalChange> trailing = new ArrayList<>();
                for (WalChange change : changes) {
                    if (ReplicationPosition.compare(change.getLsn(), boundary) > 0)
                        trailing.add(change);
                    else if (buffer != null) buffer.apply(change);
                }
                return new Boundary(boundary, trailing);
            }
            if (buffer != null) for (WalChange change : changes) buffer.apply(change);
            changes = readOrdered();
            if (changes.isEmpty()) Thread.sleep(config.positiveInt("poll.interval.ms", 100));
        }
    }

    private List<WalChange> readOrdered() throws Exception {
        List<WalChange> changes = stream.readChanges(config.positiveInt("batch.size", 1000));
        String last = observed;
        for (WalChange change : changes) {
            ReplicationPosition.requireMultiPrimary(change.getLsn());
            if (ReplicationPosition.compare(change.getLsn(), last) < 0)
                throw new IllegalStateException(
                        "HCS stream is not ordered by committed three-part position");
            last = change.getLsn();
        }
        String position = ReplicationPosition.requireMultiPrimary(stream.getLastLsn()).asString();
        if (ReplicationPosition.compare(position, last) < 0)
            throw new IllegalStateException(
                    "HCS stream safe offset moved behind its committed events");
        observed = position;
        return changes;
    }

    private void checkRunning() throws InterruptedException {
        if (!running.getAsBoolean() || Thread.currentThread().isInterrupted())
            throw new InterruptedException("Initial snapshot cancelled");
        checkDeadline();
    }

    private void checkDeadline() {
        if (System.nanoTime() - deadline >= 0)
            throw new IllegalStateException(
                    "Initial snapshot timed out before a verified HIGH boundary; no snapshot rows were published");
    }

    private static final class Boundary {
        final String position;
        final List<WalChange> trailing;

        Boundary(String position, List<WalChange> trailing) {
            this.position = position;
            this.trailing = trailing;
        }
    }
}

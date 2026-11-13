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
import com.ververica.cdc.connectors.gaussdb.internal.CommittedBatch;
import com.ververica.cdc.connectors.gaussdb.internal.wal.ReplicationPosition;
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalChange;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Publishes a fully backfilled image in bounded batches. CDC's snapshot=true records keep the
 * checkpoint lock held until the final snapshot=last record and its completed offset are emitted.
 */
public final class SnapshotPublication implements AutoCloseable {
    private final GaussDBConfig config;
    private final SnapshotBackfillBuffer buffer;
    private final String high;
    private final String observed;
    private List<WalChange> trailing;
    private boolean snapshotComplete;
    private boolean closed;
    private boolean trailingPending;

    SnapshotPublication(
            GaussDBConfig config,
            SnapshotBackfillBuffer buffer,
            String high,
            String observed,
            List<WalChange> trailing) {
        this.config = config;
        this.buffer = buffer;
        this.high = high;
        this.observed = observed;
        this.trailing = trailing;
        this.trailingPending = ReplicationPosition.compare(observed, high) > 0;
    }

    public synchronized boolean hasNext() {
        return !closed && (!snapshotComplete || trailingPending);
    }

    public synchronized SourceRecord next() {
        if (closed) throw new IllegalStateException("Snapshot publication is closed");
        if (!snapshotComplete) {
            List<WalChange> rows =
                    buffer.drain(
                            high,
                            config.positiveInt("scan.snapshot.batch.rows", 1000),
                            config.positiveInt("scan.snapshot.batch.bytes", 1048576));
            snapshotComplete = buffer.isEmpty();
            SourceRecord record = CommittedBatch.snapshot(config, rows, high, snapshotComplete);
            if (snapshotComplete) buffer.close();
            return record;
        }
        if (trailingPending) {
            SourceRecord record = CommittedBatch.create(config, trailing, observed);
            trailingPending = false;
            trailing = null;
            return record;
        }
        throw new NoSuchElementException("Snapshot publication is complete");
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        trailing = null;
        trailingPending = false;
        buffer.close();
    }
}

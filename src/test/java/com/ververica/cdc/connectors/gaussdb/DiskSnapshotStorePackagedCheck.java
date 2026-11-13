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

import com.ververica.cdc.connectors.gaussdb.internal.snapshot.*;
import com.ververica.cdc.connectors.gaussdb.internal.wal.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;

/** Standalone packaged/Java 8 small-heap check. Does not connect to a database. */
public final class DiskSnapshotStorePackagedCheck {
    public static void main(String[] args) throws Exception {
        Path parent = Files.createTempDirectory("gaussdb-cdc-packaged-check-");
        try {
            Map<String, String> options = new HashMap<>();
            options.put("hcs.cms-endpoints", "cms:8000");
            options.put("database", "pdb");
            options.put("username", "test");
            options.put("password", "test");
            options.put("hcs.tenant-name", "tenant");
            options.put("hcs.tenant-password", "test");
            options.put("slot.name", "test");
            options.put("table-name", "t");
            options.put("scan.snapshot.spill-dir", parent.toString());
            options.put("scan.snapshot.disk.cache.mb", "8");
            options.put("scan.snapshot.disk.write-buffer.bytes", "262144");
            options.put("scan.snapshot.max.rows", "1");
            options.put("scan.snapshot.max.bytes", "1");
            Map<String, Integer> columns = new LinkedHashMap<>();
            columns.put("id", 23);
            columns.put("payload", 25);
            SnapshotTable table =
                    new SnapshotTable("public", "t", columns, Collections.singletonList("id"));
            char[] chars = new char[1024];
            Arrays.fill(chars, '中');
            String payload = new String(chars);
            int total = args.length == 0 ? 50000 : Integer.parseInt(args[0]);
            try (SnapshotBackfillBuffer buffer =
                    new SnapshotBackfillBuffer(
                            new GaussDBConfig(options), Collections.singletonList(table))) {
                for (int id = 1; id <= total; id++)
                    buffer.addSnapshotRow(
                            WalChange.insert(
                                    ReplicationPosition.MULTI_PRIMARY_START,
                                    0,
                                    "public",
                                    "t",
                                    Arrays.asList(
                                            new WalChange.ColumnValue(
                                                    "id", 23, String.valueOf(id), false),
                                            new WalChange.ColumnValue(
                                                    "payload", 25, payload, false))));
                buffer.seal();
                long fileBytes = 0;
                try (Stream<Path> paths = Files.walk(parent)) {
                    for (Path path : (Iterable<Path>) paths::iterator)
                        if (Files.isRegularFile(path)) fileBytes += Files.size(path);
                }
                int count = 0;
                while (!buffer.isEmpty()) {
                    List<WalChange> batch = buffer.drain("0/64,0/1,0/100", 512, 1048576);
                    for (WalChange row : batch)
                        if (!payload.equals(row.getAfterColumns().get(1).getValue()))
                            throw new AssertionError("Payload changed");
                    count += batch.size();
                }
                if (count != total) throw new AssertionError("Rows lost: " + count);
                System.out.println(
                        "PACKAGED_DISK_MERGE_PASS rows="
                                + count
                                + " file_bytes="
                                + fileBytes
                                + " max_heap="
                                + Runtime.getRuntime().maxMemory());
            }
            try (Stream<Path> paths = Files.list(parent)) {
                if (paths.findAny().isPresent()) throw new AssertionError("Scratch files leaked");
            }
        } finally {
            Files.delete(parent);
        }
    }
}

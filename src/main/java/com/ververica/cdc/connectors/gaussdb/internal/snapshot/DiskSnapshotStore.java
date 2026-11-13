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
import com.ververica.cdc.connectors.gaussdb.internal.wal.WalChange.ColumnValue;
import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

/** Private, disposable merge state. These files are NEVER treated as a completed checkpoint. */
final class DiskSnapshotStore implements AutoCloseable {
    private final GaussDBConfig config;
    private final Path directory;
    private final Path file;
    private MVStore store;
    private MVMap<String, byte[]> rows;
    private long logicalBytes;
    private long pendingBytes;
    private int pendingWrites;
    private boolean closed;

    DiskSnapshotStore(GaussDBConfig config) {
        this.config = config;
        Path owned = null;
        try {
            Path parent =
                    Paths.get(
                            config.get(
                                    "scan.snapshot.spill-dir",
                                    System.getProperty("java.io.tmpdir")));
            owned =
                    FileSystems.getDefault().supportedFileAttributeViews().contains("posix")
                            ? Files.createTempDirectory(
                                    parent,
                                    "gaussdb-cdc-snapshot-",
                                    PosixFilePermissions.asFileAttribute(
                                            PosixFilePermissions.fromString("rwx------")))
                            : Files.createTempDirectory(parent, "gaussdb-cdc-snapshot-");
            directory = owned;
            file = directory.resolve("rows.mv");
            store =
                    new MVStore.Builder()
                            .fileName(file.toString())
                            .cacheSize(config.positiveInt("scan.snapshot.disk.cache.mb", 16))
                            .autoCommitBufferSize(1024)
                            .autoCommitDisabled()
                            .open();
            store.setRetentionTime(0);
            store.setVersionsToKeep(0);
            rows = store.openMap("rows");
            flush();
        } catch (Exception e) {
            if (store != null)
                try {
                    store.closeImmediately();
                } catch (Exception close) {
                    e.addSuppressed(close);
                }
            if (owned != null)
                try {
                    deleteOwned(owned);
                } catch (Exception close) {
                    e.addSuppressed(close);
                }
            throw new IllegalStateException("Could not create private disk snapshot store", e);
        }
    }

    boolean contains(List<String> key) {
        return rows.containsKey(key(key));
    }

    boolean isEmpty() {
        return rows.isEmpty();
    }

    Path directory() {
        return directory;
    }

    void put(List<String> key, WalChange row) {
        if (SnapshotBackfillBuffer.size(row)
                > config.positiveInt("scan.snapshot.disk.max.row.bytes", 16777216))
            throw new IllegalStateException(
                    "Snapshot row exceeds scan.snapshot.disk.max.row.bytes");
        byte[] encoded = encode(row);
        if (encoded.length > config.positiveInt("scan.snapshot.disk.max.row.bytes", 16777216))
            throw new IllegalStateException("Encoded snapshot row exceeds disk row limit");
        byte[] previous = rows.put(key(key), encoded);
        logicalBytes += encoded.length - (previous == null ? 0 : previous.length);
        if (logicalBytes > config.positiveLong("scan.snapshot.disk.max.bytes", 10737418240L))
            throw new IllegalStateException("Disk snapshot quota exceeded");
        changed(encoded.length);
    }

    void remove(List<String> key) {
        removeEncoded(key(key));
    }

    private void removeEncoded(String key) {
        byte[] previous = rows.remove(key);
        if (previous != null) {
            logicalBytes -= previous.length;
            changed(0);
        }
    }

    List<WalChange> drain(String high, int maxRows, int maxBytes) {
        List<WalChange> output = new ArrayList<>();
        List<String> remove = new ArrayList<>();
        long bytes = 0;
        Iterator<Map.Entry<String, byte[]>> iterator = rows.entrySet().iterator();
        while (iterator.hasNext() && output.size() < maxRows) {
            Map.Entry<String, byte[]> entry = iterator.next();
            WalChange row = decode(entry.getValue());
            long rowBytes = SnapshotBackfillBuffer.size(row);
            if (!output.isEmpty() && bytes + rowBytes > maxBytes) break;
            output.add(
                    WalChange.insert(
                            high, 0, row.getSchema(), row.getTable(), row.getAfterColumns()));
            remove.add(entry.getKey());
            bytes += rowBytes;
        }
        for (String key : remove) removeEncoded(key);
        flush();
        return output;
    }

    void flush() {
        store.commit();
        pendingBytes = 0;
        pendingWrites = 0;
        try {
            if (Files.size(file)
                    > config.positiveLong("scan.snapshot.disk.max.bytes", 10737418240L))
                throw new IllegalStateException("Physical disk snapshot quota exceeded");
        } catch (IOException e) {
            throw new UncheckedIOException("Could not inspect disk snapshot size", e);
        }
    }

    private void changed(int bytes) {
        pendingBytes += bytes;
        if (++pendingWrites >= 1000
                || pendingBytes
                        >= config.positiveInt("scan.snapshot.disk.write-buffer.bytes", 1048576))
            flush();
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        RuntimeException failure = null;
        // Scratch state is reconstructed from the source after failure, never resumed from local
        // files.
        try {
            store.closeImmediately();
        } catch (RuntimeException e) {
            failure = e;
        }
        try {
            deleteOwned(directory);
        } catch (IOException e) {
            RuntimeException close =
                    new UncheckedIOException("Could not remove private snapshot scratch files", e);
            if (failure == null) failure = close;
            else failure.addSuppressed(close);
        }
        if (failure != null) throw failure;
    }

    private static void deleteOwned(Path directory) throws IOException {
        if (!Files.exists(directory)) return;
        List<Path> paths;
        try (Stream<Path> stream = Files.walk(directory)) {
            paths = stream.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
        }
        for (Path path : paths) Files.deleteIfExists(path);
    }

    private static String key(List<String> parts) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeInt(parts.size());
                for (String part : parts) write(out, part);
            }
            return Base64.getEncoder().encodeToString(bytes.toByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static byte[] encode(WalChange row) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeByte(1);
                write(out, row.getSchema());
                write(out, row.getTable());
                out.writeInt(row.getAfterColumns().size());
                for (ColumnValue col : row.getAfterColumns()) {
                    write(out, col.getColumnName());
                    out.writeInt(col.getTypeOid());
                    out.writeBoolean(col.isNull());
                    if (!col.isNull()) write(out, col.getValue());
                }
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private WalChange decode(byte[] bytes) {
        if (bytes.length > config.positiveInt("scan.snapshot.disk.max.row.bytes", 16777216))
            throw new IllegalStateException("Stored snapshot row exceeds disk row limit");
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (in.readUnsignedByte() != 1) throw new IOException("Unknown snapshot row format");
            String schema = read(in), table = read(in);
            int count = in.readInt();
            if (count < 0 || count > in.available() / 9)
                throw new IOException("Invalid column count");
            List<ColumnValue> columns = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                String name = read(in);
                int oid = in.readInt();
                boolean isNull = in.readBoolean();
                columns.add(new ColumnValue(name, oid, isNull ? null : read(in), isNull));
            }
            if (in.available() != 0) throw new IOException("Unexpected bytes after snapshot row");
            return WalChange.insert(
                    ReplicationPosition.MULTI_PRIMARY_START, 0, schema, table, columns);
        } catch (IOException e) {
            throw new UncheckedIOException("Invalid disk snapshot row", e);
        }
    }

    // Encode Java UTF-16 code units without writeUTF's 64 KiB limit or lossy replacement.
    private static void write(DataOutputStream out, String text) throws IOException {
        out.writeInt(text.length());
        for (int i = 0; i < text.length(); i++) out.writeChar(text.charAt(i));
    }

    private static String read(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > in.available() / 2)
            throw new IOException("Invalid snapshot text length");
        char[] text = new char[length];
        for (int i = 0; i < length; i++) text[i] = in.readChar();
        return new String(text);
    }
}

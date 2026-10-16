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

package com.ververica.cdc.connectors.gaussdb.examples;

import static org.assertj.core.api.Assertions.assertThat;

import com.ververica.cdc.connectors.gaussdb.internal.wal.WalChange;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HcsSnapshotHandoffVerificationTest {
    @Test
    void parsesExportedHexCsnIncludingUnsignedValues() throws Exception {
        assertThat(HcsSnapshotHandoffVerification.parseHexCsn("D190E6024")).isEqualTo(56254947364L);
        assertThat(HcsSnapshotHandoffVerification.parseHexCsn("0xA")).isEqualTo(10L);
    }

    @Test
    void detectsSnapshotBoundaryLossAndDuplicateEvents() {
        List<WalChange> changes = fixture();
        List<String> failures = assess(changes);
        assertThat(failures).isEmpty();

        changes.get(3).setCsn(10);
        assertThat(assess(changes)).anyMatch(value -> value.startsWith("SNAPSHOT_CSN_PARTITION"));
        changes.get(3).setCsn(11);
        changes.add(changes.get(3));
        assertThat(assess(changes)).anyMatch(value -> value.startsWith("EACH_CHANGE_ONCE"));
    }

    private static List<String> assess(List<WalChange> changes) {
        Map<Integer, String> snapshot = new LinkedHashMap<>();
        snapshot.put(1, "old");
        snapshot.put(2, "delete-me");
        snapshot.put(3, "keep");
        Map<Integer, String> expected = new LinkedHashMap<>();
        expected.put(1, "updated");
        expected.put(3, null);
        expected.put(4, "inserted");
        expected.put(5, "long-transaction");
        expected.put(6, "中文测试");
        List<String> failures = new ArrayList<>();
        HcsSnapshotHandoffVerification.assessChanges(changes, 10L, snapshot, expected, failures);
        return failures;
    }

    private static List<WalChange> fixture() {
        List<WalChange> result = new ArrayList<>();
        result.add(insert(1, "old", 9, 1));
        result.add(insert(2, "delete-me", 9, 1));
        result.add(insert(3, "keep", 9, 1));
        result.add(insert(4, "inserted", 11, 2));
        WalChange updated =
                WalChange.update(
                        position(3),
                        1,
                        "public",
                        "probe",
                        Arrays.asList(id(1)),
                        Arrays.asList(id(1), payload("updated")));
        updated.setCsn(12);
        result.add(updated);
        WalChange deleted =
                WalChange.delete(position(4), 1, "public", "probe", Arrays.asList(id(2)));
        deleted.setCsn(13);
        result.add(deleted);
        WalChange nulled =
                WalChange.update(
                        position(5),
                        1,
                        "public",
                        "probe",
                        Arrays.asList(id(3)),
                        Arrays.asList(id(3), new WalChange.ColumnValue("payload", 25, null, true)));
        nulled.setCsn(14);
        result.add(nulled);
        result.add(insert(6, "中文测试", 15, 6));
        result.add(insert(5, "long-transaction", 16, 7));
        return result;
    }

    private static WalChange insert(int id, String text, long csn, int position) {
        WalChange change =
                WalChange.insert(
                        position(position),
                        1,
                        "public",
                        "probe",
                        Arrays.asList(id(id), payload(text)));
        change.setCsn(csn);
        return change;
    }

    private static WalChange.ColumnValue id(int id) {
        return new WalChange.ColumnValue("id", 23, String.valueOf(id), false);
    }

    private static WalChange.ColumnValue payload(String value) {
        return new WalChange.ColumnValue("payload", 25, value, false);
    }

    private static String position(int suffix) {
        return "0/" + Integer.toHexString(suffix) + ",0/1,0/" + Integer.toHexString(suffix);
    }
}

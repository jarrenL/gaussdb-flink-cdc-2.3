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

package com.ververica.cdc.connectors.gaussdb.internal.wal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class CnTransactionDecodeTest {
    private WalReplicationStream stream() {
        WalReplicationStream stream =
                new WalReplicationStream(
                        null,
                        "unused",
                        "unused",
                        "unused",
                        "unused",
                        "mppdb_decoding",
                        2,
                        "j",
                        false,
                        10);
        stream.setCnDecode(true);
        return stream;
    }

    @SuppressWarnings("unchecked")
    private List<WalChange> decode(WalReplicationStream stream, String record, String token)
            throws Exception {
        Method method =
                WalReplicationStream.class.getDeclaredMethod(
                        "parseCnRecord", byte[].class, String.class);
        method.setAccessible(true);
        return (List<WalChange>)
                method.invoke(stream, record.getBytes(StandardCharsets.UTF_8), token);
    }

    @Test
    void checkpointPositionDoesNotAdvanceInsideTransaction() throws Exception {
        WalReplicationStream stream = stream();
        assertThat(decode(stream, "BEGIN CSN: 101 first_lsn: 0/1000", "0/64")).isEmpty();
        assertThat(
                        decode(
                                stream,
                                "{\"table_name\":\"public.t\",\"op_type\":\"INSERT\",\"columns_name\":[\"id\"],\"columns_type\":[\"integer\"],\"columns_val\":[\"1\"]}",
                                "0/64"))
                .isEmpty();
        assertThat(stream.getLastLsn()).isNull();
        List<WalChange> committed = decode(stream, "COMMIT XID: 9", "0/65");
        assertThat(committed).hasSize(1);
        assertThat(committed.get(0).getLsn()).isEqualTo("0/65");
        assertThat(committed.get(0).getCsn()).isEqualTo(101);
        assertThat(stream.getLastLsn()).isEqualTo("0/65");
    }

    @Test
    void emptyCommittedTransactionStillAdvancesSafePosition() throws Exception {
        WalReplicationStream stream = stream();
        decode(stream, "BEGIN CSN: 102 first_lsn: 0/1000", "0/65");
        assertThat(decode(stream, "COMMIT XID: 10", "0/66")).isEmpty();
        assertThat(stream.getLastLsn()).isEqualTo("0/66");
    }

    @Test
    void malformedBoundaryFailsInsteadOfAdvancingPosition() throws Exception {
        WalReplicationStream stream = stream();
        assertThatThrownBy(() -> decode(stream, "COMMIT XID: 10", "0/66"))
                .isInstanceOf(InvocationTargetException.class);
        assertThat(stream.getLastLsn()).isNull();
        assertThatThrownBy(() -> decode(stream, "BEGIN 10", "0/66"))
                .isInstanceOf(InvocationTargetException.class);
    }
}

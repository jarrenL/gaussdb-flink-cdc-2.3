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

import com.huawei.gaussdb.jdbc.replication.LogSequenceNumber;

/** Lossless vendor replication positions: an ordinary LSN or an HCS multi-primary triple. */
public final class ReplicationPosition {
    public static final String MULTI_PRIMARY_START = "0/0,0/0,0/0";
    private static final String WORD = "[0-9a-fA-F]{1,8}/[0-9a-fA-F]{1,8}";

    private ReplicationPosition() {}

    public static LogSequenceNumber parse(String token) {
        if (token == null || !token.matches(WORD + "(," + WORD + "," + WORD + ")?")) {
            throw new IllegalArgumentException(
                    "Invalid replication position; expected LSN or CSN,WAL-ID,LSN");
        }
        return LogSequenceNumber.valueOf(token);
    }

    public static LogSequenceNumber requireMultiPrimary(String token) {
        LogSequenceNumber position = parse(token);
        if (!token.contains(",")) {
            throw new IllegalArgumentException(
                    "HCS multi-primary requires a three-part position; ordinary CN/DN checkpoints cannot be reused");
        }
        return position;
    }

    public static int compare(String left, String right) {
        LogSequenceNumber a = parse(left);
        LogSequenceNumber b = parse(right);
        if (left.contains(",") != right.contains(",")) {
            throw new IllegalArgumentException(
                    "Cannot compare ordinary and multi-primary positions");
        }
        return a.compareTo(b);
    }
}

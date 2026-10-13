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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringJoiner;
import java.util.regex.Pattern;

/** Validates CMS seed endpoints without performing DNS lookups. */
public final class HcsCmsEndpoints {
    private static final Pattern ENDPOINT =
            Pattern.compile("(?:\\[[0-9a-fA-F:.]+\\]|[a-zA-Z0-9][a-zA-Z0-9.-]*):[0-9]{1,5}");

    private HcsCmsEndpoints() {}

    /** Returns a normalized list, or null when the option is absent. */
    public static String normalize(String endpoints) {
        if (endpoints == null) {
            return null;
        }
        StringJoiner normalized = new StringJoiner(",");
        String[] entries = endpoints.split(",", -1);
        for (int i = 0; i < entries.length; i++) {
            String entry = entries[i].trim();
            boolean valid = ENDPOINT.matcher(entry).matches();
            if (valid) {
                try {
                    URI address = new URI("tcp://" + entry);
                    valid =
                            address.getHost() != null
                                    && address.getPort() >= 1
                                    && address.getPort() <= 65535;
                } catch (URISyntaxException e) {
                    valid = false;
                }
            }
            if (!valid) {
                // Do not echo input: a mistaken URL may contain credentials.
                throw new IllegalArgumentException(
                        "Invalid hcs.cms-endpoints entry "
                                + (i + 1)
                                + ": expected host:port or [IPv6]:port, port 1..65535");
            }
            normalized.add(entry);
        }
        return normalized.toString();
    }
}

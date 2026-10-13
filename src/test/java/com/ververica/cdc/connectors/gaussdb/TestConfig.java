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

import java.util.HashMap;
import java.util.Map;

final class TestConfig {
    static Map<String, String> options() {
        Map<String, String> p = new HashMap<>();
        p.put("hcs.cms-endpoints", "cms-a:8000,cms-b:8001");
        p.put("database", "pdb");
        p.put("username", "test");
        p.put("password", "test-password");
        p.put("hcs.tenant-name", "tenant");
        p.put("hcs.tenant-password", "test-tenant-password");
        p.put("slot.name", "cdc_test");
        p.put("table-name", "t");
        p.put("poll.interval.ms", "10");
        return p;
    }

    static GaussDBConfig config() {
        return new GaussDBConfig(options());
    }
}

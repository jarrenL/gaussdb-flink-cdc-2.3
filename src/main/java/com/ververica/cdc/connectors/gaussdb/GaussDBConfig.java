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

import com.ververica.cdc.connectors.gaussdb.internal.HcsCmsEndpoints;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/** Connection configuration for the experimental HCS incremental source. */
public final class GaussDBConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Map<String, String> options;

    public GaussDBConfig(Map<String, String> options) {
        this.options = new HashMap<>(options);
        String endpoints = HcsCmsEndpoints.normalize(required("hcs.cms-endpoints"));
        this.options.put("hcs.cms-endpoints", endpoints);
        for (String key :
                new String[] {
                    "database",
                    "username",
                    "password",
                    "hcs.tenant-name",
                    "hcs.tenant-password",
                    "slot.name",
                    "table-name"
                }) {
            required(key);
        }
        if (!"incremental".equals(get("scan.startup.mode", "incremental"))) {
            throw new IllegalArgumentException(
                    "Only scan.startup.mode=incremental is implemented; pre-create the HCS slot");
        }
        positiveInt("connect.timeout.seconds", 15);
        positiveInt("poll.interval.ms", 100);
        positiveInt("batch.size", 1000);
        Pattern.compile(get("schema-name", "public"));
        Pattern.compile(required("table-name"));
    }

    public String required(String key) {
        String value = options.get(key);
        if (value == null || value.trim().isEmpty())
            throw new IllegalArgumentException("Missing option: " + key);
        return value;
    }

    public String get(String key, String fallback) {
        return options.getOrDefault(key, fallback);
    }

    public int positiveInt(String key, int fallback) {
        int value;
        try {
            value = Integer.parseInt(get(key, String.valueOf(fallback)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer option: " + key);
        }
        if (value <= 0) throw new IllegalArgumentException(key + " must be positive");
        return value;
    }

    public String jdbcUrl() {
        String database = required("database");
        try {
            database = java.net.URLEncoder.encode(database, "UTF-8").replace("+", "%20");
        } catch (java.io.UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
        return "jdbc:gaussdb://" + required("hcs.cms-endpoints") + "/" + database;
    }

    public Properties authentication() {
        Properties props = new Properties();
        props.setProperty("user", required("username"));
        props.setProperty("password", required("password"));
        props.setProperty("connectMode", "auth");
        props.setProperty("tenantName", required("hcs.tenant-name"));
        props.setProperty("tenantPassword", required("hcs.tenant-password"));
        props.setProperty("sslmode", get("sslmode", "require"));
        props.setProperty(
                "connectTimeout", String.valueOf(positiveInt("connect.timeout.seconds", 15)));
        return props;
    }

    /** The 507 CloudNative driver requires a separate, non-thread-pool replication session. */
    public Properties replicationAuthentication() {
        Properties props = authentication();
        props.setProperty("allowConnThreadPool", "false");
        com.huawei.gaussdb.jdbc.PGProperty.REPLICATION.set(props, "database");
        com.huawei.gaussdb.jdbc.PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        // The driver's startup packet omits replication below an assumed 9.4 protocol version.
        com.huawei.gaussdb.jdbc.PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        return props;
    }

    public Map<String, String> partition() {
        Map<String, String> partition = new HashMap<>();
        partition.put("tenant", required("hcs.tenant-name"));
        partition.put("database", required("database"));
        partition.put("slot", required("slot.name"));
        return Collections.unmodifiableMap(partition);
    }

    public boolean matches(String schema, String table) {
        return schema != null
                && table != null
                && Pattern.matches(get("schema-name", "public"), schema)
                && Pattern.matches(required("table-name"), table);
    }

    public Properties engineProperties() {
        Properties p = new Properties();
        p.putAll(options);
        p.setProperty(
                "connector.class",
                "com.ververica.cdc.connectors.gaussdb.internal.GaussDBConnector");
        p.setProperty("database.server.name", "gaussdb");
        p.setProperty(
                "database.history", "com.ververica.cdc.debezium.internal.FlinkDatabaseHistory");
        return p;
    }
}

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

import com.huawei.opengauss.jdbc.PGConnection;
import com.huawei.opengauss.jdbc.core.BaseConnection;
import com.huawei.opengauss.jdbc.core.ReplicationProtocol;
import com.huawei.opengauss.jdbc.replication.fluent.logical.LogicalReplicationOptions;
import com.ververica.cdc.connectors.gaussdb.GaussDBConfig;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Inspect an ordinary tenant SQL connection without starting or acknowledging replication. */
public final class HcsConnectionDiagnostics {
    private HcsConnectionDiagnostics() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "Usage: HcsConnectionDiagnostics /path/to/hcs.properties | --driver-only");
        }
        Class<?> driver = Class.forName("com.huawei.opengauss.jdbc.Driver");
        describeCode(driver);
        if ("--driver-only".equals(args[0])) {
            Class<?> protocol =
                    Class.forName(
                            "com.huawei.opengauss.jdbc.core.v3.replication.MultiPrimaryReplicationProtocol");
            System.out.println(
                    "multiPrimary.startLogical.declaredBy="
                            + protocol.getMethod("startLogical", LogicalReplicationOptions.class)
                                    .getDeclaringClass()
                                    .getName());
            System.out.println("DRIVER_ONLY_COMPLETE_NO_DATABASE_CONNECTION");
            return;
        }
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(Paths.get(args[0]))) {
            properties.load(in);
        }
        Map<String, String> options = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            options.put(key, properties.getProperty(key));
        }
        GaussDBConfig config = new GaussDBConfig(options);
        System.out.println("diagnostic.mode=ordinary-tenant-sql; no replication start or ACK");
        System.out.println("connection.replication-property=unset");
        try (Connection connection =
                DriverManager.getConnection(config.jdbcUrl(), config.authentication())) {
            System.out.println("connection.class=" + connection.getClass().getName());
            System.out.println("driver.version=" + connection.getMetaData().getDriverVersion());
            System.out.println(
                    "server.version=" + connection.getMetaData().getDatabaseProductVersion());
            if (connection instanceof BaseConnection) {
                BaseConnection base = (BaseConnection) connection;
                ReplicationProtocol protocol = base.getReplicationProtocol();
                System.out.println("protocol.class=" + protocol.getClass().getName());
                System.out.println(
                        "protocol.startLogical.declaredBy="
                                + protocol.getClass()
                                        .getMethod("startLogical", LogicalReplicationOptions.class)
                                        .getDeclaringClass()
                                        .getName());
                System.out.println("connection.cloudNative=" + base.isCloudNative());
            } else {
                System.out.println("protocol.class=unavailable: connection is not BaseConnection");
            }
            if (connection instanceof PGConnection) {
                Object api = ((PGConnection) connection).getReplicationAPI();
                System.out.println("replicationApi.class=" + api.getClass().getName());
                System.out.println(
                        "logicalBuilder.class="
                                + ((PGConnection) connection)
                                        .getReplicationAPI()
                                        .replicationStream()
                                        .logical()
                                        .getClass()
                                        .getName());
            }
            try (PreparedStatement statement =
                    connection.prepareStatement(
                            "SELECT * FROM pg_get_replication_slots() WHERE slot_name = ?")) {
                statement.setString(1, config.required("slot.name"));
                int count = 0;
                try (ResultSet slots = statement.executeQuery()) {
                    while (slots.next()) {
                        count++;
                        System.out.println(
                                "slot[" + count + "].plugin=" + slots.getString("plugin"));
                        System.out.println(
                                "slot[" + count + "].type=" + slots.getString("slot_type"));
                    }
                }
                System.out.println("slot.matches=" + count);
            }
        }
        System.out.println("DIAGNOSTIC_COMPLETE_NOT_REPLICATION_VERIFIED");
    }

    private static void describeCode(Class<?> driver) throws Exception {
        if (driver.getProtectionDomain().getCodeSource() == null) {
            System.out.println("driver.codeSource=unavailable");
            return;
        }
        URL location = driver.getProtectionDomain().getCodeSource().getLocation();
        if (!"file".equals(location.getProtocol())) {
            System.out.println("driver.codeSource=non-file");
            return;
        }
        Path path = Paths.get(location.toURI());
        // Print only the basename: local directory names may contain identifying information.
        System.out.println("driver.codeSource.file=" + path.getFileName());
        if (!Files.isRegularFile(path)) {
            System.out.println("driver.codeSource.sha256=unavailable: directory");
            return;
        }
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream in = Files.newInputStream(path)) {
            byte[] buffer = new byte[8192];
            int n;
            while ((n = in.read(buffer)) != -1) digest.update(buffer, 0, n);
        }
        StringBuilder hash = new StringBuilder();
        for (byte b : digest.digest()) hash.append(String.format("%02x", b & 0xff));
        System.out.println("driver.codeSource.sha256=" + hash);
    }
}

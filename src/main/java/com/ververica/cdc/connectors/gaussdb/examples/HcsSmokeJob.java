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

import com.ververica.cdc.connectors.gaussdb.GaussDBConfig;
import com.ververica.cdc.connectors.gaussdb.GaussDBSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Submit with flink run -c ...HcsSmokeJob connector-all.jar /path/to/hcs.properties. */
public final class HcsSmokeJob {
    private HcsSmokeJob() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1)
            throw new IllegalArgumentException("Usage: HcsSmokeJob /path/to/hcs.properties");
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(Paths.get(args[0]))) {
            properties.load(input);
        }
        Map<String, String> options = new HashMap<>();
        for (String name : properties.stringPropertyNames())
            options.put(name, properties.getProperty(name));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.addSource(
                        new GaussDBSource<>(
                                new GaussDBConfig(options),
                                new JsonDebeziumDeserializationSchema()))
                .name("GaussDB HCS CDC 2.3")
                .uid("gaussdb-hcs-cdc23")
                .setParallelism(1)
                .print();
        env.execute("GaussDB HCS transparent multi-primary verification");
    }
}

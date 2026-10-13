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

import com.ververica.cdc.connectors.gaussdb.GaussDBConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

/** Kafka Connect task adapter hosted by CDC 2.3's embedded Debezium engine. */
public final class GaussDBConnector extends SourceConnector {
    private Map<String, String> settings;

    @Override
    public void start(Map<String, String> properties) {
        new GaussDBConfig(properties);
        settings = new HashMap<>(properties);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return GaussDBSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.singletonList(new HashMap<>(settings));
    }

    @Override
    public void stop() {}

    @Override
    public String version() {
        return "0.1.0";
    }

    @Override
    public ConfigDef config() {
        ConfigDef def = new ConfigDef();
        for (String key :
                new String[] {
                    "hcs.cms-endpoints",
                    "database",
                    "username",
                    "hcs.tenant-name",
                    "slot.name",
                    "table-name"
                }) def.define(key, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, key);
        for (String key : new String[] {"password", "hcs.tenant-password"})
            def.define(key, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, key);
        return def;
    }
}

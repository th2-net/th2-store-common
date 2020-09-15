/******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.exactpro.th2.store.common;

import com.exactpro.th2.configuration.MicroserviceConfiguration;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

import static com.exactpro.th2.ConfigurationUtils.safeLoad;
import static com.exactpro.th2.configuration.Th2Configuration.QueueNames;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

public class Configuration extends MicroserviceConfiguration {

    public static final String ENV_CRADLE_INSTANCE_NAME = "CRADLE_INSTANCE_NAME";
    public static final String ENV_TH2_MESSAGE_STORE_QUEUE_NAMES = "TH2_MESSAGE_STORE_QUEUE_NAMES";
    public static final String DEFAULT_CRADLE_INSTANCE_NAME = "instance1";

    public static String getEnvCradleInstanceName() {
        return defaultIfNull(getenv(ENV_CRADLE_INSTANCE_NAME), DEFAULT_CRADLE_INSTANCE_NAME);
    }

    public static Map<String, QueueNames> getEnvQueueNames() {
        String queueNamesData = getenv(ENV_TH2_MESSAGE_STORE_QUEUE_NAMES);
        if (StringUtils.isBlank(queueNamesData)) {
            return Collections.emptyMap();
        }
        try {
            return JSON_READER.readValue(queueNamesData, new TypeReference<Map<String, QueueNames>>() {});
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    format("could not parse '%s' env variable data", ENV_TH2_MESSAGE_STORE_QUEUE_NAMES), e);
        }
    }

    private String cradleInstanceName = getEnvCradleInstanceName();
    private Map<String, QueueNames> sourceNameToQueueNames = getEnvQueueNames();

    public String getCradleInstanceName() {
        return cradleInstanceName;
    }

    public void setCradleInstanceName(String cradleInstanceName) {
        this.cradleInstanceName = cradleInstanceName;
    }

    public Map<String, QueueNames> getSourceNameToQueueNames() {
        return sourceNameToQueueNames;
    }

    public void setSourceNameToQueueNames(Map<String, QueueNames> sourceNameToQueueNames) {
        this.sourceNameToQueueNames = sourceNameToQueueNames;
    }

    private final CassandraConfig cassandraConfig = new CassandraConfig();

    public CassandraConfig getCassandraConfig() {
        return cassandraConfig;
    }

    public static Configuration load(InputStream inputStream) throws IOException {
        return YAML_READER.readValue(inputStream, Configuration.class);
    }

    public static Configuration readConfiguration(String[] args) {
        if (args.length > 0) {
            return safeLoad(Configuration::load, Configuration::new, args[0]);
        }
        return new Configuration();
    }
}

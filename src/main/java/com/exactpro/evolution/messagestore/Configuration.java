/******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.evolution.messagestore;

import com.exactpro.evolution.common.CassandraConfig;
import com.exactpro.evolution.configuration.RabbitMQConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static java.lang.System.getenv;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

public class Configuration extends com.exactpro.evolution.configuration.Configuration {

    public static final String ENV_CRADLE_INSTANCE_NAME = "CRADLE_INSTANCE_NAME";
    public static final String DEFAULT_CRADLE_INSTANCE_NAME = "instance1";

    public static String getEnvCradleInstanceName() {
        return defaultIfNull(getenv(ENV_CRADLE_INSTANCE_NAME), DEFAULT_CRADLE_INSTANCE_NAME);
    }

    private String cradleInstanceName = getEnvCradleInstanceName();

    public String getCradleInstanceName() {
        return cradleInstanceName;
    }

    public void setCradleInstanceName(String cradleInstanceName) {
        this.cradleInstanceName = cradleInstanceName;
    }

    private CassandraConfig cassandraConfig = new CassandraConfig();

    public CassandraConfig getCassandraConfig() {
        return cassandraConfig;
    }

    private RabbitMQConfiguration rabbitMQ = new RabbitMQConfiguration();

    public RabbitMQConfiguration getRabbitMQ() {
        return rabbitMQ;
    }

    public static Configuration load(InputStream inputStream) throws IOException {
        return YAML_READER.readValue(inputStream, Configuration.class);
    }

    public static class Address {
        private String host;
        private int port;

        public Address() {
        }

        public Address(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }
}

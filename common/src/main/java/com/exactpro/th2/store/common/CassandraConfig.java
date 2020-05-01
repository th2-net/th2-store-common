/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.th2.store.common;

import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.evolution.configuration.Configuration;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.IOException;
import java.io.InputStream;

import static java.lang.System.getenv;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.apache.commons.lang3.math.NumberUtils.toInt;

public class CassandraConfig {

    public static final String ENV_CASSANDRA_DATA_CENTER = "CASSANDRA_DATA_CENTER";
    public static final String DEFAULT_CASSANDRA_DATA_CENTER = "kos";

    public static String getEnvCassandraDataCenter() {
        return defaultIfNull(getenv(ENV_CASSANDRA_DATA_CENTER), DEFAULT_CASSANDRA_DATA_CENTER);
    }

    public static final String ENV_CASSANDRA_HOST = "CASSANDRA_HOST";
    public static final String DEFAULT_CASSANDRA_HOST = "cassandra";

    public static String getEnvCassandraHost() {
        return defaultIfNull(getenv(ENV_CASSANDRA_HOST), DEFAULT_CASSANDRA_HOST);
    }

    public static final String ENV_CASSANDRA_PORT = "CASSANDRA_PORT";
    public static final int DEFAULT_CASSANDRA_PORT = 9042;

    public static int getEnvCassandraPort() {
        return toInt(getenv(ENV_CASSANDRA_PORT), DEFAULT_CASSANDRA_PORT);
    }

    public static final String ENV_CASSANDRA_KEYSPACE = "CASSANDRA_KEYSPACE";
    public static final String DEFAULT_CASSANDRA_KEYSPACE = "demo";

    public static String getEnvCassandraKeyspace() {
        return defaultIfNull(getenv(ENV_CASSANDRA_KEYSPACE), DEFAULT_CASSANDRA_KEYSPACE);
    }

    public static final String ENV_CASSANDRA_USERNAME = "CASSANDRA_USERNAME";
    public static final String DEFAULT_CASSANDRA_USERNAME = "guest";

    public static String getEnvCassandraUsername() {
        return defaultIfNull(getenv(ENV_CASSANDRA_USERNAME), DEFAULT_CASSANDRA_USERNAME);
    }

    public static final String ENV_CASSANDRA_PASSWORD = "CASSANDRA_PASSWORD";
    public static final String DEFAULT_CASSANDRA_PASSWORD = "guest";

    public static String getEnvCassandraPassword() {
        return defaultIfNull(getenv(ENV_CASSANDRA_PASSWORD), DEFAULT_CASSANDRA_PASSWORD);
    }

    @JsonIgnore
    private final CassandraConnectionSettings cassandraConnectionSettings = new CassandraConnectionSettings();

    public CassandraConfig() {
        cassandraConnectionSettings.setLocalDataCenter(getEnvCassandraDataCenter());
        cassandraConnectionSettings.setHost(getEnvCassandraHost());
        cassandraConnectionSettings.setPort(getEnvCassandraPort());
        cassandraConnectionSettings.setKeyspace(getEnvCassandraKeyspace());
        cassandraConnectionSettings.setUsername(getEnvCassandraUsername());
        cassandraConnectionSettings.setPassword(getEnvCassandraPassword());
    }

    public static CassandraConfig load(InputStream inputStream) throws IOException {
        return Configuration.YAML_READER.readValue(inputStream, CassandraConfig.class);
    }

    public String getDataCenter() {
        return cassandraConnectionSettings.getLocalDataCenter();
    }

    public void setDataCenter(String cassandraDataCenter) {
        this.cassandraConnectionSettings.setLocalDataCenter(cassandraDataCenter);
    }

    public String getHost() {
        return cassandraConnectionSettings.getHost();
    }

    public void setHost(String cassandraHost) {
        this.cassandraConnectionSettings.setHost(cassandraHost);
    }

    public int getPort() {
        return cassandraConnectionSettings.getPort();
    }

    public void setPort(int cassandraPort) {
        this.cassandraConnectionSettings.setPort(cassandraPort);
    }

    public String getKeyspace() {
        return cassandraConnectionSettings.getKeyspace();
    }

    public void setKeyspace(String cassandraKeyspace) {
        this.cassandraConnectionSettings.setKeyspace(cassandraKeyspace);
    }

    public String getUsername() {
        return cassandraConnectionSettings.getUsername();
    }

    public void setUsername(String cassandraUsername) {
        this.cassandraConnectionSettings.setUsername(cassandraUsername);
    }

    public String getPassword() {
        return cassandraConnectionSettings.getPassword();
    }

    public void setPassword(String cassandraPassword) {
        this.cassandraConnectionSettings.setPassword(cassandraPassword);
    }

    public CassandraConnectionSettings getConnectionSettings() {
        return cassandraConnectionSettings;
    }
}

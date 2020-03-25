/******************************************************************************
 * Copyright (c) 2009-2020, Exactpro Systems LLC
 * www.exactpro.com
 * Build Software to Test Software
 *
 * All rights reserved.
 * This is unpublished, licensed software, confidential and proprietary
 * information which is the property of Exactpro Systems LLC or its licensors.
 ******************************************************************************/

package com.exactpro.evolution.eventstore;

import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

public class Config
{
  private CassandraConnectionSettings connectionSettings;
	private String instanceName;

  public CassandraConnectionSettings getConnectionSettings() {
    return connectionSettings;
  }

  public void setConnectionSettings(CassandraConnectionSettings connectionSettings) {
    this.connectionSettings = connectionSettings;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public void setInstanceName(String instanceName) {
    this.instanceName = instanceName;
  }

  public static Config loadFrom(File config) throws IOException {
    Properties props = new Properties();
    props.load(new FileInputStream(config));
    Config result = new Config();
    CassandraConnectionSettings settings = new CassandraConnectionSettings(
      props.getProperty("datacenter", ""),
      props.getProperty("host", ""),
      Integer.parseInt(props.getProperty("port", "-1")),
      props.getProperty("keyspace")
    );
    settings.setUsername(props.getProperty("username", ""));
    settings.setPassword(props.getProperty("password", ""));
    result.setInstanceName(props.getProperty("instance", "Cradle feeder "+ InetAddress.getLocalHost().getHostName()));
    result.setConnectionSettings(settings);
    return result;
  }
}

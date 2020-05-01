package com.exactpro.th2.messagestore;

import com.exactpro.th2.store.common.CassandraConfig;

import java.util.Set;

public class StoreConfig {
  private CassandraConfig cassandraConfig;
  private Set<String> connectivityEndpoints;

  public StoreConfig() {
  }

  public StoreConfig(CassandraConfig cassandraConfig, Set<String> connectivityEndpoints) {
    this.cassandraConfig = cassandraConfig;
    this.connectivityEndpoints = connectivityEndpoints;
  }

  public CassandraConfig getCassandraConfig() {
    return cassandraConfig;
  }

  public void setCassandraConfig(CassandraConfig cassandraConfig) {
    this.cassandraConfig = cassandraConfig;
  }

  public Set<String> getConnectivityEndpoints() {
    return connectivityEndpoints;
  }

  public void setConnectivityEndpoints(Set<String> connectivityEndpoints) {
    this.connectivityEndpoints = connectivityEndpoints;
  }
}

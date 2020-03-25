package com.exactpro.evolution.eventstore;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.evolution.eventstore.utils.AsyncHelper;
import io.reactivex.Completable;
import io.vertx.grpc.VertxServerBuilder;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.impl.AsyncResultCompletable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class MainVerticle extends AbstractVerticle {
  private static final String DEFAULT_CONFIG_FILE_NAME = "EventStore.cfg";
  private CradleManager cradleManager;
  private Config config;

  public MainVerticle() throws IOException {
    config = Config.loadFrom(new File(DEFAULT_CONFIG_FILE_NAME));
    cradleManager = new CassandraCradleManager(new CassandraConnection(config.getConnectionSettings()));
  }

  public MainVerticle(CradleManager cradleManager) throws IOException {
    this(Config.loadFrom(new File(DEFAULT_CONFIG_FILE_NAME)), cradleManager);
  }

  public MainVerticle(Config config) {
    this(config, new CassandraCradleManager(new CassandraConnection(config.getConnectionSettings())));
  }

  public MainVerticle(Config config, CradleManager cradleManager) {
    this.cradleManager = cradleManager;
    this.config = config;
  }

  @Override
  public Completable rxStart() {
    return Completable.merge(Arrays.asList(
      initManager(),
      startService()
    ));
  }

  private Completable startService() {
    return AsyncResultCompletable.toCompletable(h ->
      VertxServerBuilder.forAddress(vertx.getDelegate(), "0.0.0.0", 8080)
        .addService(new ReportEventStoreService(cradleManager, vertx))
        .build().start(h)
    );
  }

  private Completable initManager() {
    return vertx.<Void>rxExecuteBlocking(promise -> AsyncHelper
      .wrapIntoPromise(() -> cradleManager.init(config.getInstanceName()), promise)).ignoreElement();
  }
}

package com.exactpro.evolution.eventstore;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.evolution.common.Configuration;
import com.exactpro.evolution.common.utils.AsyncHelper;
import io.reactivex.Completable;
import io.vertx.grpc.VertxServerBuilder;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.impl.AsyncResultCompletable;

import java.io.IOException;
import java.util.Arrays;

public class EventStoreVerticle extends AbstractVerticle {
    private CradleManager cradleManager;
    private final Configuration config;

    public EventStoreVerticle(Configuration config) throws IOException {
        this.config = config;
        cradleManager = new CassandraCradleManager(new CassandraConnection(this.config.getCassandraConfig().getConnectionSettings()));
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
            VertxServerBuilder.forAddress(vertx.getDelegate(),
                "0.0.0.0",
                config.getPort())
                .addService(new ReportEventStoreService(cradleManager, vertx))
                .build().start(h)
        );
    }

    private Completable initManager() {
        return vertx.<Void>rxExecuteBlocking(AsyncHelper
            .createHandler(() -> cradleManager.init(config.getCradleInstanceName()))).ignoreElement();
    }
}

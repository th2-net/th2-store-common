package com.exactpro.evolution.eventstore;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.evolution.common.CassandraConfig;
import com.exactpro.evolution.common.utils.AsyncHelper;
import com.exactpro.evolution.messagestore.Configuration;
import io.reactivex.Completable;
import io.vertx.grpc.VertxServerBuilder;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.impl.AsyncResultCompletable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static java.lang.System.getenv;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

public class EventStoreVerticle extends AbstractVerticle {
    private CradleManager cradleManager;
    private final Configuration config = new Configuration();

    public EventStoreVerticle() throws IOException {
        cradleManager = new CassandraCradleManager(new CassandraConnection(config.getCassandraConfig().getConnectionSettings()));
    }

    public EventStoreVerticle(CradleManager cradleManager) {
        this.cradleManager = cradleManager;
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
                Integer.parseInt(defaultIfNull(getenv("port"), "9095")))
                .addService(new ReportEventStoreService(cradleManager, vertx))
                .build().start(h)
        );
    }

    private Completable initManager() {
        return vertx.<Void>rxExecuteBlocking(AsyncHelper
            .createHandler(() -> cradleManager.init(config.getCradleInstanceName()))).ignoreElement();
    }
}

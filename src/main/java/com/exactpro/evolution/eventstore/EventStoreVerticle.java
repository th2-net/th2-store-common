package com.exactpro.evolution.eventstore;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.evolution.common.CassandraConfig;
import com.exactpro.evolution.common.utils.AsyncHelper;
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
    private static final String DEFAULT_CONFIG_FILE_NAME = "EventStore.cfg";
    private CradleManager cradleManager;
    private CassandraConfig config;

    public EventStoreVerticle() throws IOException {
        config = CassandraConfig.loadFrom(new File(DEFAULT_CONFIG_FILE_NAME));
        cradleManager = new CassandraCradleManager(new CassandraConnection(config.getConnectionSettings()));
    }

    public EventStoreVerticle(CradleManager cradleManager) throws IOException {
        this(CassandraConfig.loadFrom(new File(DEFAULT_CONFIG_FILE_NAME)), cradleManager);
    }

    public EventStoreVerticle(CassandraConfig config) {
        this(config, new CassandraCradleManager(new CassandraConnection(config.getConnectionSettings())));
    }

    public EventStoreVerticle(CassandraConfig config, CradleManager cradleManager) {
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
            VertxServerBuilder.forAddress(vertx.getDelegate(),
                "0.0.0.0",
                Integer.parseInt(defaultIfNull(getenv("port"), "9095")))
                .addService(new ReportEventStoreService(cradleManager, vertx))
                .build().start(h)
        );
    }

    private Completable initManager() {
        return vertx.<Void>rxExecuteBlocking(AsyncHelper
            .createHandler(() -> cradleManager.init(config.getInstanceName()))).ignoreElement();
    }
}

//package com.exactpro.evolution.messagestore;
//
//import com.exactpro.cradle.CradleManager;
//import com.exactpro.cradle.cassandra.CassandraCradleManager;
//import com.exactpro.cradle.cassandra.connection.CassandraConnection;
//import com.exactpro.evolution.common.utils.AsyncHelper;
//import io.reactivex.Completable;
//import io.reactivex.Flowable;
//import io.reactivex.Single;
//import io.vertx.core.json.JsonObject;
//import io.vertx.reactivex.core.AbstractVerticle;
//import io.vertx.reactivex.core.buffer.Buffer;
//
//import static com.exactpro.evolution.common.Configuration.getEnvCradleInstanceName;
//
//public class MessageStoreVerticle extends AbstractVerticle {
//  private static final String CONFIG_FILE_PATH = "MessageStoreConfig.json";
//
//  private final StoreConfig predefinedConfig;
//  private final CradleManager predefinedCradleManager;
//  private final IConnectivityEndpointsResolver connectivityEndpointsResolver;
//
//  public MessageStoreVerticle(StoreConfig predefinedConfig,
//                              CradleManager cradleManager,
//                              IConnectivityEndpointsResolver connectivityEndpointsResolver) {
//    this.predefinedConfig = predefinedConfig;
//    predefinedCradleManager = cradleManager;
//    this.connectivityEndpointsResolver = connectivityEndpointsResolver != null? connectivityEndpointsResolver :
//      new ConnectivityEndpointsResolver(this.vertx);
//  }
//
//  public MessageStoreVerticle() {
//    this(null, null, null);
//  }
//
//  private Single<StoreConfig> readConfig() {
//    return vertx.fileSystem().rxReadFile(CONFIG_FILE_PATH)
//      .map(Buffer::getDelegate)
//      .map(JsonObject::new)
//      .map(jo -> jo.mapTo(StoreConfig.class))
//      .cache();
//  }
//
//  private Flowable<QueueInfo> resolveEndpoints(StoreConfig config) {
//    return connectivityEndpointsResolver.resolveEndpoints(config);
//  }
//
//  private Completable setupRabbitListener(StoreConfig config, MessageStore store) {
//    Flowable<ConnectivityListener> listeners = connectivityEndpointsResolver.resolveEndpoints(config).
//      map(ConnectivityListener::new).cache();
//    return listeners.flatMapCompletable(
//      l -> vertx.<Void>rxExecuteBlocking(AsyncHelper.createHandler(l::start)).ignoreElement()
//    ).doOnComplete(() -> {
//      listeners.flatMap(ConnectivityListener::getFlowable).toObservable().doOnNext(m -> {
//        store.apply(m).subscribe();
//      }).subscribe();
//    });
//  }
//
//  @Override
//  public Completable rxStart() {
//    Single<StoreConfig> config = predefinedConfig == null? readConfig(): Single.just(predefinedConfig);
//    return config.zipWith(config.flatMap(this::initManager), this::setupRabbitListener).ignoreElement();
//  }
//
//  private Single<MessageStore> initManager(StoreConfig config) {
//    return Single.just(predefinedCradleManager == null? new CassandraCradleManager(
//      new CassandraConnection(config.getCassandraConfig().getConnectionSettings())):
//      predefinedCradleManager
//    ).flatMap(manager ->
//        vertx.<Void>rxExecuteBlocking(
//          AsyncHelper.createHandler(() ->
//            manager.init(getEnvCradleInstanceName())
//          )
//        ).ignoreElement()
//      .toSingleDefault(manager)
//    ).map(manager -> new MessageStore(manager, vertx));
//  }
//}

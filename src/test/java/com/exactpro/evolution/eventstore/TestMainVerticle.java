package com.exactpro.evolution.eventstore;

import com.exactpro.cradle.CradleManager;
import com.exactpro.evolution.common.CassandraConfig;
import io.grpc.ManagedChannel;
import io.vertx.grpc.VertxChannelBuilder;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.impl.AsyncResultSingle;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.Matchers.any;

@Disabled("Storage based on vertx is broken")
@ExtendWith(VertxExtension.class)
public class TestMainVerticle {
  private static final String CRADLE_MANAGER_MOCK_KEY = "CradleManager";
  private static final String SERVICE_STUB_KEY = "ServiceStub";
  private ConcurrentMap<Object, Object> testData;

  private CradleManagerMock getCradleManager() {
    return (CradleManagerMock)testData.get(CRADLE_MANAGER_MOCK_KEY);
  }

  private EventStoreServiceGrpc.EventStoreServiceVertxStub getServiceStub() {
    return (EventStoreServiceGrpc.EventStoreServiceVertxStub)testData.get(SERVICE_STUB_KEY);
  }

  @BeforeEach
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) {
    CradleManagerMock manager = Mockito.spy(CradleManagerMock.class);
    Mockito.when(manager.createStorage()).thenReturn(Mockito.spy(CradleStorageMock.class));
    testData = new ConcurrentHashMap<>();
    testData.put(CRADLE_MANAGER_MOCK_KEY, manager);
    ManagedChannel channel = VertxChannelBuilder
      .forAddress(vertx.getDelegate(), "localhost", 8080)
      .usePlaintext(true)
      .build();
    EventStoreServiceGrpc.EventStoreServiceVertxStub stub = EventStoreServiceGrpc.newVertxStub(channel);
    testData.put(SERVICE_STUB_KEY, stub);
    vertx.deployVerticle(new EventStoreVerticle(manager), testContext.succeeding(id -> testContext.completeNow()));
  }

  @Test
  void storeReport(Vertx vertx, VertxTestContext testContext) {
    EventStoreServiceGrpc.EventStoreServiceVertxStub stub = getServiceStub();
    Checkpoint storageCalled = testContext.checkpoint();
    CradleManager manager = getCradleManager();
    AsyncResultSingle.<Response>toSingle(h -> stub.storeReport(StoreReportRequest.newBuilder().build(), h))
      .ignoreElement()
      .doOnComplete(() -> Mockito.verify(manager.getStorage()).storeReport(any()))
      .doOnComplete(storageCalled::flag).subscribe();
  }

  @Test
  void storeEvent(Vertx vertx, VertxTestContext testContext) {
    EventStoreServiceGrpc.EventStoreServiceVertxStub stub = getServiceStub();
    Checkpoint storageCalled = testContext.checkpoint();
    CradleManager manager = getCradleManager();
    AsyncResultSingle.<Response>toSingle(h -> stub.storeEvent(StoreEventRequest.newBuilder().build(), h))
      .ignoreElement()
      .doOnComplete(() -> Mockito.verify(manager.getStorage()).storeTestEvent(any()))
      .doOnComplete(storageCalled::flag).subscribe();
  }
}

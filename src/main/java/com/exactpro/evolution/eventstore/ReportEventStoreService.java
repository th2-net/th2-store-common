package com.exactpro.evolution.eventstore;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.StoredMessageId;
import com.exactpro.cradle.StoredReport;
import com.exactpro.cradle.StoredTestEvent;
import com.exactpro.evolution.common.utils.AsyncHelper;
import com.exactpro.evolution.common.utils.TimeHelper;
import com.exactpro.evolution.eventstore.EventStoreServiceGrpc.EventStoreServiceVertxImplBase;
import com.google.protobuf.StringValue;
import io.reactivex.Single;
import io.vertx.core.Promise;
import io.vertx.reactivex.core.Vertx;

import java.util.stream.Collectors;


public class ReportEventStoreService extends EventStoreServiceVertxImplBase {
  private static final String STORAGE_LOCK_NAME = "CassandraStorageLock";
  private CradleManager cradleManager;
  private Vertx vertx;

  public ReportEventStoreService(CradleManager cradleManager, Vertx vertx) {
    this.cradleManager = cradleManager;
    this.vertx = vertx;
  }

  @Override
  public void storeReport(StoreReportRequest request, Promise<Response> response) {
    AsyncHelper.executeWithLock(vertx, STORAGE_LOCK_NAME, storeReport(request)
      .doOnSuccess(id -> response.complete(Response.newBuilder().setId(StringValue.of(id)).build()))
      .doOnError(e -> response.complete(Response.newBuilder().setError(StringValue.of(e.toString())).build()))
      .ignoreElement()
    ).subscribe();
  }

  private Single<String> storeReport(StoreReportRequest request) {
    return Single.just(request)
      .map(r -> {
        StoredReport report = new StoredReport();
        report.setId(r.getReportId());
        report.setName(r.getReportName());
        report.setSuccess(r.getSuccess());
        report.setTimestamp(TimeHelper.toInstant(r.getReportStartTimestamp()));
        report.setContent(new byte[0]);
        return report;
      })
      .flatMap(report -> vertx.rxExecuteBlocking(
          AsyncHelper.createHandler(() -> cradleManager.getStorage().storeReport(report))
        ).toSingle()
      );
  }

  @Override
  public void storeEvent(StoreEventRequest request, Promise<Response> response) {
    AsyncHelper.executeWithLock(vertx, STORAGE_LOCK_NAME, storeEvent(request)
      .doOnSuccess(id -> response.complete(Response.newBuilder().setId(StringValue.of(id)).build()))
      .doOnError(e -> response.complete(Response.newBuilder().setError(StringValue.of(e.toString())).build()))
      .ignoreElement()
    ).subscribe();
  }

  private Single<String> storeEvent(StoreEventRequest request) {
    return Single.just(request)
      .map(r -> {
        StoredTestEvent result = new StoredTestEvent();
        result.setId(r.getEventId());
        result.setName(r.getEventName());
        result.setType(r.getEventType());
        result.setParentId(r.hasParentEventId()? r.getParentEventId().getValue(): null);
        result.setReportId(r.getReportId());
        result.setStartTimestamp(r.hasEventStartTimestamp()? TimeHelper.toInstant(r.getEventStartTimestamp()): null);
        result.setEndTimestamp(r.hasEventEndTimestamp()? TimeHelper.toInstant(r.getEventEndTimestamp()): null);
        result.setSuccess(r.getSuccess());
        result.setContent(r.getBody().toByteArray());
        return result;
      }).flatMap(event -> vertx.rxExecuteBlocking(
          AsyncHelper.createHandler(() -> {
              String eventId = cradleManager.getStorage().storeTestEvent(event);
              if (request.getAttachedMessageIdsCount() != 0) {
                  cradleManager.getStorage().storeTestEventMessagesLink(eventId,
                      request.getAttachedMessageIdsList().stream()
                          .map(StoredMessageId::new)
                          .collect(Collectors.toSet())
                  );
              }
              return eventId;
          })
        ).toSingle()
      );
  }
}

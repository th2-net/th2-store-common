package com.exactpro.th2.eventstore;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc.EventStoreServiceVertxImplBase;
import com.exactpro.th2.eventstore.grpc.Response;
import com.exactpro.th2.eventstore.grpc.StoreEventRequest;
import com.exactpro.th2.store.common.utils.AsyncHelper;
import com.exactpro.th2.store.common.utils.ProtoUtil;
import io.reactivex.Single;
import io.vertx.core.Promise;
import io.vertx.reactivex.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

import static com.exactpro.th2.store.common.utils.ProtoUtil.toBatch;
import static com.exactpro.th2.store.common.utils.ProtoUtil.toStoredTestEventId;
import static com.exactpro.th2.store.common.utils.TimeHelper.toInstant;
import static com.google.protobuf.StringValue.of;


public class ReportEventStoreService extends EventStoreServiceVertxImplBase {
    private final Logger logger = LoggerFactory.getLogger(getClass().getName() + '@' + hashCode());
    private static final String STORAGE_LOCK_NAME = "CassandraStorageLock";
    private final CradleManager cradleManager;
    private final Vertx vertx;

    public ReportEventStoreService(CradleManager cradleManager, Vertx vertx) {
        this.cradleManager = cradleManager;
        this.vertx = vertx;
    }

    @Override
    public void storeEvent(StoreEventRequest request, Promise<Response> response) {
        AsyncHelper.executeWithLock(vertx, STORAGE_LOCK_NAME, storeEvent(request)
                .doOnSuccess(id -> response.complete(Response.newBuilder().setId(of(id)).build()))
                .doOnError(e -> {
                    response.complete(Response.newBuilder().setError(of(e.toString())).build());
                    logger.error("store event error: {}", e.getMessage(), e);
                })
                .ignoreElement()
        ).subscribe();
    }

    private Single<String> storeEvent(StoreEventRequest request) {
        return Single.just(request)
                .map(r -> {
                    StoredTestEvent result = new StoredTestEvent();
                    result.setId(toStoredTestEventId(r.getEventId()));
                    result.setName(r.getEventName());
                    result.setType(r.getEventType());
                    result.setParentId(r.hasParentEventId() ? toStoredTestEventId(r.getParentEventId()) : null);
                    result.setStartTimestamp(r.hasEventStartTimestamp() ? toInstant(r.getEventStartTimestamp()) : null);
                    result.setEndTimestamp(r.hasEventEndTimestamp() ? toInstant(r.getEventEndTimestamp()) : null);
                    result.setSuccess(r.getSuccess());
                    result.setContent(r.getBody().toByteArray());
                    return toBatch(result);
                }).flatMap(eventBatch -> vertx.rxExecuteBlocking(
                        AsyncHelper.createHandler(() -> {
                            cradleManager.getStorage().storeTestEventBatch(eventBatch);
                            logger.debug("event id: {}, event: {}", eventBatch.getId().getId(), eventBatch);
                            if (request.getAttachedMessageIdsCount() != 0) {
                                cradleManager.getStorage().storeTestEventMessagesLink(
                                        eventBatch.getTestEventsList().get(0).getId(),
                                        request.getAttachedMessageIdsList().stream()
                                                .map(ProtoUtil::toStoredMessageId)// FIXME: This method doesn't work because TH2 API migrated to sequence
                                                .collect(Collectors.toSet())
                                );
                            }
                            return eventBatch.getTestEventsList().get(0).getId().toString();
                        })
                        ).toSingle()
                );
    }
}

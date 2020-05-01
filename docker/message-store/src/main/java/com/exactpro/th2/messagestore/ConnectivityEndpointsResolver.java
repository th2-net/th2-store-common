package com.exactpro.th2.messagestore;

import com.exactpro.evolution.api.phase_1.ConnectivityGrpc;
import com.exactpro.evolution.api.phase_1.QueueRequest;
import com.exactpro.th2.store.common.utils.AsyncHelper;
import io.grpc.ManagedChannelBuilder;
import io.reactivex.Flowable;
import io.vertx.reactivex.core.Vertx;

public class ConnectivityEndpointsResolver implements IConnectivityEndpointsResolver {
    private final Vertx vertx;

    public ConnectivityEndpointsResolver(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public Flowable<QueueInfo> resolveEndpoints(StoreConfig config) {
        return Flowable.fromIterable(config.getConnectivityEndpoints())
                .flatMapMaybe(endpoint ->
                        vertx.rxExecuteBlocking(
                                AsyncHelper.createHandler(
                                        () -> ConnectivityGrpc.newBlockingStub(
                                                ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build())
                                                .getQueueInfo(QueueRequest.newBuilder().build()))
                        )
                )
                .map(response -> {
                    QueueInfo result = new QueueInfo();
                    result.setExchangeName(response.getExchangeName());
                    result.setInMsgQueue(response.getInMsgQueue());
                    result.setInRawMsgQueue(response.getInRawMsgQueue());
                    result.setOutMsgQueue(response.getOutMsgQueue());
                    result.setOutRawMsgQueue(response.getOutRawMsgQueue());
                    return result;
                });
    }
}

package com.exactpro.th2.messagestore;

import io.reactivex.Flowable;

public interface IConnectivityEndpointsResolver {
    Flowable<QueueInfo> resolveEndpoints(StoreConfig config);
}

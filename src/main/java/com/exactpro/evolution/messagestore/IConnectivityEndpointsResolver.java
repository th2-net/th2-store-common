package com.exactpro.evolution.messagestore;

import io.reactivex.Flowable;

public interface IConnectivityEndpointsResolver {
    Flowable<QueueInfo> resolveEndpoints(StoreConfig config);
}

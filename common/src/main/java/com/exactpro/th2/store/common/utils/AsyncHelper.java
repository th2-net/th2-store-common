package com.exactpro.th2.store.common.utils;

import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.core.Handler;
import io.vertx.reactivex.core.Promise;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.shareddata.Lock;

public class AsyncHelper {
  public interface ExceptionfullSupplier<T> {
    T execute() throws Exception;
  }

  public interface ExceptionfullRunnable {
    void execute() throws Exception;
  }

  public static <T> Promise<T> wrapIntoPromise(ExceptionfullSupplier<T> code) {
    return wrapIntoPromise(code, null);
  }

  public static <T> Promise<T> wrapIntoPromise(ExceptionfullSupplier<T> code, Promise<T> target) {
    Promise<T> result = target == null? Promise.promise(): target;
    try {
      result.complete(code.execute());
    } catch (Exception e) {
      result.fail(e);
    }
    return result;
  }

  public static <T> Handler<Promise<T>> createHandler(ExceptionfullRunnable code) {
    return (Promise<T> promise) -> {
      try {
        code.execute();
        promise.complete();
      } catch (Exception e) {
        promise.fail(e);
      }
    };
  }

  public static <T> Handler<Promise<T>> createHandler(ExceptionfullSupplier<T> code) {
    return (Promise<T> promise) -> {
      try {
        promise.complete(code.execute());
      } catch (Exception e) {
        promise.fail(e);
      }
    };
  }

  public static Completable executeWithLock(Vertx vertx, String lockName, Completable operation) {
    return vertx.sharedData().rxGetLock(lockName)
      .flatMapCompletable(l -> operation.doFinally(l::release));
  }

  public static Promise<Void> wrapIntoPromise(ExceptionfullRunnable code) {
    return wrapIntoPromise(code, null);
  }

  public static Promise<Void> wrapIntoPromise(ExceptionfullRunnable code, Promise<Void> target) {
    Promise<Void> result = target == null? Promise.promise(): target;
    try {
      code.execute();
      result.complete();
    } catch (Exception e) {
      result.fail(e);
    }
    return result;
  }
}

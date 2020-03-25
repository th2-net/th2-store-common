package com.exactpro.evolution.eventstore.utils;

import io.vertx.core.Handler;
import io.vertx.reactivex.core.Promise;

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

  public static <T> Handler<Promise<T>> createHandler(ExceptionfullSupplier<T> code) {
    return (Promise<T> promise) -> {
      try {
        promise.complete(code.execute());
      } catch (Exception e) {
        promise.fail(e);
      }
    };
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

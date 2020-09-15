/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

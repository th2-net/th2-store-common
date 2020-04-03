/******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
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
 ******************************************************************************/
package com.exactpro.evolution;

import com.exactpro.evolution.eventstore.EventStoreVerticle;
import com.exactpro.evolution.messagestore.MessageStoreVerticle;
import io.reactivex.Completable;
import io.vertx.core.Vertx;
import io.vertx.reactivex.core.AbstractVerticle;

public class MainVerticle extends AbstractVerticle {
  enum Role {
    MessageStore,
    EventStore
  }

  @Override
  public Completable rxStart() {
    Role instanceRole = Role.valueOf(System.getenv().get("INSTANCE_ROLE"));
    switch (instanceRole) {
      case EventStore:
        return vertx.rxDeployVerticle(EventStoreVerticle.class.getCanonicalName()).ignoreElement();
      case MessageStore:
        return vertx.rxDeployVerticle(MessageStoreVerticle.class.getCanonicalName()).ignoreElement();
      default:
        return Completable.error(new RuntimeException("Invalid role"));
    }
  }
  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(MainVerticle.class.getName());
  }
}

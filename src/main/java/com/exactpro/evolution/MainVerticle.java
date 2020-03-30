package com.exactpro.evolution;

import com.exactpro.evolution.eventstore.EventStoreVerticle;
import com.exactpro.evolution.messagestore.MessageStoreVerticle;
import io.reactivex.Completable;
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
}

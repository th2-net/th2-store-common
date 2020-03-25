package com.exactpro.evolution.eventstore.utils;

import com.google.protobuf.Timestamp;

import java.time.Instant;

public class TimeHelper {
  public static Instant toInstant(Timestamp timestamp) {
    if (timestamp.isInitialized()) {
      return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
    else {
      return null;
    }
  }
}

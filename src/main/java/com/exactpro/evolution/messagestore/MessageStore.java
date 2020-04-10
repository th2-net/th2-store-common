package com.exactpro.evolution.messagestore;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.StoredMessage;
import com.exactpro.cradle.StoredMessageId;
import com.exactpro.evolution.api.phase_1.Message;
import com.exactpro.evolution.api.phase_1.SessionId;
import com.exactpro.evolution.common.TcpCradleStream;
import com.exactpro.evolution.common.utils.AsyncHelper;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.reactivex.core.Vertx;

import java.nio.ByteBuffer;
import java.time.Instant;

public class MessageStore {

  private static final String SAVED_SESSIONS_MAP_KEY = "savedSessionsMapKey";
  private static final String STORAGE_LOCK_NAME = "CassandraStorageLock";

  private CradleManager cradleManager;
  private Vertx vertx;

  public MessageStore(CradleManager cradleManager, Vertx vertx) {
    this.cradleManager = cradleManager;
    this.vertx = vertx;
  }

  public Completable apply(ConnectivityMessage message) {
    switch (message.getQueue()) {
      case IN_MSG:
      case OUT_MSG:
        return handleMessage(message);
      default:
        return Completable.error(new UnsupportedOperationException());
    }
  }

  private Completable handleMessage(ConnectivityMessage message) {
    return parseMessage(message).flatMapCompletable(msg ->
      AsyncHelper.executeWithLock(vertx, STORAGE_LOCK_NAME,
        vertx.rxExecuteBlocking(AsyncHelper.createHandler(() -> cradleManager.getStorage().storeMessage(msg))
        ).ignoreElement()
      )
    );
  }

  public Single<StoredMessage> parseMessage(ConnectivityMessage message) {
    Single<Message> parsedMessage = Single.just(message)
      .map(ConnectivityMessage::getBody)
      .map(ByteBuffer::wrap)
      .map(Message::parseFrom)
      .cache();

    Single<TcpCradleStream> storedStream = parsedMessage.flatMap(this::getStoredStream);

    return mapStoredMessage(parsedMessage, storedStream, mapDirection(message));
  }

  private Single<Direction> mapDirection(ConnectivityMessage message) {
    return Single.just(message)
      .map(msg ->
               msg.getQueue() == ConnectivityListener.Source.IN_MSG ||
               msg.getQueue() == ConnectivityListener.Source.IN_RAW ? Direction.RECEIVED:
          Direction.SENT);
  }

  private Single<TcpCradleStream> getStoredStream(Message parsedMessage) {
    Single<TcpCradleStream> stream = Single.just(parsedMessage)
      .map(msg -> msg.getMetadata().getConnectivityId().getSessionId())
      .map(this::mapSession).cache();

    return vertx.sharedData().rxGetLocalAsyncMap(SAVED_SESSIONS_MAP_KEY)
      .zipWith(stream, (map, str) -> map.rxPutIfAbsent(str, true))
      .flatMapMaybe(c -> c)
      .flatMapSingle(val ->
        val != null?
          Single.just((TcpCradleStream) val):
          storeSession(stream)
      ).cache();
  }

  private TcpCradleStream mapSession(SessionId session) {
    return new TcpCradleStream(
      session.getSessionAlias(),
      session.getSourceAddress().getHost() + ":" + session.getSourceAddress().getPort(),
      session.getTargetAddress().getHost() + ":" + session.getTargetAddress().getPort()
    );
  }

  private Single<StoredMessage> mapStoredMessage(Single<Message> message,
                                         Single<TcpCradleStream> stream,
                                         Single<Direction> direction) {
    return message.map(msg -> {
      StoredMessage result = new StoredMessage();
      result.setId(new StoredMessageId(msg.getMetadata().getMessageId()));
      result.setTimestamp(Instant.now());
      return result;
    })
      .zipWith(stream, (msg, str) -> {
        msg.setStreamName(str.getName());
        return msg;
      })
      .zipWith(direction, (msg, dir) -> {
        msg.setDirection(dir);
        return msg;
      });
  }

  private Single<TcpCradleStream> storeSession(Single<TcpCradleStream> stream) {
    return stream
      .flatMapCompletable(str ->
        AsyncHelper.executeWithLock(vertx, STORAGE_LOCK_NAME, vertx.rxExecuteBlocking(
          AsyncHelper.createHandler(() -> cradleManager.getStorage().storeStream(str))
        ).ignoreElement())
      ).<TcpCradleStream>toFlowable().concatWith(stream).firstOrError();
  }
}

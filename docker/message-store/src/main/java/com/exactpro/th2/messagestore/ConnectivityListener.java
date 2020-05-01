package com.exactpro.th2.messagestore;

import com.exactpro.evolution.RabbitMqSubscriber;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.subjects.PublishSubject;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ConnectivityListener {

  public enum Source {
    IN_MSG,
    IN_RAW,
    OUT_MSG,
    OUT_RAW
  }

  private PublishSubject<ConnectivityMessage> messages;
  private RabbitMqSubscriber inMsgSubscriber;
  private RabbitMqSubscriber inRawSubscriber;
  private RabbitMqSubscriber outMsgSubscriber;
  private RabbitMqSubscriber outRawSubscriber;

  public ConnectivityListener(QueueInfo queueInfo) {
    messages = PublishSubject.create();
    inMsgSubscriber = new RabbitMqSubscriber(queueInfo.getExchangeName(),
      handler(Source.IN_MSG),
      null,
      queueInfo.getInMsgQueue());
    inRawSubscriber = new RabbitMqSubscriber(queueInfo.getExchangeName(),
      handler(Source.IN_RAW),
      null,
      queueInfo.getInRawMsgQueue());
    outMsgSubscriber = new RabbitMqSubscriber(queueInfo.getExchangeName(),
      handler(Source.OUT_MSG),
      null,
      queueInfo.getOutMsgQueue());
    outRawSubscriber = new RabbitMqSubscriber(queueInfo.getExchangeName(),
      handler(Source.OUT_RAW),
      null,
      queueInfo.getOutRawMsgQueue());
  }

  private DeliverCallback handler(Source sourceQueue) {
    return (String consumerTag, Delivery delivery) ->
      messages.onNext(new ConnectivityMessage(consumerTag, delivery.getBody(), sourceQueue));
  }

  public Flowable<ConnectivityMessage> getFlowable() {
    return messages.toFlowable(BackpressureStrategy.BUFFER);
  }

  public void start() throws IOException, TimeoutException {
    inMsgSubscriber.startListening();
    inRawSubscriber.startListening();
    outMsgSubscriber.startListening();
    outRawSubscriber.startListening();
  }

  public void stop() throws IOException {
    inMsgSubscriber.close();
    inRawSubscriber.close();
    outMsgSubscriber.close();
    outRawSubscriber.close();
    messages.onComplete();
  }
}

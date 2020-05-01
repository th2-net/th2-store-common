package com.exactpro.th2.messagestore;

public class ConnectivityMessage {
  private final String consumerTag;
  private final byte[] body;
  private final ConnectivityListener.Source queue;

  public ConnectivityMessage(String consumerTag, byte[] body, ConnectivityListener.Source queue) {
    this.consumerTag = consumerTag;
    this.body = body;
    this.queue = queue;
  }

  public String getConsumerTag() {
    return consumerTag;
  }

  public byte[] getBody() {
    return body;
  }

  public ConnectivityListener.Source getQueue() {
    return queue;
  }
}

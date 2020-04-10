package com.exactpro.evolution.messagestore;

public class QueueInfo {
  private String exchangeName;
  private String inMsgQueue;
  private String inRawMsgQueue;
  private String outMsgQueue;
  private String outRawMsgQueue;

  public QueueInfo() {
  }

  public QueueInfo(String exchangeName, String inMsgQueue, String inRawMsgQueue, String outMsgQueue, String outRawMsgQueue) {
    this.exchangeName = exchangeName;
    this.inMsgQueue = inMsgQueue;
    this.inRawMsgQueue = inRawMsgQueue;
    this.outMsgQueue = outMsgQueue;
    this.outRawMsgQueue = outRawMsgQueue;
  }

  public String getExchangeName() {
    return exchangeName;
  }

  public void setExchangeName(String exchangeName) {
    this.exchangeName = exchangeName;
  }

  public String getInMsgQueue() {
    return inMsgQueue;
  }

  public void setInMsgQueue(String inMsgQueue) {
    this.inMsgQueue = inMsgQueue;
  }

  public String getInRawMsgQueue() {
    return inRawMsgQueue;
  }

  public void setInRawMsgQueue(String inRawMsgQueue) {
    this.inRawMsgQueue = inRawMsgQueue;
  }

  public String getOutMsgQueue() {
    return outMsgQueue;
  }

  public void setOutMsgQueue(String outMsgQueue) {
    this.outMsgQueue = outMsgQueue;
  }

  public String getOutRawMsgQueue() {
    return outRawMsgQueue;
  }

  public void setOutRawMsgQueue(String outRawMsgQueue) {
    this.outRawMsgQueue = outRawMsgQueue;
  }
}

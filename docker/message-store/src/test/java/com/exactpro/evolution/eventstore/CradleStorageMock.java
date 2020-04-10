package com.exactpro.evolution.eventstore;

import com.exactpro.cradle.*;
import com.exactpro.cradle.utils.CradleStorageException;

import java.io.IOException;
import java.util.Set;

public class CradleStorageMock extends CradleStorage {
  @Override
  protected String doInit(String instanceName) throws CradleStorageException {
    return null;
  }

  @Override
  public void dispose() throws CradleStorageException {

  }

  @Override
  public StoredMessageId storeMessage(StoredMessage message) throws IOException {
    return null;
  }

  @Override
  public String storeReport(StoredReport report) throws IOException {
    return "";
  }

  @Override
  public void modifyReport(StoredReport report) throws IOException {

  }

  @Override
  public String storeTestEvent(StoredTestEvent testEvent) throws IOException {
    return "";
  }

  @Override
  public void modifyTestEvent(StoredTestEvent testEvent) throws IOException {

  }

  @Override
  public void storeReportMessagesLink(String reportId, Set<StoredMessageId> messagesIds) throws IOException {

  }

  @Override
  public void storeTestEventMessagesLink(String eventId, Set<StoredMessageId> messagesIds) throws IOException {

  }

  @Override
  public StoredMessage getMessage(StoredMessageId id) throws IOException {
    return null;
  }

  @Override
  public StoredReport getReport(String id) throws IOException {
    return null;
  }

  @Override
  public StoredTestEvent getTestEvent(String id) throws IOException {
    return null;
  }

  @Override
  public StreamsMessagesLinker getStreamsMessagesLinker() {
    return null;
  }

  @Override
  public ReportsMessagesLinker getReportsMessagesLinker() {
    return null;
  }

  @Override
  public TestEventsMessagesLinker getTestEventsMessagesLinker() {
    return null;
  }

  @Override
  public Iterable<StoredMessage> getMessages() throws IOException {
    return null;
  }

  @Override
  public Iterable<StoredReport> getReports() throws IOException {
    return null;
  }

  @Override
  public Iterable<StoredTestEvent> getReportTestEvents(String reportId) throws IOException {
    return null;
  }

  @Override
  protected String queryStreamId(String streamName) throws IOException {
    return null;
  }

  @Override
  protected String doStoreStream(CradleStream stream) throws IOException {
    return null;
  }

  @Override
  protected void doModifyStream(String id, CradleStream newStream) throws IOException {

  }

  @Override
  protected void doModifyStreamName(String id, String newName) throws IOException {

  }
}

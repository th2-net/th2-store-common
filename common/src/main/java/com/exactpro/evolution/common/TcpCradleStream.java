package com.exactpro.evolution.common;

import com.exactpro.cradle.CradleStream;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TcpCradleStream implements CradleStream {
  @JsonIgnore
  private String streamName;
  @JsonIgnore
  private String sourceAddress;
  @JsonIgnore
  private String targetAddress;

  public TcpCradleStream(String streamName, String sourceAddress, String targetAddress) {
    this.streamName = streamName;
    this.sourceAddress = sourceAddress;
    this.targetAddress = targetAddress;
  }

  @JsonIgnore
  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  @JsonIgnore
  public void setSourceAddress(String sourceAddress) {
    this.sourceAddress = sourceAddress;
  }

  @JsonIgnore
  public void setTargetAddress(String targetAddress) {
    this.targetAddress = targetAddress;
  }

  @Override
  @JsonGetter
  public String getName() {
    return streamName;
  }

  @Override
  @JsonGetter
  public String getStreamData() {
    return sourceAddress + "->" + targetAddress;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TcpCradleStream that = (TcpCradleStream) o;
    return Objects.equals(streamName, that.streamName) &&
      Objects.equals(sourceAddress, that.sourceAddress) &&
      Objects.equals(targetAddress, that.targetAddress);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamName, sourceAddress, targetAddress);
  }
}

package com.vibe.events.registry;

public class EventDefinition {
  private String key;
  private String successTable;
  private String failureTable;

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getSuccessTable() {
    return successTable;
  }

  public void setSuccessTable(String successTable) {
    this.successTable = successTable;
  }

  public String getFailureTable() {
    return failureTable;
  }

  public void setFailureTable(String failureTable) {
    this.failureTable = failureTable;
  }
}

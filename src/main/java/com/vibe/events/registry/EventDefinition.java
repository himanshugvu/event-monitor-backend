package com.vibe.events.registry;

public class EventDefinition {
  private String key;
  private String name;
  private String category;
  private String successTable;
  private String failureTable;
  private String replayUrl;

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getCategory() {
    return category;
  }

  public void setCategory(String category) {
    this.category = category;
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

  public String getReplayUrl() {
    return replayUrl;
  }

  public void setReplayUrl(String replayUrl) {
    this.replayUrl = replayUrl;
  }
}

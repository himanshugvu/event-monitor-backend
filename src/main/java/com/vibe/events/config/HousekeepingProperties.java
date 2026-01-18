package com.vibe.events.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "housekeeping")
public class HousekeepingProperties {
  private boolean enabled = true;
  private String cron = "0 15 2 * * *";
  private String replayAuditCron = "0 25 2 * * *";
  private String housekeepingAuditCron = "0 35 2 * * *";
  private int retentionDays = 7;
  private int batchSize = 10000;
  private int schedulerPoolSize = 4;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getCron() {
    return cron;
  }

  public void setCron(String cron) {
    this.cron = cron;
  }

  public String getReplayAuditCron() {
    return replayAuditCron;
  }

  public void setReplayAuditCron(String replayAuditCron) {
    this.replayAuditCron = replayAuditCron;
  }

  public String getHousekeepingAuditCron() {
    return housekeepingAuditCron;
  }

  public void setHousekeepingAuditCron(String housekeepingAuditCron) {
    this.housekeepingAuditCron = housekeepingAuditCron;
  }

  public int getRetentionDays() {
    return retentionDays;
  }

  public void setRetentionDays(int retentionDays) {
    this.retentionDays = retentionDays;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public int getSchedulerPoolSize() {
    return schedulerPoolSize;
  }

  public void setSchedulerPoolSize(int schedulerPoolSize) {
    this.schedulerPoolSize = schedulerPoolSize;
  }
}

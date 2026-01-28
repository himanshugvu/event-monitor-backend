package com.vibe.events.service;

import com.vibe.events.config.HousekeepingProperties;
import com.vibe.events.registry.EventDefinition;
import com.vibe.events.registry.EventRegistry;
import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

@Component
public class HousekeepingScheduler {
  private static final Logger log = LoggerFactory.getLogger(HousekeepingScheduler.class);
  private static final String JOB_TYPE_REPLAY_AUDIT = "REPLAY_AUDIT";
  private static final String JOB_TYPE_HOUSEKEEPING_AUDIT = "HOUSEKEEPING_AUDIT";

  private final TaskScheduler scheduler;
  private final HousekeepingService housekeepingService;
  private final EventRegistry registry;
  private final HousekeepingProperties properties;

  public HousekeepingScheduler(
      TaskScheduler scheduler,
      HousekeepingService housekeepingService,
      EventRegistry registry,
      HousekeepingProperties properties) {
    this.scheduler = scheduler;
    this.housekeepingService = housekeepingService;
    this.registry = registry;
    this.properties = properties;
  }

  @PostConstruct
  public void scheduleJobs() {
    if (!properties.isEnabled()) {
      return;
    }
    scheduleEventJobs();
    scheduleAuditJob(JOB_TYPE_REPLAY_AUDIT, properties.getReplayAuditCron());
    scheduleAuditJob(JOB_TYPE_HOUSEKEEPING_AUDIT, properties.getHousekeepingAuditCron());
    schedulePreviewCache(properties.getPreviewCron());
    warmPreviewCacheOnStartup();
  }

  private void scheduleEventJobs() {
    for (EventDefinition definition : registry.all()) {
      String cron = pickCron(definition.getRetentionCron(), properties.getCron());
      if (cron == null) {
        log.warn("No retention cron configured for event {}", definition.getKey());
        continue;
      }
      scheduler.schedule(
          () -> runRetention(definition.getKey()),
          new CronTrigger(cron, java.util.TimeZone.getDefault()));
      log.info("Scheduled retention job for {} with cron {}", definition.getKey(), cron);
    }
  }

  private void scheduleAuditJob(String jobType, String cron) {
    if (cron == null || cron.isBlank()) {
      log.warn("No cron configured for {}", jobType);
      return;
    }
    scheduler.schedule(
        () -> runAudit(jobType),
        new CronTrigger(cron, java.util.TimeZone.getDefault()));
    log.info("Scheduled {} job with cron {}", jobType, cron);
  }

  private void runRetention(String eventKey) {
    try {
      housekeepingService.runRetention(eventKey, "SCHEDULED", null);
    } catch (Exception ex) {
      log.warn("Scheduled retention failed for {}", eventKey, ex);
    }
  }

  private void runAudit(String jobType) {
    try {
      housekeepingService.runAudit(jobType, "SCHEDULED", null);
    } catch (Exception ex) {
      log.warn("Scheduled {} job failed", jobType, ex);
    }
  }

  private void schedulePreviewCache(String cron) {
    if (cron == null || cron.isBlank()) {
      log.warn("No cron configured for preview cache");
      return;
    }
    scheduler.schedule(
        housekeepingService::refreshPreviewCache,
        new CronTrigger(cron, java.util.TimeZone.getDefault()));
    log.info("Scheduled preview cache refresh with cron {}", cron);
  }

  private void warmPreviewCacheOnStartup() {
    scheduler.schedule(
        housekeepingService::refreshPreviewCache,
        Instant.now().plus(Duration.ofSeconds(2)));
    log.info("Scheduled preview cache warmup on startup");
  }

  private String pickCron(String eventCron, String defaultCron) {
    if (eventCron != null && !eventCron.isBlank()) {
      return eventCron.trim();
    }
    if (defaultCron != null && !defaultCron.isBlank()) {
      return defaultCron.trim();
    }
    return null;
  }
}

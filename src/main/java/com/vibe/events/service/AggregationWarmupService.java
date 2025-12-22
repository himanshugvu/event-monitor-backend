package com.vibe.events.service;

import com.vibe.events.config.AggregationProperties;
import com.vibe.events.registry.EventDefinition;
import com.vibe.events.registry.EventRegistry;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(prefix = "aggregation.warmup", name = "enabled", havingValue = "true", matchIfMissing = true)
public class AggregationWarmupService {
  private static final Logger log = LoggerFactory.getLogger(AggregationWarmupService.class);

  private final AggregationService aggregationService;
  private final EventRegistry registry;
  private final AggregationProperties properties;

  public AggregationWarmupService(
      AggregationService aggregationService,
      EventRegistry registry,
      AggregationProperties properties) {
    this.aggregationService = aggregationService;
    this.registry = registry;
    this.properties = properties;
  }

  @jakarta.annotation.PostConstruct
  public void warmupStartup() {
    for (LocalDate day : daysToWarm()) {
      refreshDay(day);
    }
  }

  @Scheduled(fixedDelay = 60000)
  public void refreshToday() {
    LocalDate today = LocalDate.now();
    refreshDay(today);
  }

  @Scheduled(cron = "0 0 * * * *")
  public void refreshYesterdayHourly() {
    LocalDate yesterday = LocalDate.now().minusDays(1);
    refreshDay(yesterday);
  }

  private void refreshDay(LocalDate day) {
    long start = System.currentTimeMillis();
    aggregationService.refreshHome(day);
    for (EventDefinition definition : registry.all()) {
      aggregationService.refreshEventSummary(day, definition.getKey());
      for (Integer interval : properties.getBuckets().getIntervalsMinutes()) {
        aggregationService.refreshEventBuckets(day, definition.getKey(), interval);
      }
    }
    log.info("Warmup refresh done for day {} in {}ms", day, System.currentTimeMillis() - start);
  }

  private List<LocalDate> daysToWarm() {
    int days = properties.getCache().getDaysToKeep();
    LocalDate today = LocalDate.now();
    List<LocalDate> result = new ArrayList<>(days);
    for (int i = 0; i < days; i++) {
      result.add(today.minusDays(i));
    }
    return result;
  }
}

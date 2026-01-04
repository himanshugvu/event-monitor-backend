package com.vibe.events.service;

import com.vibe.events.config.AggregationProperties;
import com.vibe.events.config.CacheConfig;
import com.vibe.events.dto.BucketPoint;
import com.vibe.events.dto.EventBreakdownRow;
import com.vibe.events.dto.EventBucketsResponse;
import com.vibe.events.dto.EventSummaryResponse;
import com.vibe.events.dto.HomeAggregationResponse;
import com.vibe.events.dto.Kpis;
import com.vibe.events.dto.LatencyStages;
import com.vibe.events.error.BadRequestException;
import com.vibe.events.registry.EventDefinition;
import com.vibe.events.registry.EventRegistry;
import com.vibe.events.repo.AggregationRepository;
import com.vibe.events.repo.BucketKey;
import com.vibe.events.repo.FailureBucket;
import com.vibe.events.repo.FailureTotals;
import com.vibe.events.repo.SuccessBucket;
import com.vibe.events.repo.SuccessTotals;
import com.vibe.events.util.CacheKeys;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class AggregationService {
  private static final Logger log = LoggerFactory.getLogger(AggregationService.class);

  private final AggregationRepository repository;
  private final EventRegistry registry;
  private final CacheManager cacheManager;
  private final AggregationProperties properties;

  public AggregationService(
      AggregationRepository repository,
      EventRegistry registry,
      CacheManager cacheManager,
      AggregationProperties properties) {
    this.repository = repository;
    this.registry = registry;
    this.cacheManager = cacheManager;
    this.properties = properties;
  }

  public HomeAggregationResponse getHomeAggregation(LocalDate day) {
    String key = CacheKeys.homeKey(day);
    Cache cache = cacheManager.getCache(CacheConfig.HOME_AGG);
    if (cache != null) {
      HomeAggregationResponse cached = cache.get(key, HomeAggregationResponse.class);
      if (cached != null) {
        return cached;
      }
    }
    HomeAggregationResponse response = computeHomeAggregation(day);
    if (cache != null) {
      cache.put(key, response);
    }
    return response;
  }

  public EventSummaryResponse getEventSummary(LocalDate day, String eventKey) {
    String key = CacheKeys.eventSummaryKey(day, eventKey);
    Cache cache = cacheManager.getCache(CacheConfig.EVENT_SUMMARY);
    if (cache != null) {
      EventSummaryResponse cached = cache.get(key, EventSummaryResponse.class);
      if (cached != null) {
        return cached;
      }
    }
    EventSummaryResponse response = computeEventSummary(day, eventKey);
    if (cache != null) {
      cache.put(key, response);
    }
    return response;
  }

  public EventBucketsResponse getEventBuckets(LocalDate day, String eventKey, int intervalMinutes) {
    validateInterval(intervalMinutes);
    String key = CacheKeys.eventBucketsKey(day, eventKey, intervalMinutes);
    Cache cache = cacheManager.getCache(CacheConfig.EVENT_BUCKETS);
    if (cache != null) {
      EventBucketsResponse cached = cache.get(key, EventBucketsResponse.class);
      if (cached != null) {
        return cached;
      }
    }
    EventBucketsResponse response = computeEventBuckets(day, eventKey, intervalMinutes);
    if (cache != null) {
      cache.put(key, response);
    }
    return response;
  }

  public void refreshHome(LocalDate day) {
    Cache cache = cacheManager.getCache(CacheConfig.HOME_AGG);
    if (cache != null) {
      cache.put(CacheKeys.homeKey(day), computeHomeAggregation(day));
    }
  }

  public void refreshEventSummary(LocalDate day, String eventKey) {
    Cache cache = cacheManager.getCache(CacheConfig.EVENT_SUMMARY);
    if (cache != null) {
      cache.put(CacheKeys.eventSummaryKey(day, eventKey), computeEventSummary(day, eventKey));
    }
  }

  public void refreshEventBuckets(LocalDate day, String eventKey, int intervalMinutes) {
    validateInterval(intervalMinutes);
    Cache cache = cacheManager.getCache(CacheConfig.EVENT_BUCKETS);
    if (cache != null) {
      cache.put(
          CacheKeys.eventBucketsKey(day, eventKey, intervalMinutes),
          computeEventBuckets(day, eventKey, intervalMinutes));
    }
  }

  @Async("refreshExecutor")
  public CompletableFuture<Void> refreshEventAcrossDays(String eventKey, int days) {
    EventDefinition definition = registry.getRequired(eventKey);
    if (days <= 0) {
      return CompletableFuture.completedFuture(null);
    }
    LocalDate today = LocalDate.now();
    for (int i = 0; i < days; i += 1) {
      LocalDate day = today.minusDays(i);
      refreshEventSummary(day, definition.getKey());
      for (Integer interval : properties.getBuckets().getIntervalsMinutes()) {
        refreshEventBuckets(day, definition.getKey(), interval);
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  private HomeAggregationResponse computeHomeAggregation(LocalDate day) {
    long start = System.currentTimeMillis();
    List<EventBreakdownRow> eventRows = new ArrayList<>();
    long totalSuccess = 0;
    long totalFailure = 0;
    long totalRetriable = 0;
    double weightedLatencySum = 0.0;
    double weightedReceivedSum = 0.0;
    double weightedSentSum = 0.0;
    List<String> latencyTables = new ArrayList<>();
    List<String> successTables = new ArrayList<>();

    for (EventDefinition definition : registry.all()) {
      EventStats stats = computeEventStats(day, definition);
      String name = definition.getName();
      if (name == null || name.isBlank()) {
        name = definition.getKey();
      }
      String category = definition.getCategory();
      if (category == null || category.isBlank()) {
        category = "Uncategorized";
      }
      eventRows.add(
          new EventBreakdownRow(
              definition.getKey(),
              name,
              category,
              stats.total(),
              stats.success(),
              stats.failure(),
              round2(stats.successRate()),
              stats.retriableFailures(),
              round2(stats.avgLatencyMs())));
      totalSuccess += stats.success();
      totalFailure += stats.failure();
      totalRetriable += stats.retriableFailures();
      weightedLatencySum += stats.avgLatencyMs() * stats.total();
      double avgReceivedSuccess =
          toLatencyValue(
              repository.loadAverageLatency(
                  definition.getSuccessTable(), "latency_event_received_ms", day));
      double avgReceivedFailure =
          toLatencyValue(
              repository.loadAverageLatency(
                  definition.getFailureTable(), "latency_event_received_ms", day));
      weightedReceivedSum += avgReceivedSuccess * stats.success();
      weightedReceivedSum += avgReceivedFailure * stats.failure();
      if (stats.success() > 0) {
        double avgSentSuccess =
            toLatencyValue(
                repository.loadAverageLatency(
                    definition.getSuccessTable(), "latency_event_sent_ms", day));
        weightedSentSum += avgSentSuccess * stats.success();
      }
      latencyTables.add(definition.getSuccessTable());
      latencyTables.add(definition.getFailureTable());
      successTables.add(definition.getSuccessTable());
    }

    long total = totalSuccess + totalFailure;
    double avgLatency = total > 0 ? weightedLatencySum / total : 0.0;
    double avgReceived = total > 0 ? weightedReceivedSum / total : 0.0;
    double avgSent = totalSuccess > 0 ? weightedSentSum / totalSuccess : 0.0;
    Double p95Latency =
        repository.loadLatencyPercentileAcrossTables(
            latencyTables, "latency_ms", day, total, 0.95);
    Double p99Latency =
        repository.loadLatencyPercentileAcrossTables(
            latencyTables, "latency_ms", day, total, 0.99);
    Double maxLatency =
        repository.loadMaxLatencyAcrossTables(latencyTables, "latency_ms", day);
    Double p95Received =
        repository.loadLatencyPercentileAcrossTables(
            latencyTables, "latency_event_received_ms", day, total, 0.95);
    Double p99Received =
        repository.loadLatencyPercentileAcrossTables(
            latencyTables, "latency_event_received_ms", day, total, 0.99);
    Double maxReceived =
        repository.loadMaxLatencyAcrossTables(latencyTables, "latency_event_received_ms", day);
    Double p95Sent =
        repository.loadLatencyPercentileAcrossTables(
            successTables, "latency_event_sent_ms", day, totalSuccess, 0.95);
    Double p99Sent =
        repository.loadLatencyPercentileAcrossTables(
            successTables, "latency_event_sent_ms", day, totalSuccess, 0.99);
    Double maxSent =
        repository.loadMaxLatencyAcrossTables(successTables, "latency_event_sent_ms", day);
    Kpis kpis =
        new Kpis(
            total,
            totalSuccess,
            totalFailure,
            round2(successRate(totalSuccess, total)),
            totalRetriable,
            round2(avgLatency),
            round2(p95Latency == null ? 0.0 : p95Latency),
            round2(p99Latency == null ? 0.0 : p99Latency),
            round2(maxLatency == null ? 0.0 : maxLatency));
    LatencyStages stageLatencies =
        new LatencyStages(
            round2(avgReceived),
            round2(p95Received == null ? 0.0 : p95Received),
            round2(p99Received == null ? 0.0 : p99Received),
            round2(maxReceived == null ? 0.0 : maxReceived),
            round2(avgSent),
            round2(p95Sent == null ? 0.0 : p95Sent),
            round2(p99Sent == null ? 0.0 : p99Sent),
            round2(maxSent == null ? 0.0 : maxSent));

    HomeAggregationResponse response =
        new HomeAggregationResponse(day, LocalDateTime.now(), kpis, stageLatencies, eventRows);
    log.info("Computed home aggregation for day {} in {}ms", day, System.currentTimeMillis() - start);
    return response;
  }

  private EventSummaryResponse computeEventSummary(LocalDate day, String eventKey) {
    long start = System.currentTimeMillis();
    EventDefinition definition = registry.getRequired(eventKey);
    EventStats stats = computeEventStats(day, definition);
    Kpis kpis =
        new Kpis(
            stats.total(),
            stats.success(),
            stats.failure(),
            round2(stats.successRate()),
            stats.retriableFailures(),
            round2(stats.avgLatencyMs()),
            round2(stats.p95LatencyMs()),
            round2(stats.p99LatencyMs()),
            round2(stats.maxLatencyMs()));
    LatencyStages stageLatencies = buildStageLatencies(definition, stats, day);
    EventSummaryResponse response =
        new EventSummaryResponse(day, eventKey, LocalDateTime.now(), kpis, stageLatencies);
    log.info(
        "Computed event summary for {} day {} in {}ms",
        eventKey,
        day,
        System.currentTimeMillis() - start);
    return response;
  }

  private EventBucketsResponse computeEventBuckets(LocalDate day, String eventKey, int intervalMinutes) {
    long start = System.currentTimeMillis();
    EventDefinition definition = registry.getRequired(eventKey);
    List<BucketPoint> buckets;
    if (intervalMinutes == 60) {
      Map<Integer, SuccessBucket> successBuckets =
          repository.loadHourlySuccessBuckets(definition.getSuccessTable(), day);
      Map<Integer, FailureBucket> failureBuckets =
          repository.loadHourlyFailureBuckets(definition.getFailureTable(), day);
      buckets = buildHourlyBuckets(day, successBuckets, failureBuckets);
    } else {
      Map<BucketKey, SuccessBucket> successBuckets =
          repository.loadQuarterHourSuccessBuckets(definition.getSuccessTable(), day);
      Map<BucketKey, FailureBucket> failureBuckets =
          repository.loadQuarterHourFailureBuckets(definition.getFailureTable(), day);
      buckets = buildQuarterHourBuckets(day, successBuckets, failureBuckets);
    }

    EventBucketsResponse response =
        new EventBucketsResponse(day, eventKey, intervalMinutes, LocalDateTime.now(), buckets);
    log.info(
        "Computed buckets for {} day {} interval {} in {}ms",
        eventKey,
        day,
        intervalMinutes,
        System.currentTimeMillis() - start);
    return response;
  }

  private EventStats computeEventStats(LocalDate day, EventDefinition definition) {
    SuccessTotals successTotals =
        repository.loadSuccessTotals(definition.getSuccessTable(), day);
    FailureTotals failureTotals =
        repository.loadFailureTotals(definition.getFailureTable(), day);
    long success = successTotals.successCount();
    long failure = failureTotals.failureCount();
    long total = success + failure;
    double successLatency = successTotals.avgLatencyMs() == null ? 0.0 : successTotals.avgLatencyMs();
    double failureLatency = failureTotals.avgLatencyMs() == null ? 0.0 : failureTotals.avgLatencyMs();
    double avgLatency =
        total > 0 ? (successLatency * success + failureLatency * failure) / total : 0.0;
    Double p95Latency =
        repository.loadLatencyPercentileAcrossTables(
            List.of(definition.getSuccessTable(), definition.getFailureTable()),
            "latency_ms",
            day,
            total,
            0.95);
    Double p99Latency =
        repository.loadLatencyPercentileAcrossTables(
            List.of(definition.getSuccessTable(), definition.getFailureTable()),
            "latency_ms",
            day,
            total,
            0.99);
    Double maxLatency =
        repository.loadMaxLatencyAcrossTables(
            List.of(definition.getSuccessTable(), definition.getFailureTable()),
            "latency_ms",
            day);
    return new EventStats(
        total,
        success,
        failure,
        successRate(success, total),
        failureTotals.retriableCount(),
        avgLatency,
        p95Latency == null ? 0.0 : p95Latency,
        p99Latency == null ? 0.0 : p99Latency,
        maxLatency == null ? 0.0 : maxLatency);
  }

  private LatencyStages buildStageLatencies(
      EventDefinition definition, EventStats stats, LocalDate day) {
    double successReceived =
        toLatencyValue(
            repository.loadAverageLatency(
                definition.getSuccessTable(), "latency_event_received_ms", day));
    double failureReceived =
        toLatencyValue(
            repository.loadAverageLatency(
                definition.getFailureTable(), "latency_event_received_ms", day));
    double avgReceived =
        stats.total() > 0
            ? (successReceived * stats.success() + failureReceived * stats.failure())
                / stats.total()
            : 0.0;
    double avgSent =
        stats.success() > 0
            ? toLatencyValue(
                repository.loadAverageLatency(
                    definition.getSuccessTable(), "latency_event_sent_ms", day))
            : 0.0;
    double p95Received =
        toLatencyValue(
            repository.loadLatencyPercentileAcrossTables(
                List.of(definition.getSuccessTable(), definition.getFailureTable()),
                "latency_event_received_ms",
                day,
                stats.total(),
                0.95));
    double p99Received =
        toLatencyValue(
            repository.loadLatencyPercentileAcrossTables(
                List.of(definition.getSuccessTable(), definition.getFailureTable()),
                "latency_event_received_ms",
                day,
                stats.total(),
                0.99));
    double maxReceived =
        toLatencyValue(
            repository.loadMaxLatencyAcrossTables(
                List.of(definition.getSuccessTable(), definition.getFailureTable()),
                "latency_event_received_ms",
                day));
    double p95Sent =
        stats.success() > 0
            ? toLatencyValue(
                repository.loadLatencyPercentileAcrossTables(
                    List.of(definition.getSuccessTable()),
                    "latency_event_sent_ms",
                    day,
                    stats.success(),
                    0.95))
            : 0.0;
    double p99Sent =
        stats.success() > 0
            ? toLatencyValue(
                repository.loadLatencyPercentileAcrossTables(
                    List.of(definition.getSuccessTable()),
                    "latency_event_sent_ms",
                    day,
                    stats.success(),
                    0.99))
            : 0.0;
    double maxSent =
        stats.success() > 0
            ? toLatencyValue(
                repository.loadMaxLatencyAcrossTables(
                    List.of(definition.getSuccessTable()),
                    "latency_event_sent_ms",
                    day))
            : 0.0;
    return new LatencyStages(
        round2(avgReceived),
        round2(p95Received),
        round2(p99Received),
        round2(maxReceived),
        round2(avgSent),
        round2(p95Sent),
        round2(p99Sent),
        round2(maxSent));
  }

  private List<BucketPoint> buildHourlyBuckets(
      LocalDate day,
      Map<Integer, SuccessBucket> successBuckets,
      Map<Integer, FailureBucket> failureBuckets) {
    List<BucketPoint> buckets = new ArrayList<>(24);
    for (int hour = 0; hour < 24; hour++) {
      SuccessBucket successBucket = successBuckets.get(hour);
      FailureBucket failureBucket = failureBuckets.get(hour);
      BucketPoint point =
          buildBucketPoint(
              day.atTime(hour, 0),
              successBucket,
              failureBucket);
      buckets.add(point);
    }
    return buckets;
  }

  private List<BucketPoint> buildQuarterHourBuckets(
      LocalDate day,
      Map<BucketKey, SuccessBucket> successBuckets,
      Map<BucketKey, FailureBucket> failureBuckets) {
    List<BucketPoint> buckets = new ArrayList<>(96);
    for (int hour = 0; hour < 24; hour++) {
      for (int quarter = 0; quarter < 4; quarter++) {
        BucketKey key = new BucketKey(hour, quarter);
        BucketPoint point =
            buildBucketPoint(
                day.atTime(hour, quarter * 15),
                successBuckets.get(key),
                failureBuckets.get(key));
        buckets.add(point);
      }
    }
    return buckets;
  }

  private BucketPoint buildBucketPoint(
      LocalDateTime bucketStart,
      SuccessBucket successBucket,
      FailureBucket failureBucket) {
    long success = successBucket == null ? 0 : successBucket.successCount();
    long failure = failureBucket == null ? 0 : failureBucket.failureCount();
    long retriable = failureBucket == null ? 0 : failureBucket.retriableCount();
    long total = success + failure;
    double successLatency =
        successBucket == null || successBucket.avgLatencyMs() == null ? 0.0 : successBucket.avgLatencyMs();
    double failureLatency =
        failureBucket == null || failureBucket.avgLatencyMs() == null ? 0.0 : failureBucket.avgLatencyMs();
    double avgLatency =
        total > 0 ? (successLatency * success + failureLatency * failure) / total : 0.0;
    return new BucketPoint(
        bucketStart,
        success,
        failure,
        total,
        round2(successRate(success, total)),
        retriable,
        round2(avgLatency));
  }

  private void validateInterval(int intervalMinutes) {
    if (!properties.getBuckets().getIntervalsMinutes().contains(intervalMinutes)) {
      throw new BadRequestException("Unsupported intervalMinutes: " + intervalMinutes);
    }
  }

  private double successRate(long success, long total) {
    return total == 0 ? 0.0 : (success * 100.0) / total;
  }

  private double toLatencyValue(Double value) {
    return value == null ? 0.0 : value;
  }

  private double round2(double value) {
    return Math.round(value * 100.0) / 100.0;
  }

  private record EventStats(
      long total,
      long success,
      long failure,
      double successRate,
      long retriableFailures,
      double avgLatencyMs,
      double p95LatencyMs,
      double p99LatencyMs,
      double maxLatencyMs) {}
}

package com.vibe.events.service;

import com.vibe.events.config.AggregationProperties;
import com.vibe.events.config.CacheConfig;
import com.vibe.events.dto.BucketPoint;
import com.vibe.events.dto.EventBreakdownRow;
import com.vibe.events.dto.EventBucketsResponse;
import com.vibe.events.dto.EventSummaryResponse;
import com.vibe.events.dto.HomeAggregationResponse;
import com.vibe.events.dto.Kpis;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
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

  private HomeAggregationResponse computeHomeAggregation(LocalDate day) {
    long start = System.currentTimeMillis();
    List<EventBreakdownRow> eventRows = new ArrayList<>();
    long totalSuccess = 0;
    long totalFailure = 0;
    long totalRetriable = 0;
    double weightedLatencySum = 0.0;

    for (EventDefinition definition : registry.all()) {
      EventStats stats = computeEventStats(day, definition);
      eventRows.add(
          new EventBreakdownRow(
              definition.getKey(),
              stats.total(),
              stats.success(),
              stats.failure(),
              round2(stats.successRate()),
              stats.retriableFailures(),
              round2(stats.avgLatencyMs())));
      totalSuccess += stats.success();
      totalFailure += stats.failure();
      totalRetriable += stats.retriableFailures();
      weightedLatencySum += stats.avgLatencyMs() * stats.success();
    }

    long total = totalSuccess + totalFailure;
    double avgLatency = totalSuccess > 0 ? weightedLatencySum / totalSuccess : 0.0;
    Kpis kpis =
        new Kpis(
            total,
            totalSuccess,
            totalFailure,
            round2(successRate(totalSuccess, total)),
            totalRetriable,
            round2(avgLatency));

    HomeAggregationResponse response =
        new HomeAggregationResponse(day, LocalDateTime.now(), kpis, eventRows);
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
            round2(stats.avgLatencyMs()));
    EventSummaryResponse response =
        new EventSummaryResponse(day, eventKey, LocalDateTime.now(), kpis);
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
    double avgLatency = successTotals.avgLatencyMs() == null ? 0.0 : successTotals.avgLatencyMs();
    return new EventStats(
        total,
        success,
        failure,
        successRate(success, total),
        failureTotals.retriableCount(),
        avgLatency);
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
    double avgLatency =
        successBucket == null || successBucket.avgLatencyMs() == null
            ? 0.0
            : successBucket.avgLatencyMs();
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

  private double round2(double value) {
    return Math.round(value * 100.0) / 100.0;
  }

  private record EventStats(
      long total,
      long success,
      long failure,
      double successRate,
      long retriableFailures,
      double avgLatencyMs) {}
}

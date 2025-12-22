package com.vibe.events.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.vibe.events.registry.EventRegistry;
import java.time.Duration;
import java.util.List;
import com.github.benmanes.caffeine.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CacheConfig {
  public static final String HOME_AGG = "homeAgg";
  public static final String EVENT_SUMMARY = "eventSummary";
  public static final String EVENT_BUCKETS = "eventBuckets";

  @Bean
  public CacheManager cacheManager(EventRegistry registry, AggregationProperties properties) {
    int daysToKeep = properties.getCache().getDaysToKeep();
    int eventCount = Math.max(registry.all().size(), 1);
    List<Integer> intervals = properties.getBuckets().getIntervalsMinutes();
    int intervalCount = Math.max(intervals.size(), 1);

    CaffeineCacheManager manager = new CaffeineCacheManager();
    manager.setAllowNullValues(false);

    manager.registerCustomCache(
        HOME_AGG, buildCache(Duration.ofDays(daysToKeep), 32));
    manager.registerCustomCache(
        EVENT_SUMMARY,
        buildCache(Duration.ofDays(daysToKeep), eventCount * daysToKeep));
    manager.registerCustomCache(
        EVENT_BUCKETS,
        buildCache(
            Duration.ofDays(daysToKeep),
            eventCount * daysToKeep * intervalCount));

    return manager;
  }

  private Cache<Object, Object> buildCache(Duration ttl, int maxSize) {
    return Caffeine.newBuilder().expireAfterWrite(ttl).maximumSize(maxSize).build();
  }
}

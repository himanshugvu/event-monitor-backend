package com.vibe.events.config;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "aggregation")
public class AggregationProperties {
  private final Cache cache = new Cache();
  private final Buckets buckets = new Buckets();
  private final Warmup warmup = new Warmup();
  private int percentileBucketMs = 0;
  private int percentileSampleSizePerTable = 0;

  public Cache getCache() {
    return cache;
  }

  public Buckets getBuckets() {
    return buckets;
  }

  public Warmup getWarmup() {
    return warmup;
  }

  public int getPercentileBucketMs() {
    return percentileBucketMs;
  }

  public void setPercentileBucketMs(int percentileBucketMs) {
    this.percentileBucketMs = percentileBucketMs;
  }

  public int getPercentileSampleSizePerTable() {
    return percentileSampleSizePerTable;
  }

  public void setPercentileSampleSizePerTable(int percentileSampleSizePerTable) {
    this.percentileSampleSizePerTable = percentileSampleSizePerTable;
  }

  public static class Cache {
    private int daysToKeep = 7;

    public int getDaysToKeep() {
      return daysToKeep;
    }

    public void setDaysToKeep(int daysToKeep) {
      this.daysToKeep = daysToKeep;
    }
  }

  public static class Buckets {
    private List<Integer> intervalsMinutes = new ArrayList<>(List.of(60));

    public List<Integer> getIntervalsMinutes() {
      return intervalsMinutes;
    }

    public void setIntervalsMinutes(List<Integer> intervalsMinutes) {
      this.intervalsMinutes = intervalsMinutes;
    }
  }

  public static class Warmup {
    private boolean enabled = true;

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }
  }
}

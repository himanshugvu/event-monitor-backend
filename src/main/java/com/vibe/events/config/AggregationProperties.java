package com.vibe.events.config;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "aggregation")
public class AggregationProperties {
  private final Cache cache = new Cache();
  private final Buckets buckets = new Buckets();

  public Cache getCache() {
    return cache;
  }

  public Buckets getBuckets() {
    return buckets;
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
}

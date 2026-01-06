package com.vibe.events.util;

import java.time.LocalDate;

public final class CacheKeys {
  private CacheKeys() {}

  public static String homeKey(LocalDate day) {
    return day.toString();
  }

  public static String eventSummaryKey(LocalDate day, String eventKey) {
    return day + "|" + eventKey;
  }

  public static String eventBucketsKey(LocalDate day, String eventKey, int intervalMinutes) {
    return day + "|" + eventKey + "|" + intervalMinutes;
  }

  public static String homeBucketsKey(LocalDate day, int intervalMinutes) {
    return day + "|" + intervalMinutes;
  }
}

package com.vibe.events.repo;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class AggregationRepository {
  private final JdbcClient jdbcClient;

  public AggregationRepository(JdbcClient jdbcClient) {
    this.jdbcClient = jdbcClient;
  }

  public SuccessTotals loadSuccessTotals(String successTable, LocalDate day) {
    String sql =
        "SELECT COUNT(*) AS success_count, "
            + "AVG(latency_ms) AS avg_latency_ms "
            + "FROM "
            + successTable
            + " WHERE event_date = :day";
    return jdbcClient
        .sql(sql)
        .param("day", day)
        .query(
            (rs, rowNum) ->
                new SuccessTotals(
                    rs.getLong("success_count"),
                    rs.getObject("avg_latency_ms", Double.class),
                    null))
        .single();
  }

  public FailureTotals loadFailureTotals(String failureTable, LocalDate day) {
    String sql =
        "SELECT COUNT(*) AS failure_count, "
            + "SUM(CASE WHEN retriable = 1 THEN 1 ELSE 0 END) AS retriable_count, "
            + "AVG(latency_ms) AS avg_latency_ms "
            + "FROM "
            + failureTable
            + " WHERE event_date = :day";
    return jdbcClient
        .sql(sql)
        .param("day", day)
        .query(
            (rs, rowNum) ->
                new FailureTotals(
                    rs.getLong("failure_count"),
                    rs.getLong("retriable_count"),
                    rs.getObject("avg_latency_ms", Double.class)))
        .single();
  }

  public Map<Integer, SuccessBucket> loadHourlySuccessBuckets(String successTable, LocalDate day) {
    String sql =
        "SELECT HOUR(event_date_time) AS hour_of_day, "
            + "COUNT(*) AS success_count, "
            + "AVG(latency_ms) AS avg_latency_ms "
            + "FROM "
            + successTable
            + " WHERE event_date = :day "
            + "GROUP BY HOUR(event_date_time) "
            + "ORDER BY hour_of_day";
    List<SuccessBucket> buckets =
        jdbcClient
            .sql(sql)
            .param("day", day)
            .query(
                (rs, rowNum) ->
                    new SuccessBucket(
                        rs.getInt("hour_of_day"),
                        null,
                        rs.getLong("success_count"),
                        rs.getObject("avg_latency_ms", Double.class)))
            .list();
    Map<Integer, SuccessBucket> result = new HashMap<>();
    for (SuccessBucket bucket : buckets) {
      result.put(bucket.hourOfDay(), bucket);
    }
    return result;
  }

  public Map<Integer, FailureBucket> loadHourlyFailureBuckets(String failureTable, LocalDate day) {
    String sql =
        "SELECT HOUR(event_date_time) AS hour_of_day, "
            + "COUNT(*) AS failure_count, "
            + "SUM(CASE WHEN retriable = 1 THEN 1 ELSE 0 END) AS retriable_count, "
            + "AVG(latency_ms) AS avg_latency_ms "
            + "FROM "
            + failureTable
            + " WHERE event_date = :day "
            + "GROUP BY HOUR(event_date_time) "
            + "ORDER BY hour_of_day";
    List<FailureBucket> buckets =
        jdbcClient
            .sql(sql)
            .param("day", day)
            .query(
                (rs, rowNum) ->
                    new FailureBucket(
                        rs.getInt("hour_of_day"),
                        null,
                        rs.getLong("failure_count"),
                        rs.getLong("retriable_count"),
                        rs.getObject("avg_latency_ms", Double.class)))
            .list();
    Map<Integer, FailureBucket> result = new HashMap<>();
    for (FailureBucket bucket : buckets) {
      result.put(bucket.hourOfDay(), bucket);
    }
    return result;
  }

  public Map<BucketKey, SuccessBucket> loadQuarterHourSuccessBuckets(
      String successTable, LocalDate day) {
    String sql =
        "SELECT HOUR(event_date_time) AS hour_of_day, "
            + "FLOOR(MINUTE(event_date_time) / 15) AS quarter, "
            + "COUNT(*) AS success_count, "
            + "AVG(latency_ms) AS avg_latency_ms "
            + "FROM "
            + successTable
            + " WHERE event_date = :day "
            + "GROUP BY HOUR(event_date_time), FLOOR(MINUTE(event_date_time) / 15) "
            + "ORDER BY hour_of_day, quarter";
    List<SuccessBucket> buckets =
        jdbcClient
            .sql(sql)
            .param("day", day)
            .query(
                (rs, rowNum) ->
                    new SuccessBucket(
                        rs.getInt("hour_of_day"),
                        rs.getInt("quarter"),
                        rs.getLong("success_count"),
                        rs.getObject("avg_latency_ms", Double.class)))
            .list();
    Map<BucketKey, SuccessBucket> result = new HashMap<>();
    for (SuccessBucket bucket : buckets) {
      result.put(new BucketKey(bucket.hourOfDay(), bucket.quarter()), bucket);
    }
    return result;
  }

  public Map<BucketKey, FailureBucket> loadQuarterHourFailureBuckets(
      String failureTable, LocalDate day) {
    String sql =
        "SELECT HOUR(event_date_time) AS hour_of_day, "
            + "FLOOR(MINUTE(event_date_time) / 15) AS quarter, "
            + "COUNT(*) AS failure_count, "
            + "SUM(CASE WHEN retriable = 1 THEN 1 ELSE 0 END) AS retriable_count, "
            + "AVG(latency_ms) AS avg_latency_ms "
            + "FROM "
            + failureTable
            + " WHERE event_date = :day "
            + "GROUP BY HOUR(event_date_time), FLOOR(MINUTE(event_date_time) / 15) "
            + "ORDER BY hour_of_day, quarter";
    List<FailureBucket> buckets =
        jdbcClient
            .sql(sql)
            .param("day", day)
            .query(
                (rs, rowNum) ->
                    new FailureBucket(
                        rs.getInt("hour_of_day"),
                        rs.getInt("quarter"),
                        rs.getLong("failure_count"),
                        rs.getLong("retriable_count"),
                        rs.getObject("avg_latency_ms", Double.class)))
            .list();
    Map<BucketKey, FailureBucket> result = new HashMap<>();
    for (FailureBucket bucket : buckets) {
      result.put(new BucketKey(bucket.hourOfDay(), bucket.quarter()), bucket);
    }
    return result;
  }

  public Double loadAverageLatency(String table, String column, LocalDate day) {
    String sql =
        "SELECT AVG(" + column + ") AS avg_latency_ms FROM " + table + " WHERE event_date = :day";
    return jdbcClient
        .sql(sql)
        .param("day", day)
        .query((rs, rowNum) -> rs.getObject("avg_latency_ms", Double.class))
        .optional()
        .orElse(null);
  }

  public Double loadLatencyPercentileAcrossTables(
      List<String> tables, String column, LocalDate day, long totalCount, double percentile) {
    if (totalCount <= 0 || tables == null || tables.isEmpty()) {
      return null;
    }
    String unionSql =
        tables.stream()
            .map((table) -> baseLatencySelect(table, column))
            .collect(Collectors.joining(" UNION ALL "));
    return loadLatencyPercentile(unionSql, day, totalCount, percentile);
  }

  public Double loadMaxLatencyAcrossTables(List<String> tables, String column, LocalDate day) {
    if (tables == null || tables.isEmpty()) {
      return null;
    }
    String unionSql =
        tables.stream()
            .map((table) -> baseLatencySelect(table, column))
            .collect(Collectors.joining(" UNION ALL "));
    String sql = "SELECT MAX(latency_ms) AS max_latency FROM (" + unionSql + ") latency_values";
    return jdbcClient
        .sql(sql)
        .param("day", day)
        .query((rs, rowNum) -> rs.getObject("max_latency", Double.class))
        .optional()
        .orElse(null);
  }

  private String baseLatencySelect(String table, String column) {
    return "SELECT " + column + " AS latency_ms FROM " + table + " WHERE event_date = :day";
  }

  private Double loadLatencyPercentile(
      String baseSql, LocalDate day, long totalCount, double percentile) {
    if (totalCount <= 0) {
      return null;
    }
    long offset = (long) Math.ceil(totalCount * percentile) - 1;
    if (offset < 0) {
      offset = 0;
    }
    String sql =
        "SELECT latency_ms FROM ("
            + baseSql
            + ") latency_sorted ORDER BY latency_ms LIMIT "
            + offset
            + ", 1";
    Optional<Double> result =
        jdbcClient
            .sql(sql)
            .param("day", day)
            .query((rs, rowNum) -> rs.getDouble("latency_ms"))
            .optional();
    return result.orElse(null);
  }
}

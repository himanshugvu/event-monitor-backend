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
            + "AVG(TIMESTAMPDIFF(MICROSECOND, event_received_timestamp, event_sent_timestamp)) / 1000 "
            + "AS avg_latency_ms "
            + "FROM "
            + successTable
            + " WHERE day = :day";
    SuccessTotals totals =
        jdbcClient
            .sql(sql)
            .param("day", day)
            .query(
                (rs, rowNum) ->
                    new SuccessTotals(
                        rs.getLong("success_count"),
                        rs.getObject("avg_latency_ms", Double.class),
                        null))
            .single();
    Double p95 = loadSuccessLatencyPercentile(successTable, day, totals.successCount(), 0.95);
    return new SuccessTotals(totals.successCount(), totals.avgLatencyMs(), p95);
  }

  public FailureTotals loadFailureTotals(String failureTable, LocalDate day) {
    String sql =
        "SELECT COUNT(*) AS failure_count, "
            + "SUM(CASE WHEN retriable = 1 THEN 1 ELSE 0 END) AS retriable_count "
            + "FROM "
            + failureTable
            + " WHERE day = :day";
    return jdbcClient
        .sql(sql)
        .param("day", day)
        .query(
            (rs, rowNum) ->
                new FailureTotals(rs.getLong("failure_count"), rs.getLong("retriable_count")))
        .single();
  }

  public Map<Integer, SuccessBucket> loadHourlySuccessBuckets(String successTable, LocalDate day) {
    String sql =
        "SELECT HOUR(event_received_timestamp) AS hour_of_day, "
            + "COUNT(*) AS success_count, "
            + "AVG(TIMESTAMPDIFF(MICROSECOND, event_received_timestamp, event_sent_timestamp)) / 1000 "
            + "AS avg_latency_ms "
            + "FROM "
            + successTable
            + " WHERE day = :day "
            + "GROUP BY HOUR(event_received_timestamp) "
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
        "SELECT HOUR(event_received_timestamp) AS hour_of_day, "
            + "COUNT(*) AS failure_count, "
            + "SUM(CASE WHEN retriable = 1 THEN 1 ELSE 0 END) AS retriable_count "
            + "FROM "
            + failureTable
            + " WHERE day = :day "
            + "GROUP BY HOUR(event_received_timestamp) "
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
                        rs.getLong("retriable_count")))
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
        "SELECT HOUR(event_received_timestamp) AS hour_of_day, "
            + "FLOOR(MINUTE(event_received_timestamp) / 15) AS quarter, "
            + "COUNT(*) AS success_count, "
            + "AVG(TIMESTAMPDIFF(MICROSECOND, event_received_timestamp, event_sent_timestamp)) / 1000 "
            + "AS avg_latency_ms "
            + "FROM "
            + successTable
            + " WHERE day = :day "
            + "GROUP BY HOUR(event_received_timestamp), FLOOR(MINUTE(event_received_timestamp) / 15) "
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
        "SELECT HOUR(event_received_timestamp) AS hour_of_day, "
            + "FLOOR(MINUTE(event_received_timestamp) / 15) AS quarter, "
            + "COUNT(*) AS failure_count, "
            + "SUM(CASE WHEN retriable = 1 THEN 1 ELSE 0 END) AS retriable_count "
            + "FROM "
            + failureTable
            + " WHERE day = :day "
            + "GROUP BY HOUR(event_received_timestamp), FLOOR(MINUTE(event_received_timestamp) / 15) "
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
                        rs.getLong("retriable_count")))
            .list();
    Map<BucketKey, FailureBucket> result = new HashMap<>();
    for (FailureBucket bucket : buckets) {
      result.put(new BucketKey(bucket.hourOfDay(), bucket.quarter()), bucket);
    }
    return result;
  }

  public Double loadSuccessLatencyPercentileAcrossTables(
      List<String> successTables, LocalDate day, long totalSuccess, double percentile) {
    if (totalSuccess <= 0 || successTables == null || successTables.isEmpty()) {
      return null;
    }
    String unionSql =
        successTables.stream()
            .map(this::baseLatencySelect)
            .collect(Collectors.joining(" UNION ALL "));
    return loadLatencyPercentile(unionSql, day, totalSuccess, percentile);
  }

  private Double loadSuccessLatencyPercentile(
      String successTable, LocalDate day, long totalSuccess, double percentile) {
    if (totalSuccess <= 0) {
      return null;
    }
    return loadLatencyPercentile(baseLatencySelect(successTable), day, totalSuccess, percentile);
  }

  private String baseLatencySelect(String successTable) {
    return "SELECT TIMESTAMPDIFF(MICROSECOND, event_received_timestamp, event_sent_timestamp) / 1000 "
        + "AS latency_ms FROM "
        + successTable
        + " WHERE day = :day";
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

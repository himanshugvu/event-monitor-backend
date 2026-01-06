package com.vibe.events.repo;

import java.time.LocalDate;
import java.time.LocalDateTime;
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
    DateTimeRange range = dayRange(day);
    String sql =
        "SELECT COUNT(*) AS success_count, "
            + "AVG(latency_ms) AS avg_latency_ms, "
            + "AVG(latency_event_received_ms) AS avg_received_latency_ms, "
            + "AVG(latency_event_sent_ms) AS avg_sent_latency_ms "
            + "FROM "
            + successTable
            + " WHERE event_datetime >= :startTs AND event_datetime < :endTs";
    return jdbcClient
        .sql(sql)
        .param("startTs", range.start())
        .param("endTs", range.end())
        .query(
            (rs, rowNum) ->
                new SuccessTotals(
                    rs.getLong("success_count"),
                    rs.getObject("avg_latency_ms", Double.class),
                    rs.getObject("avg_received_latency_ms", Double.class),
                    rs.getObject("avg_sent_latency_ms", Double.class)))
        .single();
  }

  public FailureTotals loadFailureTotals(String failureTable, LocalDate day) {
    DateTimeRange range = dayRange(day);
    String sql =
        "SELECT COUNT(*) AS failure_count, "
            + "SUM(CASE WHEN retriable = 1 THEN 1 ELSE 0 END) AS retriable_count, "
            + "AVG(latency_ms) AS avg_latency_ms, "
            + "AVG(latency_event_received_ms) AS avg_received_latency_ms "
            + "FROM "
            + failureTable
            + " WHERE event_datetime >= :startTs AND event_datetime < :endTs";
    return jdbcClient
        .sql(sql)
        .param("startTs", range.start())
        .param("endTs", range.end())
        .query(
            (rs, rowNum) ->
                new FailureTotals(
                    rs.getLong("failure_count"),
                    rs.getLong("retriable_count"),
                    rs.getObject("avg_latency_ms", Double.class),
                    rs.getObject("avg_received_latency_ms", Double.class)))
        .single();
  }

  public Map<Integer, SuccessBucket> loadHourlySuccessBuckets(String successTable, LocalDate day) {
    DateTimeRange range = dayRange(day);
    String sql =
        "SELECT HOUR(event_datetime) AS hour_of_day, "
            + "COUNT(*) AS success_count, "
            + "AVG(latency_ms) AS avg_latency_ms "
            + "FROM "
            + successTable
            + " WHERE event_datetime >= :startTs AND event_datetime < :endTs "
            + "GROUP BY HOUR(event_datetime) "
            + "ORDER BY hour_of_day";
    List<SuccessBucket> buckets =
        jdbcClient
            .sql(sql)
            .param("startTs", range.start())
            .param("endTs", range.end())
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
    DateTimeRange range = dayRange(day);
    String sql =
        "SELECT HOUR(event_datetime) AS hour_of_day, "
            + "COUNT(*) AS failure_count, "
            + "SUM(CASE WHEN retriable = 1 THEN 1 ELSE 0 END) AS retriable_count, "
            + "AVG(latency_ms) AS avg_latency_ms "
            + "FROM "
            + failureTable
            + " WHERE event_datetime >= :startTs AND event_datetime < :endTs "
            + "GROUP BY HOUR(event_datetime) "
            + "ORDER BY hour_of_day";
    List<FailureBucket> buckets =
        jdbcClient
            .sql(sql)
            .param("startTs", range.start())
            .param("endTs", range.end())
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
    DateTimeRange range = dayRange(day);
    String sql =
        "SELECT HOUR(event_datetime) AS hour_of_day, "
            + "FLOOR(MINUTE(event_datetime) / 15) AS quarter, "
            + "COUNT(*) AS success_count, "
            + "AVG(latency_ms) AS avg_latency_ms "
            + "FROM "
            + successTable
            + " WHERE event_datetime >= :startTs AND event_datetime < :endTs "
            + "GROUP BY HOUR(event_datetime), FLOOR(MINUTE(event_datetime) / 15) "
            + "ORDER BY hour_of_day, quarter";
    List<SuccessBucket> buckets =
        jdbcClient
            .sql(sql)
            .param("startTs", range.start())
            .param("endTs", range.end())
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
    DateTimeRange range = dayRange(day);
    String sql =
        "SELECT HOUR(event_datetime) AS hour_of_day, "
            + "FLOOR(MINUTE(event_datetime) / 15) AS quarter, "
            + "COUNT(*) AS failure_count, "
            + "SUM(CASE WHEN retriable = 1 THEN 1 ELSE 0 END) AS retriable_count, "
            + "AVG(latency_ms) AS avg_latency_ms "
            + "FROM "
            + failureTable
            + " WHERE event_datetime >= :startTs AND event_datetime < :endTs "
            + "GROUP BY HOUR(event_datetime), FLOOR(MINUTE(event_datetime) / 15) "
            + "ORDER BY hour_of_day, quarter";
    List<FailureBucket> buckets =
        jdbcClient
            .sql(sql)
            .param("startTs", range.start())
            .param("endTs", range.end())
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
    DateTimeRange range = dayRange(day);
    String sql =
        "SELECT AVG("
            + column
            + ") AS avg_latency_ms FROM "
            + table
            + " WHERE event_datetime >= :startTs AND event_datetime < :endTs";
    return jdbcClient
        .sql(sql)
        .param("startTs", range.start())
        .param("endTs", range.end())
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
    return loadLatencyPercentile(unionSql, day, totalCount, percentile, null);
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
    DateTimeRange range = dayRange(day);
    return jdbcClient
        .sql(sql)
        .param("startTs", range.start())
        .param("endTs", range.end())
        .query((rs, rowNum) -> rs.getObject("max_latency", Double.class))
        .optional()
        .orElse(null);
  }

  public LatencyStats loadLatencyStatsAcrossTables(
      List<String> tables,
      String column,
      LocalDate day,
      long totalCount,
      int percentileBucketMs,
      int percentileSampleSizePerTable) {
    if (totalCount <= 0 || tables == null || tables.isEmpty()) {
      return new LatencyStats(null, null, null);
    }
    if (percentileSampleSizePerTable > 0) {
      String unionSql =
          tables.stream()
              .map((table) -> baseLatencySampleSelect(table, column))
              .collect(Collectors.joining(" UNION ALL "));
      long sampleCount =
          Math.min(totalCount, (long) percentileSampleSizePerTable * tables.size());
      Double p95 =
          loadLatencyPercentile(
              unionSql, day, sampleCount, 0.95, percentileSampleSizePerTable);
      Double p99 =
          loadLatencyPercentile(
              unionSql, day, sampleCount, 0.99, percentileSampleSizePerTable);
      Double max = loadLatencyMax(unionSql, day, percentileSampleSizePerTable);
      return new LatencyStats(p95, p99, max);
    }
    if (percentileBucketMs <= 0) {
      Double p95 = loadLatencyPercentileAcrossTables(tables, column, day, totalCount, 0.95);
      Double p99 = loadLatencyPercentileAcrossTables(tables, column, day, totalCount, 0.99);
      Double max = loadMaxLatencyAcrossTables(tables, column, day);
      return new LatencyStats(p95, p99, max);
    }

    List<LatencyBucket> buckets =
        loadLatencyBucketsAcrossTables(tables, column, day, percentileBucketMs);
    if (buckets.isEmpty()) {
      return new LatencyStats(null, null, null);
    }
    long targetP95 = Math.max(0, (long) Math.ceil(totalCount * 0.95));
    long targetP99 = Math.max(0, (long) Math.ceil(totalCount * 0.99));
    long running = 0;
    Double p95 = null;
    Double p99 = null;
    for (LatencyBucket bucket : buckets) {
      running += bucket.count();
      if (p95 == null && running >= targetP95) {
        p95 = (bucket.bucket() + 1) * (double) percentileBucketMs;
      }
      if (p99 == null && running >= targetP99) {
        p99 = (bucket.bucket() + 1) * (double) percentileBucketMs;
      }
      if (p95 != null && p99 != null) {
        break;
      }
    }
    LatencyBucket last = buckets.get(buckets.size() - 1);
    Double max = (last.bucket() + 1) * (double) percentileBucketMs;
    return new LatencyStats(p95, p99, max);
  }

  private String baseLatencySelect(String table, String column) {
    return "SELECT "
        + column
        + " AS latency_ms FROM "
        + table
        + " WHERE event_datetime >= :startTs AND event_datetime < :endTs";
  }

  private String baseLatencySampleSelect(String table, String column) {
    return "(SELECT "
        + column
        + " AS latency_ms FROM "
        + table
        + " WHERE event_datetime >= :startTs AND event_datetime < :endTs"
        + " ORDER BY event_datetime LIMIT :sampleLimit)";
  }

  private List<LatencyBucket> loadLatencyBucketsAcrossTables(
      List<String> tables, String column, LocalDate day, int bucketSizeMs) {
    String unionSql =
        tables.stream()
            .map((table) -> baseLatencyBucketSelect(table, column))
            .collect(Collectors.joining(" UNION ALL "));
    String sql =
        "SELECT bucket, SUM(bucket_count) AS bucket_count FROM ("
            + unionSql
            + ") latency_buckets GROUP BY bucket ORDER BY bucket";
    DateTimeRange range = dayRange(day);
    return jdbcClient
        .sql(sql)
        .param("startTs", range.start())
        .param("endTs", range.end())
        .param("bucketSize", bucketSizeMs)
        .query((rs, rowNum) -> new LatencyBucket(rs.getInt("bucket"), rs.getLong("bucket_count")))
        .list();
  }

  private String baseLatencyBucketSelect(String table, String column) {
    return "SELECT FLOOR("
        + column
        + " / :bucketSize) AS bucket, COUNT(*) AS bucket_count FROM "
        + table
        + " WHERE event_datetime >= :startTs AND event_datetime < :endTs GROUP BY bucket";
  }

  private Double loadLatencyPercentile(
      String baseSql,
      LocalDate day,
      long totalCount,
      double percentile,
      Integer sampleLimit) {
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
    DateTimeRange range = dayRange(day);
    JdbcClient.StatementSpec statement =
        jdbcClient.sql(sql).param("startTs", range.start()).param("endTs", range.end());
    if (sampleLimit != null) {
      statement = statement.param("sampleLimit", sampleLimit);
    }
    Optional<Double> result =
        statement.query((rs, rowNum) -> rs.getDouble("latency_ms")).optional();
    return result.orElse(null);
  }

  private Double loadLatencyMax(String baseSql, LocalDate day, Integer sampleLimit) {
    String sql = "SELECT MAX(latency_ms) AS max_latency FROM (" + baseSql + ") latency_values";
    DateTimeRange range = dayRange(day);
    JdbcClient.StatementSpec statement =
        jdbcClient.sql(sql).param("startTs", range.start()).param("endTs", range.end());
    if (sampleLimit != null) {
      statement = statement.param("sampleLimit", sampleLimit);
    }
    return statement
        .query((rs, rowNum) -> rs.getObject("max_latency", Double.class))
        .optional()
        .orElse(null);
  }

  private DateTimeRange dayRange(LocalDate day) {
    return new DateTimeRange(day.atStartOfDay(), day.plusDays(1).atStartOfDay());
  }

  private record DateTimeRange(LocalDateTime start, LocalDateTime end) {}

  public record LatencyStats(Double p95, Double p99, Double max) {}

  private record LatencyBucket(int bucket, long count) {}
}


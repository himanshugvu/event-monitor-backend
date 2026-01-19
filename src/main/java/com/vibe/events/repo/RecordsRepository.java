package com.vibe.events.repo;

import com.vibe.events.util.RowMapperUtil;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class RecordsRepository {
  private final JdbcClient jdbcClient;
  private static final String SUCCESS_LIST_COLUMNS =
      "id, event_datetime, event_trace_id, account_number, customer_type, "
          + "source_partition_id, target_partition_id, source_offset, target_offset, "
          + "latency_event_received_ms, latency_ms";
  private static final String FAILURE_LIST_COLUMNS =
      "id, event_datetime, event_trace_id, account_number, exception_type, "
          + "exception_message, retriable, retry_attempt, "
          + "source_partition_id, source_offset, "
          + "latency_event_received_ms, latency_ms";

  public RecordsRepository(JdbcClient jdbcClient) {
    this.jdbcClient = jdbcClient;
  }

  public List<Map<String, Object>> loadSuccessRows(
      String successTable,
      LocalDateTime startTimestamp,
      LocalDateTime endTimestamp,
      boolean endInclusive,
      String traceId,
      String messageKey,
      String accountNumber,
      Long latencyMin,
      Long latencyMax,
      Long receivedLatencyMin,
      Long receivedLatencyMax,
      int offset,
      int limit) {
    StringBuilder sql =
        new StringBuilder("SELECT ").append(SUCCESS_LIST_COLUMNS).append(" FROM ").append(successTable);
    Map<String, Object> params = new HashMap<>();
    sql.append(" WHERE event_datetime >= :startTs AND event_datetime ");
    sql.append(endInclusive ? "<= :endTs" : "< :endTs");
    params.put("startTs", startTimestamp);
    params.put("endTs", endTimestamp);

    if (traceId != null && !traceId.isBlank()) {
      sql.append(" AND event_trace_id = :traceId");
      params.put("traceId", traceId);
    }
    if (messageKey != null && !messageKey.isBlank()) {
      sql.append(" AND message_key = :messageKey");
      params.put("messageKey", messageKey);
    }
    if (accountNumber != null && !accountNumber.isBlank()) {
      sql.append(" AND account_number = :accountNumber");
      params.put("accountNumber", accountNumber);
    }
    if (latencyMin != null) {
      sql.append(" AND latency_ms >= :latencyMin");
      params.put("latencyMin", latencyMin);
    }
    if (latencyMax != null) {
      sql.append(" AND latency_ms <= :latencyMax");
      params.put("latencyMax", latencyMax);
    }
    if (receivedLatencyMin != null) {
      sql.append(" AND latency_event_received_ms >= :receivedLatencyMin");
      params.put("receivedLatencyMin", receivedLatencyMin);
    }
    if (receivedLatencyMax != null) {
      sql.append(" AND latency_event_received_ms <= :receivedLatencyMax");
      params.put("receivedLatencyMax", receivedLatencyMax);
    }

    sql.append(" ORDER BY event_datetime DESC, id DESC LIMIT :limit OFFSET :offset");
    params.put("limit", limit);
    params.put("offset", offset);

    return jdbcClient.sql(sql.toString()).params(params).query(RowMapperUtil.dynamicRowMapper()).list();
  }

  public long loadSuccessRowCount(
      String successTable,
      LocalDateTime startTimestamp,
      LocalDateTime endTimestamp,
      boolean endInclusive,
      String traceId,
      String messageKey,
      String accountNumber,
      Long latencyMin,
      Long latencyMax,
      Long receivedLatencyMin,
      Long receivedLatencyMax) {
    StringBuilder sql = new StringBuilder("SELECT COUNT(*) AS total_count FROM ").append(successTable);
    Map<String, Object> params = new HashMap<>();
    sql.append(" WHERE event_datetime >= :startTs AND event_datetime ");
    sql.append(endInclusive ? "<= :endTs" : "< :endTs");
    params.put("startTs", startTimestamp);
    params.put("endTs", endTimestamp);

    if (traceId != null && !traceId.isBlank()) {
      sql.append(" AND event_trace_id = :traceId");
      params.put("traceId", traceId);
    }
    if (messageKey != null && !messageKey.isBlank()) {
      sql.append(" AND message_key = :messageKey");
      params.put("messageKey", messageKey);
    }
    if (accountNumber != null && !accountNumber.isBlank()) {
      sql.append(" AND account_number = :accountNumber");
      params.put("accountNumber", accountNumber);
    }
    if (latencyMin != null) {
      sql.append(" AND latency_ms >= :latencyMin");
      params.put("latencyMin", latencyMin);
    }
    if (latencyMax != null) {
      sql.append(" AND latency_ms <= :latencyMax");
      params.put("latencyMax", latencyMax);
    }
    if (receivedLatencyMin != null) {
      sql.append(" AND latency_event_received_ms >= :receivedLatencyMin");
      params.put("receivedLatencyMin", receivedLatencyMin);
    }
    if (receivedLatencyMax != null) {
      sql.append(" AND latency_event_received_ms <= :receivedLatencyMax");
      params.put("receivedLatencyMax", receivedLatencyMax);
    }

    return jdbcClient
        .sql(sql.toString())
        .params(params)
        .query((rs, rowNum) -> rs.getLong("total_count"))
        .single();
  }

  public List<Map<String, Object>> loadFailureRows(
      String failureTable,
      LocalDateTime startTimestamp,
      LocalDateTime endTimestamp,
      boolean endInclusive,
      String traceId,
      String messageKey,
      String accountNumber,
      Long latencyMin,
      Long latencyMax,
      Long receivedLatencyMin,
      Long receivedLatencyMax,
      String exceptionType,
      Boolean retriable,
      Integer retryAttemptMin,
      Integer retryAttemptMax,
      int offset,
      int limit) {
    StringBuilder sql =
        new StringBuilder("SELECT ").append(FAILURE_LIST_COLUMNS).append(" FROM ").append(failureTable);
    Map<String, Object> params = new HashMap<>();
    sql.append(" WHERE event_datetime >= :startTs AND event_datetime ");
    sql.append(endInclusive ? "<= :endTs" : "< :endTs");
    params.put("startTs", startTimestamp);
    params.put("endTs", endTimestamp);

    if (traceId != null && !traceId.isBlank()) {
      sql.append(" AND event_trace_id = :traceId");
      params.put("traceId", traceId);
    }
    if (messageKey != null && !messageKey.isBlank()) {
      sql.append(" AND message_key = :messageKey");
      params.put("messageKey", messageKey);
    }
    if (accountNumber != null && !accountNumber.isBlank()) {
      sql.append(" AND account_number = :accountNumber");
      params.put("accountNumber", accountNumber);
    }
    if (latencyMin != null) {
      sql.append(" AND latency_ms >= :latencyMin");
      params.put("latencyMin", latencyMin);
    }
    if (latencyMax != null) {
      sql.append(" AND latency_ms <= :latencyMax");
      params.put("latencyMax", latencyMax);
    }
    if (receivedLatencyMin != null) {
      sql.append(" AND latency_event_received_ms >= :receivedLatencyMin");
      params.put("receivedLatencyMin", receivedLatencyMin);
    }
    if (receivedLatencyMax != null) {
      sql.append(" AND latency_event_received_ms <= :receivedLatencyMax");
      params.put("receivedLatencyMax", receivedLatencyMax);
    }
    if (exceptionType != null && !exceptionType.isBlank()) {
      sql.append(" AND exception_type = :exceptionType");
      params.put("exceptionType", exceptionType);
    }
    if (retriable != null) {
      sql.append(" AND retriable = :retriable");
      params.put("retriable", retriable ? 1 : 0);
    }
    if (retryAttemptMin != null) {
      sql.append(" AND retry_attempt >= :retryAttemptMin");
      params.put("retryAttemptMin", retryAttemptMin);
    }
    if (retryAttemptMax != null) {
      sql.append(" AND retry_attempt <= :retryAttemptMax");
      params.put("retryAttemptMax", retryAttemptMax);
    }

    sql.append(" ORDER BY event_datetime DESC, id DESC LIMIT :limit OFFSET :offset");
    params.put("limit", limit);
    params.put("offset", offset);

    return jdbcClient.sql(sql.toString()).params(params).query(RowMapperUtil.dynamicRowMapper()).list();
  }

  public long loadFailureRowCount(
      String failureTable,
      LocalDateTime startTimestamp,
      LocalDateTime endTimestamp,
      boolean endInclusive,
      String traceId,
      String messageKey,
      String accountNumber,
      Long latencyMin,
      Long latencyMax,
      Long receivedLatencyMin,
      Long receivedLatencyMax,
      String exceptionType,
      Boolean retriable,
      Integer retryAttemptMin,
      Integer retryAttemptMax) {
    StringBuilder sql = new StringBuilder("SELECT COUNT(*) AS total_count FROM ").append(failureTable);
    Map<String, Object> params = new HashMap<>();
    sql.append(" WHERE event_datetime >= :startTs AND event_datetime ");
    sql.append(endInclusive ? "<= :endTs" : "< :endTs");
    params.put("startTs", startTimestamp);
    params.put("endTs", endTimestamp);

    if (traceId != null && !traceId.isBlank()) {
      sql.append(" AND event_trace_id = :traceId");
      params.put("traceId", traceId);
    }
    if (messageKey != null && !messageKey.isBlank()) {
      sql.append(" AND message_key = :messageKey");
      params.put("messageKey", messageKey);
    }
    if (accountNumber != null && !accountNumber.isBlank()) {
      sql.append(" AND account_number = :accountNumber");
      params.put("accountNumber", accountNumber);
    }
    if (latencyMin != null) {
      sql.append(" AND latency_ms >= :latencyMin");
      params.put("latencyMin", latencyMin);
    }
    if (latencyMax != null) {
      sql.append(" AND latency_ms <= :latencyMax");
      params.put("latencyMax", latencyMax);
    }
    if (receivedLatencyMin != null) {
      sql.append(" AND latency_event_received_ms >= :receivedLatencyMin");
      params.put("receivedLatencyMin", receivedLatencyMin);
    }
    if (receivedLatencyMax != null) {
      sql.append(" AND latency_event_received_ms <= :receivedLatencyMax");
      params.put("receivedLatencyMax", receivedLatencyMax);
    }
    if (exceptionType != null && !exceptionType.isBlank()) {
      sql.append(" AND exception_type = :exceptionType");
      params.put("exceptionType", exceptionType);
    }
    if (retriable != null) {
      sql.append(" AND retriable = :retriable");
      params.put("retriable", retriable ? 1 : 0);
    }
    if (retryAttemptMin != null) {
      sql.append(" AND retry_attempt >= :retryAttemptMin");
      params.put("retryAttemptMin", retryAttemptMin);
    }
    if (retryAttemptMax != null) {
      sql.append(" AND retry_attempt <= :retryAttemptMax");
      params.put("retryAttemptMax", retryAttemptMax);
    }

    return jdbcClient
        .sql(sql.toString())
        .params(params)
        .query((rs, rowNum) -> rs.getLong("total_count"))
        .single();
  }

  public List<String> loadFailureExceptionTypes(
      String failureTable,
      LocalDateTime startTimestamp,
      LocalDateTime endTimestamp,
      boolean endInclusive) {
    StringBuilder sql = new StringBuilder("SELECT DISTINCT exception_type FROM ").append(failureTable);
    Map<String, Object> params = new HashMap<>();
    sql.append(" WHERE event_datetime >= :startTs AND event_datetime ");
    sql.append(endInclusive ? "<= :endTs" : "< :endTs");
    params.put("startTs", startTimestamp);
    params.put("endTs", endTimestamp);
    sql.append(" AND exception_type IS NOT NULL AND exception_type <> ''");
    sql.append(" ORDER BY exception_type");

    return jdbcClient
        .sql(sql.toString())
        .params(params)
        .query((rs, rowNum) -> rs.getString("exception_type"))
        .list();
  }

  public Map<String, Object> loadSuccessRowById(String successTable, long id) {
    String sql = "SELECT * FROM " + successTable + " WHERE id = :id";
    return jdbcClient
        .sql(sql)
        .param("id", id)
        .query(RowMapperUtil.dynamicRowMapper())
        .optional()
        .orElse(null);
  }

  public Map<String, Object> loadFailureRowById(String failureTable, long id) {
    String sql = "SELECT * FROM " + failureTable + " WHERE id = :id";
    return jdbcClient
        .sql(sql)
        .param("id", id)
        .query(RowMapperUtil.dynamicRowMapper())
        .optional()
        .orElse(null);
  }

  public List<Map<String, Object>> loadFailureRowsByIds(String failureTable, List<Long> ids) {
    if (ids == null || ids.isEmpty()) {
      return List.of();
    }
    String sql = "SELECT * FROM " + failureTable + " WHERE id IN (:ids)";
    Map<String, Object> params = new HashMap<>();
    params.put("ids", ids);
    return jdbcClient.sql(sql).params(params).query(RowMapperUtil.dynamicRowMapper()).list();
  }

  public List<Map<String, Object>> loadFailureRowsForReplay(
      String failureTable,
      LocalDateTime startTimestamp,
      LocalDateTime endTimestamp,
      boolean endInclusive,
      String traceId,
      String messageKey,
      String accountNumber,
      Long latencyMin,
      Long latencyMax,
      Long receivedLatencyMin,
      Long receivedLatencyMax,
      String exceptionType,
      Boolean retriable,
      Integer retryAttemptMin,
      Integer retryAttemptMax) {
    StringBuilder sql = new StringBuilder("SELECT * FROM ").append(failureTable);
    Map<String, Object> params = new HashMap<>();
    sql.append(" WHERE event_datetime >= :startTs AND event_datetime ");
    sql.append(endInclusive ? "<= :endTs" : "< :endTs");
    params.put("startTs", startTimestamp);
    params.put("endTs", endTimestamp);

    if (traceId != null && !traceId.isBlank()) {
      sql.append(" AND event_trace_id = :traceId");
      params.put("traceId", traceId);
    }
    if (messageKey != null && !messageKey.isBlank()) {
      sql.append(" AND message_key = :messageKey");
      params.put("messageKey", messageKey);
    }
    if (accountNumber != null && !accountNumber.isBlank()) {
      sql.append(" AND account_number = :accountNumber");
      params.put("accountNumber", accountNumber);
    }
    if (latencyMin != null) {
      sql.append(" AND latency_ms >= :latencyMin");
      params.put("latencyMin", latencyMin);
    }
    if (latencyMax != null) {
      sql.append(" AND latency_ms <= :latencyMax");
      params.put("latencyMax", latencyMax);
    }
    if (receivedLatencyMin != null) {
      sql.append(" AND latency_event_received_ms >= :receivedLatencyMin");
      params.put("receivedLatencyMin", receivedLatencyMin);
    }
    if (receivedLatencyMax != null) {
      sql.append(" AND latency_event_received_ms <= :receivedLatencyMax");
      params.put("receivedLatencyMax", receivedLatencyMax);
    }
    if (exceptionType != null && !exceptionType.isBlank()) {
      sql.append(" AND exception_type = :exceptionType");
      params.put("exceptionType", exceptionType);
    }
    if (retriable != null) {
      sql.append(" AND retriable = :retriable");
      params.put("retriable", retriable ? 1 : 0);
    }
    if (retryAttemptMin != null) {
      sql.append(" AND retry_attempt >= :retryAttemptMin");
      params.put("retryAttemptMin", retryAttemptMin);
    }
    if (retryAttemptMax != null) {
      sql.append(" AND retry_attempt <= :retryAttemptMax");
      params.put("retryAttemptMax", retryAttemptMax);
    }

    sql.append(" ORDER BY event_datetime DESC, id DESC");
    return jdbcClient.sql(sql.toString()).params(params).query(RowMapperUtil.dynamicRowMapper()).list();
  }
}


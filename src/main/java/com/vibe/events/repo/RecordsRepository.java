package com.vibe.events.repo;

import com.vibe.events.util.RowMapperUtil;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class RecordsRepository {
  private final JdbcClient jdbcClient;

  public RecordsRepository(JdbcClient jdbcClient) {
    this.jdbcClient = jdbcClient;
  }

  public List<Map<String, Object>> loadSuccessRows(
      String successTable,
      LocalDate startDay,
      LocalDate endDay,
      LocalDateTime startTimestamp,
      LocalDateTime endTimestamp,
      String traceId,
      String messageKey,
      String accountNumber,
      int offset,
      int limit) {
    StringBuilder sql = new StringBuilder("SELECT * FROM ").append(successTable);
    Map<String, Object> params = new HashMap<>();
    sql.append(" WHERE event_date BETWEEN :startDay AND :endDay");
    params.put("startDay", startDay);
    params.put("endDay", endDay);
    sql.append(" AND event_received_timestamp BETWEEN :startTs AND :endTs");
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

    sql.append(" ORDER BY event_received_timestamp DESC, id DESC LIMIT :limit OFFSET :offset");
    params.put("limit", limit);
    params.put("offset", offset);

    return jdbcClient.sql(sql.toString()).params(params).query(RowMapperUtil.dynamicRowMapper()).list();
  }

  public long loadSuccessRowCount(
      String successTable,
      LocalDate startDay,
      LocalDate endDay,
      LocalDateTime startTimestamp,
      LocalDateTime endTimestamp,
      String traceId,
      String messageKey,
      String accountNumber) {
    StringBuilder sql = new StringBuilder("SELECT COUNT(*) AS total_count FROM ").append(successTable);
    Map<String, Object> params = new HashMap<>();
    sql.append(" WHERE event_date BETWEEN :startDay AND :endDay");
    params.put("startDay", startDay);
    params.put("endDay", endDay);
    sql.append(" AND event_received_timestamp BETWEEN :startTs AND :endTs");
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

    return jdbcClient
        .sql(sql.toString())
        .params(params)
        .query((rs, rowNum) -> rs.getLong("total_count"))
        .single();
  }

  public List<Map<String, Object>> loadFailureRows(
      String failureTable,
      LocalDate startDay,
      LocalDate endDay,
      LocalDateTime startTimestamp,
      LocalDateTime endTimestamp,
      String traceId,
      String messageKey,
      String accountNumber,
      String exceptionType,
      Boolean retriable,
      Integer retryAttemptMin,
      Integer retryAttemptMax,
      int offset,
      int limit) {
    StringBuilder sql = new StringBuilder("SELECT * FROM ").append(failureTable);
    Map<String, Object> params = new HashMap<>();
    sql.append(" WHERE event_date BETWEEN :startDay AND :endDay");
    params.put("startDay", startDay);
    params.put("endDay", endDay);
    sql.append(" AND event_received_timestamp BETWEEN :startTs AND :endTs");
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

    sql.append(" ORDER BY event_received_timestamp DESC, id DESC LIMIT :limit OFFSET :offset");
    params.put("limit", limit);
    params.put("offset", offset);

    return jdbcClient.sql(sql.toString()).params(params).query(RowMapperUtil.dynamicRowMapper()).list();
  }

  public long loadFailureRowCount(
      String failureTable,
      LocalDate startDay,
      LocalDate endDay,
      LocalDateTime startTimestamp,
      LocalDateTime endTimestamp,
      String traceId,
      String messageKey,
      String accountNumber,
      String exceptionType,
      Boolean retriable,
      Integer retryAttemptMin,
      Integer retryAttemptMax) {
    StringBuilder sql = new StringBuilder("SELECT COUNT(*) AS total_count FROM ").append(failureTable);
    Map<String, Object> params = new HashMap<>();
    sql.append(" WHERE event_date BETWEEN :startDay AND :endDay");
    params.put("startDay", startDay);
    params.put("endDay", endDay);
    sql.append(" AND event_received_timestamp BETWEEN :startTs AND :endTs");
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
      LocalDate startDay,
      LocalDate endDay,
      LocalDateTime startTimestamp,
      LocalDateTime endTimestamp) {
    StringBuilder sql = new StringBuilder("SELECT DISTINCT exception_type FROM ").append(failureTable);
    Map<String, Object> params = new HashMap<>();
    sql.append(" WHERE event_date BETWEEN :startDay AND :endDay");
    params.put("startDay", startDay);
    params.put("endDay", endDay);
    sql.append(" AND event_received_timestamp BETWEEN :startTs AND :endTs");
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
}

package com.vibe.events.repo;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class ReplayAuditRepository {
  private final JdbcClient jdbcClient;

  public ReplayAuditRepository(JdbcClient jdbcClient) {
    this.jdbcClient = jdbcClient;
  }

  public void insertJob(ReplayJobRecord record) {
    String sql =
        """
        INSERT INTO replay_jobs (
          id,
          event_key,
          day,
          selection_type,
          filters_json,
          snapshot_at,
          requested_by,
          reason,
          total_requested,
          status,
          created_at
        ) VALUES (
          :id,
          :eventKey,
          :day,
          :selectionType,
          :filtersJson,
          :snapshotAt,
          :requestedBy,
          :reason,
          :totalRequested,
          :status,
          :createdAt
        )
        """;
    Map<String, Object> params = new java.util.HashMap<>();
    params.put("id", record.id());
    params.put("eventKey", record.eventKey());
    params.put("day", record.day());
    params.put("selectionType", record.selectionType());
    params.put("filtersJson", record.filtersJson());
    params.put("snapshotAt", record.snapshotAt());
    params.put("requestedBy", record.requestedBy());
    params.put("reason", record.reason());
    params.put("totalRequested", record.totalRequested());
    params.put("status", record.status());
    params.put("createdAt", record.createdAt());
    jdbcClient.sql(sql).params(params).update();
  }

  public void insertItems(List<ReplayItemRecord> items) {
    if (items == null || items.isEmpty()) {
      return;
    }
    String sql =
        """
        INSERT INTO replay_items (
          job_id,
          record_id,
          event_key,
          status,
          attempt_count,
          last_attempt_at,
          last_error,
          emitted_id,
          updated_at,
          trace_id,
          message_key,
          account_number,
          exception_type,
          event_datetime,
          source_payload
        ) VALUES (
          :jobId,
          :recordId,
          :eventKey,
          :status,
          :attemptCount,
          :lastAttemptAt,
          :lastError,
          :emittedId,
          :updatedAt,
          :traceId,
          :messageKey,
          :accountNumber,
          :exceptionType,
          :eventDatetime,
          :sourcePayload
        )
        """;
    for (ReplayItemRecord item : items) {
      Map<String, Object> params = new java.util.HashMap<>();
      params.put("jobId", item.jobId());
      params.put("recordId", item.recordId());
      params.put("eventKey", item.eventKey());
      params.put("status", item.status());
      params.put("attemptCount", item.attemptCount());
      params.put("lastAttemptAt", item.lastAttemptAt());
      params.put("lastError", item.lastError());
      params.put("emittedId", item.emittedId());
      params.put("updatedAt", item.updatedAt());
      params.put("traceId", item.traceId());
      params.put("messageKey", item.messageKey());
      params.put("accountNumber", item.accountNumber());
      params.put("exceptionType", item.exceptionType());
      params.put("eventDatetime", item.eventDatetime());
      params.put("sourcePayload", item.sourcePayload());
      jdbcClient
          .sql(sql)
          .params(params)
          .update();
    }
  }

  public void updateItemStatus(ReplayItemUpdate update) {
    String sql =
        """
        UPDATE replay_items
        SET status = :status,
            attempt_count = :attemptCount,
            last_attempt_at = :lastAttemptAt,
            last_error = :lastError,
            emitted_id = :emittedId,
            updated_at = :updatedAt
        WHERE job_id = :jobId
          AND record_id = :recordId
        """;
    Map<String, Object> params = new java.util.HashMap<>();
    params.put("status", update.status());
    params.put("attemptCount", update.attemptCount());
    params.put("lastAttemptAt", update.lastAttemptAt());
    params.put("lastError", update.lastError());
    params.put("emittedId", update.emittedId());
    params.put("updatedAt", update.updatedAt());
    params.put("jobId", update.jobId());
    params.put("recordId", update.recordId());
    jdbcClient
        .sql(sql)
        .params(params)
        .update();
  }

  public void updateJobSummary(ReplayJobUpdate update) {
    String sql =
        """
        UPDATE replay_jobs
        SET total_requested = :totalRequested,
            status = :status,
            completed_at = :completedAt
        WHERE id = :id
        """;
    jdbcClient
        .sql(sql)
        .params(
            Map.of(
                "id", update.id(),
                "totalRequested", update.totalRequested(),
                "status", update.status(),
                "completedAt", update.completedAt()))
        .update();
  }

  public List<ReplayJobSummaryRow> loadReplayJobs(
      String eventKey,
      String itemStatus,
      String requestedBy,
      String search,
      int limit,
      int offset) {
    StringBuilder sql =
        new StringBuilder(
            """
        SELECT j.id,
               j.event_key,
               j.selection_type,
               j.total_requested,
               j.status,
               j.requested_by,
               j.reason,
               j.created_at,
               j.completed_at,
               SUM(CASE WHEN i.status = 'REPLAYED' THEN 1 ELSE 0 END) AS succeeded,
               SUM(CASE WHEN i.status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
               SUM(CASE WHEN i.status = 'QUEUED' THEN 1 ELSE 0 END) AS queued
        FROM replay_jobs j
        LEFT JOIN replay_items i ON i.job_id = j.id
        WHERE 1=1
        """);
    Map<String, Object> params = new java.util.HashMap<>();
    if (eventKey != null && !eventKey.isBlank()) {
      sql.append(" AND j.event_key = :eventKey");
      params.put("eventKey", eventKey);
    }
    if (itemStatus != null && !itemStatus.isBlank()) {
      sql.append(
          """
           AND EXISTS (
             SELECT 1
             FROM replay_items si
             WHERE si.job_id = j.id
               AND si.status = :itemStatus
           )
          """);
      params.put("itemStatus", itemStatus);
    }
    if (requestedBy != null && !requestedBy.isBlank()) {
      sql.append(" AND j.requested_by = :requestedBy");
      params.put("requestedBy", requestedBy);
    }
    if (search != null && !search.isBlank()) {
      sql.append(
          """
           AND (
             j.id LIKE :search
             OR EXISTS (
               SELECT 1
               FROM replay_items si
               WHERE si.job_id = j.id
                 AND (CAST(si.record_id AS CHAR) LIKE :search OR si.trace_id LIKE :search)
             )
           )
          """);
      params.put("search", "%" + search + "%");
    }
    sql.append(
        """
        GROUP BY j.id
        ORDER BY j.created_at DESC
        LIMIT :limit OFFSET :offset
        """);
    params.put("limit", limit);
    params.put("offset", offset);
    return jdbcClient
        .sql(sql.toString())
        .params(params)
        .query(
            (rs, rowNum) ->
                new ReplayJobSummaryRow(
                    rs.getString("id"),
                    rs.getString("event_key"),
                    rs.getString("selection_type"),
                    rs.getInt("total_requested"),
                    rs.getString("status"),
                    rs.getString("requested_by"),
                    rs.getString("reason"),
                    rs.getTimestamp("created_at").toLocalDateTime(),
                    rs.getTimestamp("completed_at") == null
                        ? null
                        : rs.getTimestamp("completed_at").toLocalDateTime(),
                    rs.getLong("succeeded"),
                    rs.getLong("failed"),
                    rs.getLong("queued")))
        .list();
  }

  public long loadReplayJobCount(
      String eventKey, String itemStatus, String requestedBy, String search) {
    StringBuilder sql =
        new StringBuilder(
            """
        SELECT COUNT(1)
        FROM replay_jobs j
        WHERE 1=1
        """);
    Map<String, Object> params = new java.util.HashMap<>();
    if (eventKey != null && !eventKey.isBlank()) {
      sql.append(" AND j.event_key = :eventKey");
      params.put("eventKey", eventKey);
    }
    if (itemStatus != null && !itemStatus.isBlank()) {
      sql.append(
          """
           AND EXISTS (
             SELECT 1
             FROM replay_items si
             WHERE si.job_id = j.id
               AND si.status = :itemStatus
           )
          """);
      params.put("itemStatus", itemStatus);
    }
    if (requestedBy != null && !requestedBy.isBlank()) {
      sql.append(" AND j.requested_by = :requestedBy");
      params.put("requestedBy", requestedBy);
    }
    if (search != null && !search.isBlank()) {
      sql.append(
          """
           AND (
             j.id LIKE :search
             OR EXISTS (
               SELECT 1
               FROM replay_items si
               WHERE si.job_id = j.id
                 AND (CAST(si.record_id AS CHAR) LIKE :search OR si.trace_id LIKE :search)
             )
           )
          """);
      params.put("search", "%" + search + "%");
    }
    return jdbcClient.sql(sql.toString()).params(params).query(Long.class).single();
  }

  public List<ReplayJobItemRow> loadReplayJobItems(String jobId) {
    String sql =
        """
        SELECT record_id,
               event_key,
               status,
               attempt_count,
               last_attempt_at,
               last_error,
               emitted_id,
               updated_at,
               trace_id,
               message_key,
               account_number,
               exception_type,
               event_datetime,
               source_payload
        FROM replay_items
        WHERE job_id = :jobId
        ORDER BY record_id
        """;
    return jdbcClient
        .sql(sql)
        .param("jobId", jobId)
        .query(
            (rs, rowNum) ->
                new ReplayJobItemRow(
                    rs.getLong("record_id"),
                    rs.getString("event_key"),
                    rs.getString("status"),
                    rs.getInt("attempt_count"),
                    rs.getTimestamp("last_attempt_at") == null
                        ? null
                        : rs.getTimestamp("last_attempt_at").toLocalDateTime(),
                    rs.getString("last_error"),
                    rs.getString("emitted_id"),
                    rs.getTimestamp("updated_at") == null
                        ? null
                        : rs.getTimestamp("updated_at").toLocalDateTime(),
                    rs.getString("trace_id"),
                    rs.getString("message_key"),
                    rs.getString("account_number"),
                    rs.getString("exception_type"),
                    rs.getTimestamp("event_datetime") == null
                        ? null
                        : rs.getTimestamp("event_datetime").toLocalDateTime(),
                    rs.getString("source_payload")))
        .list();
  }

  public List<ReplayAuditItemRow> loadReplayAuditItems(
      String eventKey,
      String status,
      String requestedBy,
      String search,
      int limit,
      int offset) {
    StringBuilder sql =
        new StringBuilder(
            """
        SELECT i.job_id AS replay_id,
               i.record_id,
               i.event_key,
               i.trace_id,
               i.status,
               COALESCE(i.last_attempt_at, i.updated_at, i.created_at, j.created_at) AS replayed_at,
               j.requested_by,
               j.reason,
               i.last_error,
               i.emitted_id,
               TIMESTAMPDIFF(
                   MICROSECOND,
                   i.created_at,
                   COALESCE(i.last_attempt_at, i.updated_at, i.created_at)
               ) / 1000 AS duration_ms
        FROM replay_items i
        JOIN replay_jobs j ON j.id = i.job_id
        WHERE 1=1
        """);
    Map<String, Object> params = new java.util.HashMap<>();
    appendAuditFilters(sql, params, eventKey, status, requestedBy, search);
    sql.append(
        """
        ORDER BY i.id DESC
        LIMIT :limit OFFSET :offset
        """);
    params.put("limit", limit);
    params.put("offset", offset);
    return jdbcClient
        .sql(sql.toString())
        .params(params)
        .query(
            (rs, rowNum) -> {
              java.sql.Timestamp replayedAtTs = rs.getTimestamp("replayed_at");
              Long durationMs =
                  rs.getObject("duration_ms") == null ? null : rs.getLong("duration_ms");
              return new ReplayAuditItemRow(
                  rs.getString("replay_id"),
                  rs.getLong("record_id"),
                  rs.getString("event_key"),
                  rs.getString("trace_id"),
                  rs.getString("status"),
                  replayedAtTs == null ? null : replayedAtTs.toLocalDateTime(),
                  durationMs,
                  rs.getString("requested_by"),
                  rs.getString("reason"),
                  rs.getString("last_error"),
                  rs.getString("emitted_id"));
            })
        .list();
  }

  public ReplayAuditStatsRow loadReplayAuditStats(
      String eventKey, String status, String requestedBy, String search) {
    StringBuilder sql =
        new StringBuilder(
            """
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN i.status = 'REPLAYED' THEN 1 ELSE 0 END) AS replayed,
               SUM(CASE WHEN i.status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
               SUM(CASE WHEN i.status = 'QUEUED' THEN 1 ELSE 0 END) AS queued,
               AVG(
                 TIMESTAMPDIFF(
                   MICROSECOND,
                   i.created_at,
                   COALESCE(i.last_attempt_at, i.updated_at, i.created_at)
                 ) / 1000
               ) AS avg_duration_ms,
               MAX(COALESCE(i.last_attempt_at, i.updated_at, i.created_at, j.created_at)) AS latest_at
        FROM replay_items i
        JOIN replay_jobs j ON j.id = i.job_id
        WHERE 1=1
        """);
    Map<String, Object> params = new java.util.HashMap<>();
    appendAuditFilters(sql, params, eventKey, status, requestedBy, search);
    return jdbcClient
        .sql(sql.toString())
        .params(params)
        .query(
            (rs, rowNum) -> {
              Double avgDuration =
                  rs.getObject("avg_duration_ms") == null ? null : rs.getDouble("avg_duration_ms");
              java.sql.Timestamp latestAt = rs.getTimestamp("latest_at");
              return new ReplayAuditStatsRow(
                  rs.getLong("total"),
                  rs.getLong("replayed"),
                  rs.getLong("failed"),
                  rs.getLong("queued"),
                  avgDuration,
                  latestAt == null ? null : latestAt.toLocalDateTime());
            })
        .single();
  }

  public List<String> loadReplayAuditOperators() {
    String sql =
        """
        SELECT DISTINCT requested_by
        FROM replay_jobs
        WHERE requested_by IS NOT NULL
          AND requested_by <> ''
        ORDER BY requested_by
        """;
    return jdbcClient
        .sql(sql)
        .query((rs, rowNum) -> rs.getString("requested_by"))
        .list();
  }

  public List<String> loadReplayAuditEventKeys() {
    String sql =
        """
        SELECT DISTINCT event_key
        FROM replay_items
        WHERE event_key IS NOT NULL
          AND event_key <> ''
        ORDER BY event_key
        """;
    return jdbcClient.sql(sql).query((rs, rowNum) -> rs.getString("event_key")).list();
  }

  private void appendAuditFilters(
      StringBuilder sql,
      Map<String, Object> params,
      String eventKey,
      String status,
      String requestedBy,
      String search) {
    if (eventKey != null && !eventKey.isBlank()) {
      sql.append(" AND i.event_key = :eventKey");
      params.put("eventKey", eventKey);
    }
    if (status != null && !status.isBlank()) {
      sql.append(" AND i.status = :status");
      params.put("status", status);
    }
    if (requestedBy != null && !requestedBy.isBlank()) {
      sql.append(" AND j.requested_by = :requestedBy");
      params.put("requestedBy", requestedBy);
    }
    if (search != null && !search.isBlank()) {
      sql.append(
          """
           AND (
             j.id LIKE :search
             OR CAST(i.record_id AS CHAR) LIKE :search
             OR i.trace_id LIKE :search
           )
          """);
      params.put("search", "%" + search + "%");
    }
  }

  public record ReplayJobRecord(
      String id,
      String eventKey,
      LocalDate day,
      String selectionType,
      String filtersJson,
      LocalDateTime snapshotAt,
      String requestedBy,
      String reason,
      int totalRequested,
      String status,
      LocalDateTime createdAt) {}

  public record ReplayItemRecord(
      String jobId,
      long recordId,
      String eventKey,
      String status,
      int attemptCount,
      LocalDateTime lastAttemptAt,
      String lastError,
      String emittedId,
      LocalDateTime updatedAt,
      String traceId,
      String messageKey,
      String accountNumber,
      String exceptionType,
      LocalDateTime eventDatetime,
      String sourcePayload) {}

  public record ReplayItemUpdate(
      String jobId,
      long recordId,
      String status,
      int attemptCount,
      LocalDateTime lastAttemptAt,
      String lastError,
      String emittedId,
      LocalDateTime updatedAt) {}

  public record ReplayJobUpdate(
      String id, int totalRequested, String status, LocalDateTime completedAt) {}

  public record ReplayJobSummaryRow(
      String id,
      String eventKey,
      String selectionType,
      int totalRequested,
      String status,
      String requestedBy,
      String reason,
      LocalDateTime createdAt,
      LocalDateTime completedAt,
      long succeeded,
      long failed,
      long queued) {}

  public record ReplayJobItemRow(
      long recordId,
      String eventKey,
      String status,
      int attemptCount,
      LocalDateTime lastAttemptAt,
      String lastError,
      String emittedId,
      LocalDateTime updatedAt,
      String traceId,
      String messageKey,
      String accountNumber,
      String exceptionType,
      LocalDateTime eventDatetime,
      String sourcePayload) {}

  public record ReplayAuditItemRow(
      String replayId,
      long recordId,
      String eventKey,
      String traceId,
      String status,
      LocalDateTime replayedAt,
      Long durationMs,
      String requestedBy,
      String reason,
      String lastError,
      String emittedId) {}

  public record ReplayAuditStatsRow(
      long total,
      long replayed,
      long failed,
      long queued,
      Double avgDurationMs,
      LocalDateTime latestAt) {}
}

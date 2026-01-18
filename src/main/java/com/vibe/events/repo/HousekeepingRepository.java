package com.vibe.events.repo;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class HousekeepingRepository {
  private final JdbcClient jdbcClient;

  public HousekeepingRepository(JdbcClient jdbcClient) {
    this.jdbcClient = jdbcClient;
  }

  public void insertRun(HousekeepingRunRecord record) {
    String sql =
        """
        INSERT INTO housekeeping_runs (
          id,
          job_type,
          event_key,
          trigger_type,
          run_date,
          attempt,
          status,
          cutoff_date,
          started_at,
          deleted_success,
          deleted_failure,
          deleted_total
        ) VALUES (
          :id,
          :jobType,
          :eventKey,
          :triggerType,
          :runDate,
          :attempt,
          :status,
          :cutoffDate,
          :startedAt,
          :deletedSuccess,
          :deletedFailure,
          :deletedTotal
        )
        """;
    Map<String, Object> params = new java.util.HashMap<>();
    params.put("id", record.id());
    params.put("jobType", record.jobType());
    params.put("eventKey", record.eventKey());
    params.put("triggerType", record.triggerType());
    params.put("runDate", record.runDate());
    params.put("attempt", record.attempt());
    params.put("status", record.status());
    params.put("cutoffDate", record.cutoffDate());
    params.put("startedAt", record.startedAt());
    params.put("deletedSuccess", record.deletedSuccess());
    params.put("deletedFailure", record.deletedFailure());
    params.put("deletedTotal", record.deletedTotal());
    jdbcClient.sql(sql).params(params).update();
  }

  public void upsertDailySnapshot(HousekeepingDailySnapshot snapshot) {
    String sql =
        """
        INSERT INTO housekeeping_daily (
          job_type,
          event_key,
          run_date,
          retention_days,
          cutoff_date,
          snapshot_at,
          eligible_success,
          eligible_failure,
          eligible_total,
          last_status,
          last_attempt
        ) VALUES (
          :jobType,
          :eventKey,
          :runDate,
          :retentionDays,
          :cutoffDate,
          :snapshotAt,
          :eligibleSuccess,
          :eligibleFailure,
          :eligibleTotal,
          :lastStatus,
          :lastAttempt
        )
        ON DUPLICATE KEY UPDATE
          retention_days = VALUES(retention_days),
          cutoff_date = VALUES(cutoff_date),
          snapshot_at = VALUES(snapshot_at),
          eligible_success = VALUES(eligible_success),
          eligible_failure = VALUES(eligible_failure),
          eligible_total = VALUES(eligible_total)
        """;
    Map<String, Object> params = new java.util.HashMap<>();
    params.put("jobType", snapshot.jobType());
    params.put("eventKey", snapshot.eventKey());
    params.put("runDate", snapshot.runDate());
    params.put("retentionDays", snapshot.retentionDays());
    params.put("cutoffDate", snapshot.cutoffDate());
    params.put("snapshotAt", snapshot.snapshotAt());
    params.put("eligibleSuccess", snapshot.eligibleSuccess());
    params.put("eligibleFailure", snapshot.eligibleFailure());
    params.put("eligibleTotal", snapshot.eligibleTotal());
    params.put("lastStatus", snapshot.lastStatus());
    params.put("lastAttempt", snapshot.lastAttempt());
    jdbcClient.sql(sql).params(params).update();
  }

  public HousekeepingDailyRow lockDaily(String jobType, String eventKey, LocalDate runDate) {
    String sql =
        """
        SELECT job_type,
               event_key,
               run_date,
               retention_days,
               cutoff_date,
               snapshot_at,
               eligible_success,
               eligible_failure,
               eligible_total,
               last_status,
               last_run_id,
               last_attempt,
               last_started_at,
               last_completed_at,
               last_error
        FROM housekeeping_daily
        WHERE job_type = :jobType
          AND event_key = :eventKey
          AND run_date = :runDate
        FOR UPDATE
        """;
    return jdbcClient
        .sql(sql)
        .params(Map.of("jobType", jobType, "eventKey", eventKey, "runDate", runDate))
        .query(
            (rs, rowNum) ->
                new HousekeepingDailyRow(
                    rs.getString("job_type"),
                    rs.getString("event_key"),
                    rs.getDate("run_date").toLocalDate(),
                    rs.getInt("retention_days"),
                    rs.getDate("cutoff_date").toLocalDate(),
                    rs.getTimestamp("snapshot_at").toLocalDateTime(),
                    rs.getLong("eligible_success"),
                    rs.getLong("eligible_failure"),
                    rs.getLong("eligible_total"),
                    rs.getString("last_status"),
                    rs.getString("last_run_id"),
                    rs.getInt("last_attempt"),
                    rs.getTimestamp("last_started_at") == null
                        ? null
                        : rs.getTimestamp("last_started_at").toLocalDateTime(),
                    rs.getTimestamp("last_completed_at") == null
                        ? null
                        : rs.getTimestamp("last_completed_at").toLocalDateTime(),
                    rs.getString("last_error")))
        .optional()
        .orElse(null);
  }

  public HousekeepingDailyRow loadDaily(String jobType, String eventKey, LocalDate runDate) {
    String sql =
        """
        SELECT job_type,
               event_key,
               run_date,
               retention_days,
               cutoff_date,
               snapshot_at,
               eligible_success,
               eligible_failure,
               eligible_total,
               last_status,
               last_run_id,
               last_attempt,
               last_started_at,
               last_completed_at,
               last_error
        FROM housekeeping_daily
        WHERE job_type = :jobType
          AND event_key = :eventKey
          AND run_date = :runDate
        """;
    return jdbcClient
        .sql(sql)
        .params(Map.of("jobType", jobType, "eventKey", eventKey, "runDate", runDate))
        .query(
            (rs, rowNum) ->
                new HousekeepingDailyRow(
                    rs.getString("job_type"),
                    rs.getString("event_key"),
                    rs.getDate("run_date").toLocalDate(),
                    rs.getInt("retention_days"),
                    rs.getDate("cutoff_date").toLocalDate(),
                    rs.getTimestamp("snapshot_at").toLocalDateTime(),
                    rs.getLong("eligible_success"),
                    rs.getLong("eligible_failure"),
                    rs.getLong("eligible_total"),
                    rs.getString("last_status"),
                    rs.getString("last_run_id"),
                    rs.getInt("last_attempt"),
                    rs.getTimestamp("last_started_at") == null
                        ? null
                        : rs.getTimestamp("last_started_at").toLocalDateTime(),
                    rs.getTimestamp("last_completed_at") == null
                        ? null
                        : rs.getTimestamp("last_completed_at").toLocalDateTime(),
                    rs.getString("last_error")))
        .optional()
        .orElse(null);
  }

  public void updateDailyStatus(HousekeepingDailyUpdate update) {
    String sql =
        """
        UPDATE housekeeping_daily
        SET last_status = :lastStatus,
            last_run_id = :lastRunId,
            last_attempt = :lastAttempt,
            last_started_at = :lastStartedAt,
            last_completed_at = :lastCompletedAt,
            last_error = :lastError
        WHERE job_type = :jobType
          AND event_key = :eventKey
          AND run_date = :runDate
        """;
    Map<String, Object> params = new java.util.HashMap<>();
    params.put("jobType", update.jobType());
    params.put("eventKey", update.eventKey());
    params.put("runDate", update.runDate());
    params.put("lastStatus", update.lastStatus());
    params.put("lastRunId", update.lastRunId());
    params.put("lastAttempt", update.lastAttempt());
    params.put("lastStartedAt", update.lastStartedAt());
    params.put("lastCompletedAt", update.lastCompletedAt());
    params.put("lastError", update.lastError());
    jdbcClient.sql(sql).params(params).update();
  }

  public void updateRunProgress(HousekeepingRunProgress progress) {
    String sql =
        """
        UPDATE housekeeping_runs
        SET status = :status,
            deleted_success = :deletedSuccess,
            deleted_failure = :deletedFailure,
            deleted_total = :deletedTotal
        WHERE id = :id
        """;
    jdbcClient
        .sql(sql)
        .params(
            Map.of(
                "id", progress.id(),
                "status", progress.status(),
                "deletedSuccess", progress.deletedSuccess(),
                "deletedFailure", progress.deletedFailure(),
                "deletedTotal", progress.deletedTotal()))
        .update();
  }

  public void updateRun(HousekeepingRunUpdate update) {
    String sql =
        """
        UPDATE housekeeping_runs
        SET status = :status,
            completed_at = :completedAt,
            duration_ms = :durationMs,
            deleted_success = :deletedSuccess,
            deleted_failure = :deletedFailure,
            deleted_total = :deletedTotal,
            error_message = :errorMessage
        WHERE id = :id
        """;
    Map<String, Object> params = new java.util.HashMap<>();
    params.put("id", update.id());
    params.put("status", update.status());
    params.put("completedAt", update.completedAt());
    params.put("durationMs", update.durationMs());
    params.put("deletedSuccess", update.deletedSuccess());
    params.put("deletedFailure", update.deletedFailure());
    params.put("deletedTotal", update.deletedTotal());
    params.put("errorMessage", update.errorMessage());
    jdbcClient.sql(sql).params(params).update();
  }

  public void insertRunItem(HousekeepingRunItemRecord record) {
    String sql =
        """
        INSERT INTO housekeeping_run_items (
          run_id,
          event_key,
          deleted_success,
          deleted_failure,
          deleted_total,
          created_at
        ) VALUES (
          :runId,
          :eventKey,
          :deletedSuccess,
          :deletedFailure,
          :deletedTotal,
          :createdAt
        )
        """;
    jdbcClient
        .sql(sql)
        .params(
            Map.of(
                "runId", record.runId(),
                "eventKey", record.eventKey(),
                "deletedSuccess", record.deletedSuccess(),
                "deletedFailure", record.deletedFailure(),
                "deletedTotal", record.deletedTotal(),
                "createdAt", record.createdAt()))
        .update();
  }

  public List<HousekeepingRunItemRow> loadRunItems(String runId) {
    String sql =
        """
        SELECT run_id,
               event_key,
               deleted_success,
               deleted_failure,
               deleted_total,
               created_at
        FROM housekeeping_run_items
        WHERE run_id = :runId
        ORDER BY event_key
        """;
    return jdbcClient
        .sql(sql)
        .param("runId", runId)
        .query(
            (rs, rowNum) ->
                new HousekeepingRunItemRow(
                    rs.getString("run_id"),
                    rs.getString("event_key"),
                    rs.getLong("deleted_success"),
                    rs.getLong("deleted_failure"),
                    rs.getLong("deleted_total"),
                    rs.getTimestamp("created_at").toLocalDateTime()))
        .list();
  }

  public List<HousekeepingRunRow> loadRuns(int limit) {
    String sql =
        """
        SELECT id,
               job_type,
               event_key,
               trigger_type,
               run_date,
               attempt,
               status,
               cutoff_date,
               started_at,
               completed_at,
               duration_ms,
               deleted_success,
               deleted_failure,
               deleted_total,
               error_message
        FROM housekeeping_runs
        ORDER BY started_at DESC, id DESC
        LIMIT :limit
        """;
    return jdbcClient
        .sql(sql)
        .param("limit", limit)
        .query(
            (rs, rowNum) ->
                new HousekeepingRunRow(
                    rs.getString("id"),
                    rs.getString("job_type"),
                    rs.getString("event_key"),
                    rs.getString("trigger_type"),
                    rs.getDate("run_date").toLocalDate(),
                    rs.getInt("attempt"),
                    rs.getString("status"),
                    rs.getDate("cutoff_date").toLocalDate(),
                    rs.getTimestamp("started_at").toLocalDateTime(),
                    rs.getTimestamp("completed_at") == null
                        ? null
                        : rs.getTimestamp("completed_at").toLocalDateTime(),
                    rs.getObject("duration_ms") == null ? null : rs.getLong("duration_ms"),
                    rs.getLong("deleted_success"),
                    rs.getLong("deleted_failure"),
                    rs.getLong("deleted_total"),
                    rs.getString("error_message")))
        .list();
  }

  public List<HousekeepingRunHistoryRow> loadRunHistory(String jobType, int limit) {
    String sql =
        """
        SELECT r.id,
               r.job_type,
               r.event_key,
               r.trigger_type,
               r.run_date,
               r.attempt,
               r.status,
               r.cutoff_date,
               r.started_at,
               r.completed_at,
               r.duration_ms,
               r.deleted_success,
               r.deleted_failure,
               r.deleted_total,
               r.error_message,
               COALESCE(items.event_count, 0) AS event_count,
               items.event_keys
        FROM housekeeping_runs r
        LEFT JOIN (
          SELECT run_id,
                 COUNT(*) AS event_count,
                 GROUP_CONCAT(event_key ORDER BY event_key SEPARATOR ', ') AS event_keys
          FROM housekeeping_run_items
          GROUP BY run_id
        ) items ON items.run_id = r.id
        WHERE r.job_type = :jobType
        ORDER BY r.started_at DESC, r.id DESC
        LIMIT :limit
        """;
    return jdbcClient
        .sql(sql)
        .params(Map.of("jobType", jobType, "limit", limit))
        .query(
            (rs, rowNum) ->
                new HousekeepingRunHistoryRow(
                    rs.getString("id"),
                    rs.getString("job_type"),
                    rs.getString("event_key"),
                    rs.getString("trigger_type"),
                    rs.getDate("run_date").toLocalDate(),
                    rs.getInt("attempt"),
                    rs.getString("status"),
                    rs.getDate("cutoff_date").toLocalDate(),
                    rs.getTimestamp("started_at").toLocalDateTime(),
                    rs.getTimestamp("completed_at") == null
                        ? null
                        : rs.getTimestamp("completed_at").toLocalDateTime(),
                    rs.getObject("duration_ms") == null ? null : rs.getLong("duration_ms"),
                    rs.getLong("deleted_success"),
                    rs.getLong("deleted_failure"),
                    rs.getLong("deleted_total"),
                    rs.getString("error_message"),
                    rs.getInt("event_count"),
                    rs.getString("event_keys")))
        .list();
  }

  public List<HousekeepingRunRow> loadRunsForDate(
      String jobType, String eventKey, LocalDate runDate) {
    String sql =
        """
        SELECT id,
               job_type,
               event_key,
               trigger_type,
               run_date,
               attempt,
               status,
               cutoff_date,
               started_at,
               completed_at,
               duration_ms,
               deleted_success,
               deleted_failure,
               deleted_total,
               error_message
        FROM housekeeping_runs
        WHERE job_type = :jobType
          AND event_key = :eventKey
          AND run_date = :runDate
        ORDER BY attempt ASC
        """;
    return jdbcClient
        .sql(sql)
        .params(Map.of("jobType", jobType, "eventKey", eventKey, "runDate", runDate))
        .query(
            (rs, rowNum) ->
                new HousekeepingRunRow(
                    rs.getString("id"),
                    rs.getString("job_type"),
                    rs.getString("event_key"),
                    rs.getString("trigger_type"),
                    rs.getDate("run_date").toLocalDate(),
                    rs.getInt("attempt"),
                    rs.getString("status"),
                    rs.getDate("cutoff_date").toLocalDate(),
                    rs.getTimestamp("started_at").toLocalDateTime(),
                    rs.getTimestamp("completed_at") == null
                        ? null
                        : rs.getTimestamp("completed_at").toLocalDateTime(),
                    rs.getObject("duration_ms") == null ? null : rs.getLong("duration_ms"),
                    rs.getLong("deleted_success"),
                    rs.getLong("deleted_failure"),
                    rs.getLong("deleted_total"),
                    rs.getString("error_message")))
        .list();
  }

  public List<HousekeepingRunRow> loadLatestRunsForDate(String jobType, LocalDate runDate) {
    String sql =
        """
        SELECT r.id,
               r.job_type,
               r.event_key,
               r.trigger_type,
               r.run_date,
               r.attempt,
               r.status,
               r.cutoff_date,
               r.started_at,
               r.completed_at,
               r.duration_ms,
               r.deleted_success,
               r.deleted_failure,
               r.deleted_total,
               r.error_message
        FROM housekeeping_runs r
        JOIN (
          SELECT event_key,
                 MAX(attempt) AS max_attempt
          FROM housekeeping_runs
          WHERE job_type = :jobType
            AND run_date = :runDate
          GROUP BY event_key
        ) latest
          ON latest.event_key = r.event_key
         AND latest.max_attempt = r.attempt
        WHERE r.job_type = :jobType
          AND r.run_date = :runDate
        ORDER BY r.event_key
        """;
    return jdbcClient
        .sql(sql)
        .params(Map.of("jobType", jobType, "runDate", runDate))
        .query(
            (rs, rowNum) ->
                new HousekeepingRunRow(
                    rs.getString("id"),
                    rs.getString("job_type"),
                    rs.getString("event_key"),
                    rs.getString("trigger_type"),
                    rs.getDate("run_date").toLocalDate(),
                    rs.getInt("attempt"),
                    rs.getString("status"),
                    rs.getDate("cutoff_date").toLocalDate(),
                    rs.getTimestamp("started_at").toLocalDateTime(),
                    rs.getTimestamp("completed_at") == null
                        ? null
                        : rs.getTimestamp("completed_at").toLocalDateTime(),
                    rs.getObject("duration_ms") == null ? null : rs.getLong("duration_ms"),
                    rs.getLong("deleted_success"),
                    rs.getLong("deleted_failure"),
                    rs.getLong("deleted_total"),
                    rs.getString("error_message")))
        .list();
  }

  public List<HousekeepingRunSummaryRow> loadRunSummary(
      String jobType, String eventKey, int limit, int offset) {
    String eventFilter =
        eventKey == null || eventKey.isBlank() || "ALL".equalsIgnoreCase(eventKey)
            ? ""
            : " AND i.event_key = :eventKey";
    String sql =
        """
        SELECT summary.run_date,
               summary.event_key,
               summary.attempts,
               summary.deleted_success,
               summary.deleted_failure,
               summary.deleted_total,
               summary.max_attempt AS latest_attempt,
               latest.status AS latest_status,
               latest.trigger_type AS latest_trigger_type,
               latest.completed_at AS latest_completed_at,
               latest.duration_ms AS latest_duration_ms,
               latest.error_message AS latest_error_message
        FROM (
          SELECT r.run_date,
                 i.event_key,
                 COUNT(*) AS attempts,
                 SUM(i.deleted_success) AS deleted_success,
                 SUM(i.deleted_failure) AS deleted_failure,
                 SUM(i.deleted_total) AS deleted_total,
                 MAX(r.attempt) AS max_attempt
          FROM housekeeping_runs r
          JOIN housekeeping_run_items i ON i.run_id = r.id
          WHERE r.job_type = :jobType"""
            + eventFilter
            + """
          GROUP BY r.run_date, i.event_key
        ) summary
        JOIN housekeeping_runs latest
          ON latest.job_type = :jobType
         AND latest.run_date = summary.run_date
         AND latest.attempt = summary.max_attempt
        JOIN housekeeping_run_items latest_item
          ON latest_item.run_id = latest.id
         AND latest_item.event_key = summary.event_key
        ORDER BY summary.run_date DESC, summary.event_key
        LIMIT :limit OFFSET :offset
        """;
    Map<String, Object> params = new java.util.HashMap<>();
    params.put("jobType", jobType);
    params.put("limit", limit);
    params.put("offset", offset);
    if (!eventFilter.isBlank()) {
      params.put("eventKey", eventKey);
    }
    return jdbcClient
        .sql(sql)
        .params(params)
        .query(
            (rs, rowNum) ->
                new HousekeepingRunSummaryRow(
                    jobType,
                    rs.getDate("run_date").toLocalDate(),
                    rs.getString("event_key"),
                    rs.getInt("attempts"),
                    rs.getLong("deleted_success"),
                    rs.getLong("deleted_failure"),
                    rs.getLong("deleted_total"),
                    rs.getInt("latest_attempt"),
                    rs.getString("latest_status"),
                    rs.getString("latest_trigger_type"),
                    rs.getTimestamp("latest_completed_at") == null
                        ? null
                        : rs.getTimestamp("latest_completed_at").toLocalDateTime(),
                    rs.getObject("latest_duration_ms") == null
                        ? null
                        : rs.getLong("latest_duration_ms"),
                    rs.getString("latest_error_message")))
        .list();
  }

  public List<HousekeepingDailyRow> loadDailySummary(String jobType, int limit) {
    String sql =
        """
        SELECT :jobType AS job_type,
               'ALL' AS event_key,
               run_date,
               MAX(retention_days) AS retention_days,
               MAX(cutoff_date) AS cutoff_date,
               MAX(snapshot_at) AS snapshot_at,
               SUM(eligible_success) AS eligible_success,
               SUM(eligible_failure) AS eligible_failure,
               SUM(eligible_total) AS eligible_total,
               CASE
                 WHEN SUM(CASE WHEN last_status = 'FAILED' THEN 1 ELSE 0 END) > 0 THEN 'FAILED'
                 WHEN SUM(CASE WHEN last_status = 'RUNNING' THEN 1 ELSE 0 END) > 0 THEN 'RUNNING'
                 WHEN SUM(CASE WHEN last_status = 'READY' THEN 1 ELSE 0 END) > 0 THEN 'READY'
                 ELSE 'COMPLETED'
               END AS last_status,
               NULL AS last_run_id,
               MAX(last_attempt) AS last_attempt,
               MAX(last_started_at) AS last_started_at,
               MAX(last_completed_at) AS last_completed_at,
               MAX(CASE WHEN last_status = 'FAILED' THEN last_error ELSE NULL END) AS last_error
        FROM housekeeping_daily
        WHERE job_type = :jobType
        GROUP BY run_date
        ORDER BY run_date DESC
        LIMIT :limit
        """;
    return jdbcClient
        .sql(sql)
        .params(Map.of("jobType", jobType, "limit", limit))
        .query(
            (rs, rowNum) ->
                new HousekeepingDailyRow(
                    rs.getString("job_type"),
                    rs.getString("event_key"),
                    rs.getDate("run_date").toLocalDate(),
                    rs.getInt("retention_days"),
                    rs.getDate("cutoff_date").toLocalDate(),
                    rs.getTimestamp("snapshot_at").toLocalDateTime(),
                    rs.getLong("eligible_success"),
                    rs.getLong("eligible_failure"),
                    rs.getLong("eligible_total"),
                    rs.getString("last_status"),
                    rs.getString("last_run_id"),
                    rs.getInt("last_attempt"),
                    rs.getTimestamp("last_started_at") == null
                        ? null
                        : rs.getTimestamp("last_started_at").toLocalDateTime(),
                    rs.getTimestamp("last_completed_at") == null
                        ? null
                        : rs.getTimestamp("last_completed_at").toLocalDateTime(),
                    rs.getString("last_error")))
        .list();
  }

  public List<HousekeepingDailyRow> loadDailyRows(String jobType, String eventKey, int limit) {
    String sql =
        """
        SELECT job_type,
               event_key,
               run_date,
               retention_days,
               cutoff_date,
               snapshot_at,
               eligible_success,
               eligible_failure,
               eligible_total,
               last_status,
               last_run_id,
               last_attempt,
               last_started_at,
               last_completed_at,
               last_error
        FROM housekeeping_daily
        WHERE job_type = :jobType
          AND event_key = :eventKey
        ORDER BY run_date DESC
        LIMIT :limit
        """;
    return jdbcClient
        .sql(sql)
        .params(Map.of("jobType", jobType, "eventKey", eventKey, "limit", limit))
        .query(
            (rs, rowNum) ->
                new HousekeepingDailyRow(
                    rs.getString("job_type"),
                    rs.getString("event_key"),
                    rs.getDate("run_date").toLocalDate(),
                    rs.getInt("retention_days"),
                    rs.getDate("cutoff_date").toLocalDate(),
                    rs.getTimestamp("snapshot_at").toLocalDateTime(),
                    rs.getLong("eligible_success"),
                    rs.getLong("eligible_failure"),
                    rs.getLong("eligible_total"),
                    rs.getString("last_status"),
                    rs.getString("last_run_id"),
                    rs.getInt("last_attempt"),
                    rs.getTimestamp("last_started_at") == null
                        ? null
                        : rs.getTimestamp("last_started_at").toLocalDateTime(),
                    rs.getTimestamp("last_completed_at") == null
                        ? null
                        : rs.getTimestamp("last_completed_at").toLocalDateTime(),
                    rs.getString("last_error")))
        .list();
  }

  public long countOldRows(String table, LocalDateTime cutoff) {
    String sql = "SELECT COUNT(1) FROM " + table + " WHERE event_datetime < :cutoff";
    Long count = jdbcClient.sql(sql).param("cutoff", cutoff).query(Long.class).single();
    return count == null ? 0 : count;
  }

  public int deleteOldRows(String table, LocalDateTime cutoff, int limit) {
    String sql =
        "DELETE FROM " + table + " WHERE event_datetime < :cutoff ORDER BY event_datetime LIMIT :limit";
    return jdbcClient
        .sql(sql)
        .params(Map.of("cutoff", cutoff, "limit", limit))
        .update();
  }

  public long countReplayJobs(LocalDate cutoffDate) {
    String sql = "SELECT COUNT(1) FROM replay_jobs WHERE day < :cutoffDate";
    Long count = jdbcClient.sql(sql).param("cutoffDate", cutoffDate).query(Long.class).single();
    return count == null ? 0 : count;
  }

  public long countReplayItems(LocalDate cutoffDate) {
    String sql =
        """
        SELECT COUNT(1)
        FROM replay_items i
        JOIN replay_jobs j ON j.id = i.job_id
        WHERE j.day < :cutoffDate
        """;
    Long count = jdbcClient.sql(sql).param("cutoffDate", cutoffDate).query(Long.class).single();
    return count == null ? 0 : count;
  }

  public List<String> loadReplayJobIds(LocalDate cutoffDate, int limit) {
    String sql =
        """
        SELECT id
        FROM replay_jobs
        WHERE day < :cutoffDate
        ORDER BY day
        LIMIT :limit
        """;
    return jdbcClient
        .sql(sql)
        .params(Map.of("cutoffDate", cutoffDate, "limit", limit))
        .query(String.class)
        .list();
  }

  public int deleteReplayItemsByJobIds(List<String> jobIds) {
    if (jobIds == null || jobIds.isEmpty()) {
      return 0;
    }
    String sql = "DELETE FROM replay_items WHERE job_id IN (:jobIds)";
    return jdbcClient.sql(sql).params(Map.of("jobIds", jobIds)).update();
  }

  public int deleteReplayJobsByIds(List<String> jobIds) {
    if (jobIds == null || jobIds.isEmpty()) {
      return 0;
    }
    String sql = "DELETE FROM replay_jobs WHERE id IN (:jobIds)";
    return jdbcClient.sql(sql).params(Map.of("jobIds", jobIds)).update();
  }

  public long countHousekeepingRuns(LocalDate cutoffDate) {
    String sql = "SELECT COUNT(1) FROM housekeeping_runs WHERE run_date < :cutoffDate";
    Long count = jdbcClient.sql(sql).param("cutoffDate", cutoffDate).query(Long.class).single();
    return count == null ? 0 : count;
  }

  public long countHousekeepingRunItems(LocalDate cutoffDate) {
    String sql =
        """
        SELECT COUNT(1)
        FROM housekeeping_run_items i
        JOIN housekeeping_runs r ON r.id = i.run_id
        WHERE r.run_date < :cutoffDate
        """;
    Long count = jdbcClient.sql(sql).param("cutoffDate", cutoffDate).query(Long.class).single();
    return count == null ? 0 : count;
  }

  public long countHousekeepingDaily(LocalDate cutoffDate) {
    String sql = "SELECT COUNT(1) FROM housekeeping_daily WHERE run_date < :cutoffDate";
    Long count = jdbcClient.sql(sql).param("cutoffDate", cutoffDate).query(Long.class).single();
    return count == null ? 0 : count;
  }

  public List<String> loadHousekeepingRunIds(LocalDate cutoffDate, int limit) {
    String sql =
        """
        SELECT id
        FROM housekeeping_runs
        WHERE run_date < :cutoffDate
        ORDER BY run_date, attempt
        LIMIT :limit
        """;
    return jdbcClient
        .sql(sql)
        .params(Map.of("cutoffDate", cutoffDate, "limit", limit))
        .query(String.class)
        .list();
  }

  public int deleteHousekeepingRunItemsByRunIds(List<String> runIds) {
    if (runIds == null || runIds.isEmpty()) {
      return 0;
    }
    String sql = "DELETE FROM housekeeping_run_items WHERE run_id IN (:runIds)";
    return jdbcClient.sql(sql).params(Map.of("runIds", runIds)).update();
  }

  public int deleteHousekeepingRunsByIds(List<String> runIds) {
    if (runIds == null || runIds.isEmpty()) {
      return 0;
    }
    String sql = "DELETE FROM housekeeping_runs WHERE id IN (:runIds)";
    return jdbcClient.sql(sql).params(Map.of("runIds", runIds)).update();
  }

  public int deleteHousekeepingDaily(LocalDate cutoffDate, int limit) {
    String sql =
        """
        DELETE FROM housekeeping_daily
        WHERE run_date < :cutoffDate
        ORDER BY run_date
        LIMIT :limit
        """;
    return jdbcClient
        .sql(sql)
        .params(Map.of("cutoffDate", cutoffDate, "limit", limit))
        .update();
  }

  public record HousekeepingRunRecord(
      String id,
      String jobType,
      String eventKey,
      String triggerType,
      LocalDate runDate,
      int attempt,
      String status,
      LocalDate cutoffDate,
      LocalDateTime startedAt,
      long deletedSuccess,
      long deletedFailure,
      long deletedTotal) {}

  public record HousekeepingRunProgress(
      String id,
      String status,
      long deletedSuccess,
      long deletedFailure,
      long deletedTotal) {}

  public record HousekeepingRunUpdate(
      String id,
      String status,
      LocalDateTime completedAt,
      Long durationMs,
      long deletedSuccess,
      long deletedFailure,
      long deletedTotal,
      String errorMessage) {}

  public record HousekeepingRunRow(
      String id,
      String jobType,
      String eventKey,
      String triggerType,
      LocalDate runDate,
      int attempt,
      String status,
      LocalDate cutoffDate,
      LocalDateTime startedAt,
      LocalDateTime completedAt,
      Long durationMs,
      long deletedSuccess,
      long deletedFailure,
      long deletedTotal,
      String errorMessage) {}

  public record HousekeepingRunHistoryRow(
      String id,
      String jobType,
      String eventKey,
      String triggerType,
      LocalDate runDate,
      int attempt,
      String status,
      LocalDate cutoffDate,
      LocalDateTime startedAt,
      LocalDateTime completedAt,
      Long durationMs,
      long deletedSuccess,
      long deletedFailure,
      long deletedTotal,
      String errorMessage,
      int eventCount,
      String eventKeys) {}

  public record HousekeepingRunSummaryRow(
      String jobType,
      LocalDate runDate,
      String eventKey,
      int attempts,
      long deletedSuccess,
      long deletedFailure,
      long deletedTotal,
      int latestAttempt,
      String latestStatus,
      String latestTriggerType,
      LocalDateTime latestCompletedAt,
      Long latestDurationMs,
      String latestErrorMessage) {}

  public record HousekeepingDailySnapshot(
      String jobType,
      String eventKey,
      LocalDate runDate,
      int retentionDays,
      LocalDate cutoffDate,
      LocalDateTime snapshotAt,
      long eligibleSuccess,
      long eligibleFailure,
      long eligibleTotal,
      String lastStatus,
      int lastAttempt) {}

  public record HousekeepingDailyRow(
      String jobType,
      String eventKey,
      LocalDate runDate,
      int retentionDays,
      LocalDate cutoffDate,
      LocalDateTime snapshotAt,
      long eligibleSuccess,
      long eligibleFailure,
      long eligibleTotal,
      String lastStatus,
      String lastRunId,
      int lastAttempt,
      LocalDateTime lastStartedAt,
      LocalDateTime lastCompletedAt,
      String lastError) {}

  public record HousekeepingDailyUpdate(
      String jobType,
      String eventKey,
      LocalDate runDate,
      String lastStatus,
      String lastRunId,
      int lastAttempt,
      LocalDateTime lastStartedAt,
      LocalDateTime lastCompletedAt,
      String lastError) {}

  public record HousekeepingRunItemRecord(
      String runId,
      String eventKey,
      long deletedSuccess,
      long deletedFailure,
      long deletedTotal,
      LocalDateTime createdAt) {}

  public record HousekeepingRunItemRow(
      String runId,
      String eventKey,
      long deletedSuccess,
      long deletedFailure,
      long deletedTotal,
      LocalDateTime createdAt) {}
}

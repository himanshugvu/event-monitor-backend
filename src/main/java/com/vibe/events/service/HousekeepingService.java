package com.vibe.events.service;

import com.vibe.events.config.HousekeepingProperties;
import com.vibe.events.dto.HousekeepingDailyResponse;
import com.vibe.events.dto.HousekeepingPreviewEvent;
import com.vibe.events.dto.HousekeepingPreviewResponse;
import com.vibe.events.dto.HousekeepingRunHistoryResponse;
import com.vibe.events.dto.HousekeepingRunItemResponse;
import com.vibe.events.dto.HousekeepingRunResponse;
import com.vibe.events.dto.HousekeepingRunSummaryResponse;
import com.vibe.events.dto.HousekeepingStatusResponse;
import com.vibe.events.registry.EventDefinition;
import com.vibe.events.registry.EventRegistry;
import com.vibe.events.repo.HousekeepingRepository;
import com.vibe.events.repo.HousekeepingRepository.HousekeepingDailyRow;
import com.vibe.events.repo.HousekeepingRepository.HousekeepingDailySnapshot;
import com.vibe.events.repo.HousekeepingRepository.HousekeepingDailyUpdate;
import com.vibe.events.repo.HousekeepingRepository.HousekeepingRunRecord;
import com.vibe.events.repo.HousekeepingRepository.HousekeepingRunItemRecord;
import com.vibe.events.repo.HousekeepingRepository.HousekeepingRunItemRow;
import com.vibe.events.repo.HousekeepingRepository.HousekeepingRunHistoryRow;
import com.vibe.events.repo.HousekeepingRepository.HousekeepingRunProgress;
import com.vibe.events.repo.HousekeepingRepository.HousekeepingRunRow;
import com.vibe.events.repo.HousekeepingRepository.HousekeepingRunSummaryRow;
import com.vibe.events.repo.HousekeepingRepository.HousekeepingRunUpdate;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class HousekeepingService {
  private static final Logger log = LoggerFactory.getLogger(HousekeepingService.class);
  private static final String JOB_TYPE_RETENTION = "RETENTION";
  private static final String JOB_TYPE_REPLAY_AUDIT = "REPLAY_AUDIT";
  private static final String JOB_TYPE_HOUSEKEEPING_AUDIT = "HOUSEKEEPING_AUDIT";
  private static final String EVENT_KEY_AUDIT = "__audit__";
  private static final String EVENT_KEY_ALL = "ALL";
  private static final String STATUS_RUNNING = "RUNNING";
  private static final String STATUS_COMPLETED = "COMPLETED";
  private static final String STATUS_FAILED = "FAILED";
  private static final String STATUS_SKIPPED = "SKIPPED";
  private static final String STATUS_READY = "READY";

  private final HousekeepingRepository repository;
  private final EventRegistry registry;
  private final HousekeepingProperties properties;
  private final TransactionTemplate transactionTemplate;

  public HousekeepingService(
      HousekeepingRepository repository,
      EventRegistry registry,
      HousekeepingProperties properties,
      PlatformTransactionManager transactionManager) {
    this.repository = repository;
    this.registry = registry;
    this.properties = properties;
    this.transactionTemplate = new TransactionTemplate(transactionManager);
  }

  public HousekeepingRunResponse runRetention(
      String eventKey, String triggerType, LocalDate dateOverride) {
    return runHousekeeping(JOB_TYPE_RETENTION, eventKey, triggerType, dateOverride);
  }

  public HousekeepingRunResponse runAudit(
      String jobType, String triggerType, LocalDate dateOverride) {
    return runHousekeeping(jobType, EVENT_KEY_AUDIT, triggerType, dateOverride);
  }

  public HousekeepingRunResponse runHousekeeping(
      String jobType, String eventKey, String triggerType, LocalDate dateOverride) {
    LocalDate runDate = dateOverride == null ? LocalDate.now() : dateOverride;
    HousekeepingSnapshot snapshot = refreshSnapshot(jobType, eventKey, runDate);
    RunDecision decision =
        transactionTemplate.execute(
            status -> beginRun(jobType, triggerType, snapshot));
    if (decision == null || !decision.shouldRun()) {
      return loadLatestRunResponse(
          jobType, decision == null ? null : decision.daily(), snapshot);
    }
    HousekeepingDailyRow daily = decision.daily();
    int attempt = decision.attempt();
    String id = decision.runId();
    LocalDateTime start = decision.startTime();
    LocalDateTime cutoffDateTime = snapshot.cutoffDate().atStartOfDay();

    long deletedSuccess = 0;
    long deletedFailure = 0;
    List<HousekeepingRunItemResponse> items = new ArrayList<>();
    try {
      if (JOB_TYPE_RETENTION.equals(jobType)) {
        EventDefinition definition = registry.getRequired(eventKey);
        long successDeleted =
            purgeTable(definition.getSuccessTable(), cutoffDateTime, properties.getBatchSize());
        long failureDeleted =
            purgeTable(definition.getFailureTable(), cutoffDateTime, properties.getBatchSize());
        long eventTotal = successDeleted + failureDeleted;
        deletedSuccess += successDeleted;
        deletedFailure += failureDeleted;
        items.add(
            new HousekeepingRunItemResponse(
                definition.getKey(), successDeleted, failureDeleted, eventTotal));
        repository.insertRunItem(
            new HousekeepingRunItemRecord(
                id,
                definition.getKey(),
                successDeleted,
                failureDeleted,
                eventTotal,
                LocalDateTime.now()));
        repository.updateRunProgress(
            new HousekeepingRunProgress(
                id, STATUS_RUNNING, deletedSuccess, deletedFailure, deletedSuccess + deletedFailure));
      } else if (JOB_TYPE_REPLAY_AUDIT.equals(jobType)) {
        ReplayAuditDeletion deletion = purgeReplayAudit(cutoffDateTime, properties.getBatchSize());
        deletedSuccess = deletion.jobsDeleted();
        deletedFailure = deletion.itemsDeleted();
        items.add(
            new HousekeepingRunItemResponse(
                "replay_jobs", deletion.jobsDeleted(), 0, deletion.jobsDeleted()));
        items.add(
            new HousekeepingRunItemResponse(
                "replay_items", deletion.itemsDeleted(), 0, deletion.itemsDeleted()));
        repository.insertRunItem(
            new HousekeepingRunItemRecord(
                id,
                "replay_jobs",
                deletion.jobsDeleted(),
                0,
                deletion.jobsDeleted(),
                LocalDateTime.now()));
        repository.insertRunItem(
            new HousekeepingRunItemRecord(
                id,
                "replay_items",
                deletion.itemsDeleted(),
                0,
                deletion.itemsDeleted(),
                LocalDateTime.now()));
        repository.updateRunProgress(
            new HousekeepingRunProgress(
                id, STATUS_RUNNING, deletedSuccess, deletedFailure, deletedSuccess + deletedFailure));
      } else if (JOB_TYPE_HOUSEKEEPING_AUDIT.equals(jobType)) {
        JobAuditDeletion deletion = purgeHousekeepingAudit(cutoffDateTime, properties.getBatchSize());
        deletedSuccess = deletion.runsDeleted();
        deletedFailure = deletion.itemsDeleted();
        items.add(
            new HousekeepingRunItemResponse(
                "housekeeping_runs", deletion.runsDeleted(), 0, deletion.runsDeleted()));
        items.add(
            new HousekeepingRunItemResponse(
                "housekeeping_run_items",
                deletion.itemsDeleted(),
                0,
                deletion.itemsDeleted()));
        items.add(
            new HousekeepingRunItemResponse(
                "housekeeping_daily", deletion.dailyDeleted(), 0, deletion.dailyDeleted()));
        repository.insertRunItem(
            new HousekeepingRunItemRecord(
                id,
                "housekeeping_runs",
                deletion.runsDeleted(),
                0,
                deletion.runsDeleted(),
                LocalDateTime.now()));
        repository.insertRunItem(
            new HousekeepingRunItemRecord(
                id,
                "housekeeping_run_items",
                deletion.itemsDeleted(),
                0,
                deletion.itemsDeleted(),
                LocalDateTime.now()));
        repository.insertRunItem(
            new HousekeepingRunItemRecord(
                id,
                "housekeeping_daily",
                deletion.dailyDeleted(),
                0,
                deletion.dailyDeleted(),
                LocalDateTime.now()));
        repository.updateRunProgress(
            new HousekeepingRunProgress(
                id,
                STATUS_RUNNING,
                deletedSuccess,
                deletedFailure,
                deletedSuccess + deletedFailure));
      }
      long deletedTotal = deletedSuccess + deletedFailure;
      LocalDateTime completedAt = LocalDateTime.now();
      long durationMs = Duration.between(start, completedAt).toMillis();
      repository.updateRun(
          new HousekeepingRunUpdate(
              id,
              STATUS_COMPLETED,
              completedAt,
              durationMs,
              deletedSuccess,
              deletedFailure,
              deletedTotal,
              null));
      repository.updateDailyStatus(
          new HousekeepingDailyUpdate(
              jobType,
              snapshot.eventKey(),
              runDate,
              STATUS_COMPLETED,
              id,
              attempt,
              start,
              completedAt,
              null));
      refreshSnapshotSafely(jobType, snapshot.eventKey(), runDate);
      return new HousekeepingRunResponse(
          id,
          jobType,
          snapshot.eventKey(),
          triggerType,
          runDate,
          attempt,
          STATUS_COMPLETED,
          snapshot.cutoffDate(),
          start,
          completedAt,
          durationMs,
          deletedSuccess,
          deletedFailure,
          deletedTotal,
          null,
          items);
    } catch (Exception ex) {
      LocalDateTime completedAt = LocalDateTime.now();
      long durationMs = Duration.between(start, completedAt).toMillis();
      long deletedTotal = deletedSuccess + deletedFailure;
      repository.updateRun(
          new HousekeepingRunUpdate(
              id,
              STATUS_FAILED,
              completedAt,
              durationMs,
              deletedSuccess,
              deletedFailure,
              deletedTotal,
              ex.getMessage()));
      repository.updateDailyStatus(
          new HousekeepingDailyUpdate(
              jobType,
              snapshot.eventKey(),
              runDate,
              STATUS_FAILED,
              id,
              attempt,
              start,
              completedAt,
              ex.getMessage()));
      refreshSnapshotSafely(jobType, snapshot.eventKey(), runDate);
      throw ex;
    }
  }

  public HousekeepingPreviewResponse preview(String jobType, String eventKey, boolean refresh) {
    LocalDate runDate = LocalDate.now();
    if (!refresh) {
      if (JOB_TYPE_RETENTION.equals(jobType) && isAllEvents(eventKey)) {
        HousekeepingPreviewResponse cached = previewAllRetentionCached(runDate);
        if (cached != null) {
          return cached;
        }
      } else {
        HousekeepingDailyRow daily = repository.loadDaily(jobType, eventKey, runDate);
        if (daily != null && daily.snapshotAt() != null) {
          return previewFromDaily(daily, resolveNextRunAt(jobType, eventKey));
        }
        HousekeepingDailyRow latest = repository.loadLatestDaily(jobType, eventKey);
        if (latest != null && latest.snapshotAt() != null) {
          return previewFromDaily(latest, resolveNextRunAt(jobType, eventKey));
        }
      }
    }
    if (JOB_TYPE_RETENTION.equals(jobType) && isAllEvents(eventKey)) {
      return previewAllRetention();
    }
    HousekeepingSnapshot snapshot = refreshSnapshot(jobType, eventKey, runDate);
    return new HousekeepingPreviewResponse(
        snapshot.cutoffDate(),
        snapshot.retentionDays(),
        snapshot.snapshotAt(),
        snapshot.eligibleSuccess(),
        snapshot.eligibleFailure(),
        snapshot.eligibleTotal(),
        resolveNextRunAt(jobType, eventKey),
        snapshot.events());
  }

  public void refreshPreviewCache() {
    LocalDate today = LocalDate.now();
    LocalDate tomorrow = today.plusDays(1);
    refreshPreviewCacheForDate(today);
    refreshPreviewCacheForDate(tomorrow);
  }

  private void refreshPreviewCacheForDate(LocalDate runDate) {
    for (EventDefinition definition : registry.all()) {
      refreshSnapshotSafely(JOB_TYPE_RETENTION, definition.getKey(), runDate);
    }
    refreshSnapshotSafely(JOB_TYPE_REPLAY_AUDIT, EVENT_KEY_AUDIT, runDate);
    refreshSnapshotSafely(JOB_TYPE_HOUSEKEEPING_AUDIT, EVENT_KEY_AUDIT, runDate);
  }

  public HousekeepingStatusResponse status(String jobType, String eventKey, LocalDate date) {
    LocalDate runDate = date == null ? LocalDate.now() : date;
    if (JOB_TYPE_RETENTION.equals(jobType) && isAllEvents(eventKey)) {
      return statusAllRetention(runDate);
    }
    HousekeepingDailyRow daily = repository.loadDaily(jobType, eventKey, runDate);
    if (daily == null || daily.lastRunId() == null) {
      return null;
    }
    List<HousekeepingRunRow> runs = repository.loadRunsForDate(jobType, eventKey, runDate);
    if (runs.isEmpty()) {
      return null;
    }
    HousekeepingRunRow run = runs.get(runs.size() - 1);
    List<HousekeepingRunItemResponse> items = loadRunItems(run.id());
    return new HousekeepingStatusResponse(
        run.id(),
        run.jobType(),
        run.eventKey(),
        run.triggerType(),
        run.runDate(),
        run.attempt(),
        run.status(),
        run.cutoffDate(),
        run.startedAt(),
        run.completedAt(),
        run.durationMs(),
        run.deletedSuccess(),
        run.deletedFailure(),
        run.deletedTotal(),
        run.errorMessage(),
        items);
  }

  public List<HousekeepingDailyResponse> listDaily(
      String jobType, String eventKey, int limit) {
    List<HousekeepingDailyRow> rows =
        JOB_TYPE_RETENTION.equals(jobType) && isAllEvents(eventKey)
            ? repository.loadDailySummary(jobType, limit)
            : repository.loadDailyRows(jobType, eventKey, limit);
    List<HousekeepingDailyResponse> responses = new ArrayList<>();
    for (HousekeepingDailyRow row : rows) {
      responses.add(
          new HousekeepingDailyResponse(
              row.jobType(),
              row.eventKey(),
              row.runDate(),
              row.retentionDays(),
              row.cutoffDate(),
              row.snapshotAt(),
              row.eligibleSuccess(),
              row.eligibleFailure(),
              row.eligibleTotal(),
              row.lastStatus(),
              row.lastRunId(),
              row.lastAttempt(),
              row.lastStartedAt(),
              row.lastCompletedAt(),
              row.lastError()));
    }
    return responses;
  }

  public List<HousekeepingRunHistoryResponse> listRuns(String jobType, int limit) {
    List<HousekeepingRunHistoryRow> rows = repository.loadRunHistory(jobType, limit);
    List<HousekeepingRunHistoryResponse> responses = new ArrayList<>();
    for (HousekeepingRunHistoryRow row : rows) {
      List<HousekeepingRunItemResponse> items = loadRunItems(row.id());
      responses.add(
          new HousekeepingRunHistoryResponse(
              row.id(),
              row.jobType(),
              row.eventKey(),
              row.triggerType(),
              row.runDate(),
              row.attempt(),
              row.status(),
              row.cutoffDate(),
              row.startedAt(),
              row.completedAt(),
              row.durationMs(),
              row.deletedSuccess(),
              row.deletedFailure(),
              row.deletedTotal(),
              row.errorMessage(),
              row.eventCount(),
              row.eventKeys(),
              items));
    }
    return responses;
  }

  public List<HousekeepingRunSummaryResponse> listRunSummary(
      String jobType, String eventKey, int limit, int offset) {
    String effectiveEventKey =
        JOB_TYPE_RETENTION.equals(jobType) && !isAllEvents(eventKey) ? eventKey : null;
    List<HousekeepingRunSummaryRow> rows =
        repository.loadRunSummary(jobType, effectiveEventKey, limit, offset);
    List<HousekeepingRunSummaryResponse> responses = new ArrayList<>();
    for (HousekeepingRunSummaryRow row : rows) {
      responses.add(
          new HousekeepingRunSummaryResponse(
              row.jobType(),
              row.runDate(),
              row.eventKey(),
              row.attempts(),
              row.deletedSuccess(),
              row.deletedFailure(),
              row.deletedTotal(),
              row.latestAttempt(),
              row.latestStatus(),
              row.latestTriggerType(),
              row.latestCompletedAt(),
              row.latestDurationMs(),
              row.latestErrorMessage()));
    }
    return responses;
  }

  public List<HousekeepingRunResponse> listRunsForDate(
      String jobType, String eventKey, LocalDate runDate) {
    List<HousekeepingRunRow> rows = repository.loadRunsForDate(jobType, eventKey, runDate);
    List<HousekeepingRunResponse> responses = new ArrayList<>();
    for (HousekeepingRunRow row : rows) {
      List<HousekeepingRunItemResponse> items = loadRunItems(row.id());
      responses.add(
          new HousekeepingRunResponse(
              row.id(),
              row.jobType(),
              row.eventKey(),
              row.triggerType(),
              row.runDate(),
              row.attempt(),
              row.status(),
              row.cutoffDate(),
              row.startedAt(),
              row.completedAt(),
              row.durationMs(),
              row.deletedSuccess(),
              row.deletedFailure(),
              row.deletedTotal(),
              row.errorMessage(),
              items));
    }
    return responses;
  }

  private long purgeTable(String table, LocalDateTime cutoff, int batchSize) {
    long total = 0;
    int deleted;
    do {
      deleted = repository.deleteOldRows(table, cutoff, batchSize);
      total += deleted;
      if (deleted > 0) {
        log.info("Housekeeping deleted {} rows from {}", deleted, table);
      }
    } while (deleted == batchSize);
    return total;
  }

  private ReplayAuditDeletion purgeReplayAudit(LocalDateTime cutoff, int batchSize) {
    LocalDate cutoffDate = cutoff.toLocalDate();
    long jobsDeleted = 0;
    long itemsDeleted = 0;
    while (true) {
      List<String> jobIds = repository.loadReplayJobIds(cutoffDate, batchSize);
      if (jobIds.isEmpty()) {
        break;
      }
      int items = repository.deleteReplayItemsByJobIds(jobIds);
      int jobs = repository.deleteReplayJobsByIds(jobIds);
      itemsDeleted += items;
      jobsDeleted += jobs;
      if (jobIds.size() < batchSize) {
        break;
      }
    }
    return new ReplayAuditDeletion(jobsDeleted, itemsDeleted);
  }

  private JobAuditDeletion purgeHousekeepingAudit(LocalDateTime cutoff, int batchSize) {
    LocalDate cutoffDate = cutoff.toLocalDate();
    long runsDeleted = 0;
    long itemsDeleted = 0;
    while (true) {
      List<String> runIds = repository.loadHousekeepingRunIds(cutoffDate, batchSize);
      if (runIds.isEmpty()) {
        break;
      }
      int items = repository.deleteHousekeepingRunItemsByRunIds(runIds);
      int runs = repository.deleteHousekeepingRunsByIds(runIds);
      itemsDeleted += items;
      runsDeleted += runs;
      if (runIds.size() < batchSize) {
        break;
      }
    }
    long dailyDeleted = 0;
    int deleted;
    do {
      deleted = repository.deleteHousekeepingDaily(cutoffDate, batchSize);
      dailyDeleted += deleted;
    } while (deleted == batchSize);
    return new JobAuditDeletion(runsDeleted, itemsDeleted, dailyDeleted);
  }

  private HousekeepingPreviewResponse previewAllRetention() {
    LocalDateTime snapshotAt = LocalDateTime.now();
    int retentionDays = properties.getRetentionDays();
    LocalDate cutoffDate = LocalDate.now().minusDays(retentionDays);
    LocalDateTime cutoffDateTime = cutoffDate.atStartOfDay();
    ZonedDateTime now = ZonedDateTime.now(ZoneId.systemDefault());
    List<HousekeepingPreviewEvent> events = new ArrayList<>();
    long totalSuccess = 0;
    long totalFailure = 0;
    for (EventDefinition definition : registry.all()) {
      long success = repository.countOldRows(definition.getSuccessTable(), cutoffDateTime);
      long failure = repository.countOldRows(definition.getFailureTable(), cutoffDateTime);
      long total = success + failure;
      LocalDateTime nextRunAt =
          nextRunAt(definition.getRetentionCron(), properties.getCron(), now);
      events.add(new HousekeepingPreviewEvent(definition.getKey(), success, failure, total, nextRunAt));
      totalSuccess += success;
      totalFailure += failure;
    }
    LocalDateTime nextRunAt = resolveNextRunAt(JOB_TYPE_RETENTION, EVENT_KEY_ALL);
    return new HousekeepingPreviewResponse(
        cutoffDate,
        retentionDays,
        snapshotAt,
        totalSuccess,
        totalFailure,
        totalSuccess + totalFailure,
        nextRunAt,
        events);
  }

  private HousekeepingPreviewResponse previewAllRetentionCached(LocalDate runDate) {
    List<HousekeepingDailyRow> rows = repository.loadDailyRowsForDate(JOB_TYPE_RETENTION, runDate);
    if (rows.isEmpty()) {
      return null;
    }
    java.util.Map<String, HousekeepingDailyRow> byEvent = new java.util.HashMap<>();
    for (HousekeepingDailyRow row : rows) {
      byEvent.put(row.eventKey(), row);
    }
    LocalDateTime snapshotAt = null;
    int retentionDays = properties.getRetentionDays();
    LocalDate cutoffDate = runDate.minusDays(retentionDays);
    List<HousekeepingPreviewEvent> events = new ArrayList<>();
    long totalSuccess = 0;
    long totalFailure = 0;
    ZonedDateTime now = ZonedDateTime.now(ZoneId.systemDefault());
    for (EventDefinition definition : registry.all()) {
      HousekeepingDailyRow row = byEvent.get(definition.getKey());
      if (row == null) {
        row = repository.loadLatestDaily(JOB_TYPE_RETENTION, definition.getKey());
      }
      if (row == null || row.snapshotAt() == null) {
        return null;
      }
      if (snapshotAt == null || row.snapshotAt().isAfter(snapshotAt)) {
        snapshotAt = row.snapshotAt();
      }
      long success = row.eligibleSuccess();
      long failure = row.eligibleFailure();
      long total = row.eligibleTotal();
      LocalDateTime nextRunAt = nextRunAt(definition.getRetentionCron(), properties.getCron(), now);
      events.add(new HousekeepingPreviewEvent(definition.getKey(), success, failure, total, nextRunAt));
      totalSuccess += success;
      totalFailure += failure;
    }
    LocalDateTime nextRunAt = resolveNextRunAt(JOB_TYPE_RETENTION, EVENT_KEY_ALL);
    return new HousekeepingPreviewResponse(
        cutoffDate,
        retentionDays,
        snapshotAt,
        totalSuccess,
        totalFailure,
        totalSuccess + totalFailure,
        nextRunAt,
        events);
  }

  private HousekeepingPreviewResponse previewFromDaily(
      HousekeepingDailyRow daily, LocalDateTime nextRunAt) {
    List<HousekeepingPreviewEvent> events = new ArrayList<>();
    events.add(
        new HousekeepingPreviewEvent(
            daily.eventKey(),
            daily.eligibleSuccess(),
            daily.eligibleFailure(),
            daily.eligibleTotal(),
            nextRunAt));
    return new HousekeepingPreviewResponse(
        daily.cutoffDate(),
        daily.retentionDays(),
        daily.snapshotAt(),
        daily.eligibleSuccess(),
        daily.eligibleFailure(),
        daily.eligibleTotal(),
        nextRunAt,
        events);
  }

  private void refreshSnapshotSafely(String jobType, String eventKey, LocalDate runDate) {
    try {
      refreshSnapshot(jobType, eventKey, runDate);
    } catch (Exception ex) {
      log.warn("Failed to refresh snapshot for {} {} {}", jobType, eventKey, runDate, ex);
    }
  }

  private HousekeepingStatusResponse statusAllRetention(LocalDate runDate) {
    List<HousekeepingRunRow> runs = repository.loadLatestRunsForDate(JOB_TYPE_RETENTION, runDate);
    if (runs.isEmpty()) {
      return null;
    }
    long deletedSuccess = 0;
    long deletedFailure = 0;
    LocalDateTime startedAt = null;
    LocalDateTime completedAt = null;
    String status = STATUS_COMPLETED;
    String triggerType = runs.get(0).triggerType();
    int attempt = 0;
    List<HousekeepingRunItemResponse> items = new ArrayList<>();
    for (HousekeepingRunRow run : runs) {
      deletedSuccess += run.deletedSuccess();
      deletedFailure += run.deletedFailure();
      attempt = Math.max(attempt, run.attempt());
      if (startedAt == null || run.startedAt().isBefore(startedAt)) {
        startedAt = run.startedAt();
      }
      if (run.completedAt() == null) {
        completedAt = null;
      } else if (completedAt == null || run.completedAt().isAfter(completedAt)) {
        completedAt = run.completedAt();
      }
      if (!triggerType.equals(run.triggerType())) {
        triggerType = "MIXED";
      }
      if (STATUS_FAILED.equals(run.status())) {
        status = STATUS_FAILED;
      } else if (STATUS_RUNNING.equals(run.status()) && !STATUS_FAILED.equals(status)) {
        status = STATUS_RUNNING;
      }
      items.add(
          new HousekeepingRunItemResponse(
              run.eventKey(), run.deletedSuccess(), run.deletedFailure(), run.deletedTotal()));
    }
    long deletedTotal = deletedSuccess + deletedFailure;
    Long durationMs =
        startedAt == null || completedAt == null
            ? null
            : Duration.between(startedAt, completedAt).toMillis();
    return new HousekeepingStatusResponse(
        "ALL-" + runDate,
        JOB_TYPE_RETENTION,
        EVENT_KEY_ALL,
        triggerType,
        runDate,
        attempt,
        status,
        runs.get(0).cutoffDate(),
        startedAt,
        completedAt,
        durationMs,
        deletedSuccess,
        deletedFailure,
        deletedTotal,
        null,
        items);
  }

  private HousekeepingSnapshot refreshSnapshot(
      String jobType, String eventKey, LocalDate runDate) {
    LocalDateTime snapshotAt = LocalDateTime.now();
    int retentionDays = properties.getRetentionDays();
    if (JOB_TYPE_RETENTION.equals(jobType)) {
      EventDefinition definition = registry.getRequired(eventKey);
      if (definition.getRetentionDays() != null) {
        retentionDays = definition.getRetentionDays();
      }
    }
    LocalDate cutoffDate = runDate.minusDays(retentionDays);
    LocalDateTime cutoffDateTime = cutoffDate.atStartOfDay();
    List<HousekeepingPreviewEvent> events = new ArrayList<>();
    long totalSuccess = 0;
    long totalFailure = 0;
    LocalDateTime nextRunAt = resolveNextRunAt(jobType, eventKey);
    if (JOB_TYPE_RETENTION.equals(jobType)) {
      EventDefinition definition = registry.getRequired(eventKey);
      long success = repository.countOldRows(definition.getSuccessTable(), cutoffDateTime);
      long failure = repository.countOldRows(definition.getFailureTable(), cutoffDateTime);
      long total = success + failure;
      events.add(new HousekeepingPreviewEvent(definition.getKey(), success, failure, total, nextRunAt));
      totalSuccess = success;
      totalFailure = failure;
    } else if (JOB_TYPE_REPLAY_AUDIT.equals(jobType)) {
      long jobs = repository.countReplayJobs(cutoffDate);
      long items = repository.countReplayItems(cutoffDate);
      events.add(new HousekeepingPreviewEvent("replay_jobs", jobs, 0, jobs, nextRunAt));
      events.add(new HousekeepingPreviewEvent("replay_items", items, 0, items, nextRunAt));
      totalSuccess = jobs;
      totalFailure = items;
    } else if (JOB_TYPE_HOUSEKEEPING_AUDIT.equals(jobType)) {
      long runs = repository.countHousekeepingRuns(cutoffDate);
      long items = repository.countHousekeepingRunItems(cutoffDate);
      long daily = repository.countHousekeepingDaily(cutoffDate);
      events.add(new HousekeepingPreviewEvent("housekeeping_runs", runs, 0, runs, nextRunAt));
      events.add(new HousekeepingPreviewEvent("housekeeping_run_items", items, 0, items, nextRunAt));
      events.add(new HousekeepingPreviewEvent("housekeeping_daily", daily, 0, daily, nextRunAt));
      totalSuccess = runs + daily;
      totalFailure = items;
    }
    repository.upsertDailySnapshot(
        new HousekeepingDailySnapshot(
            jobType,
            eventKey,
            runDate,
            retentionDays,
            cutoffDate,
            snapshotAt,
            totalSuccess,
            totalFailure,
            totalSuccess + totalFailure,
            STATUS_READY,
            0));
    return new HousekeepingSnapshot(
        jobType,
        eventKey,
        runDate,
        retentionDays,
        cutoffDate,
        snapshotAt,
        totalSuccess,
        totalFailure,
        totalSuccess + totalFailure,
        events);
  }

  private boolean isAllEvents(String eventKey) {
    return eventKey == null || eventKey.isBlank() || EVENT_KEY_ALL.equalsIgnoreCase(eventKey);
  }

  private LocalDateTime resolveNextRunAt(String jobType, String eventKey) {
    ZonedDateTime now = ZonedDateTime.now(ZoneId.systemDefault());
    if (JOB_TYPE_RETENTION.equals(jobType)) {
      if (isAllEvents(eventKey)) {
        LocalDateTime earliest = null;
        for (EventDefinition definition : registry.all()) {
          LocalDateTime next = nextRunAt(definition.getRetentionCron(), properties.getCron(), now);
          if (next == null) {
            continue;
          }
          if (earliest == null || next.isBefore(earliest)) {
            earliest = next;
          }
        }
        if (earliest != null) {
          return earliest;
        }
        return nextRunAt(null, properties.getCron(), now);
      }
      EventDefinition definition = registry.getRequired(eventKey);
      return nextRunAt(definition.getRetentionCron(), properties.getCron(), now);
    }
    if (JOB_TYPE_REPLAY_AUDIT.equals(jobType)) {
      return nextRunAt(null, properties.getReplayAuditCron(), now);
    }
    if (JOB_TYPE_HOUSEKEEPING_AUDIT.equals(jobType)) {
      return nextRunAt(null, properties.getHousekeepingAuditCron(), now);
    }
    return null;
  }

  private LocalDateTime nextRunAt(String preferredCron, String fallbackCron, ZonedDateTime now) {
    String cron = preferredCron == null || preferredCron.isBlank() ? fallbackCron : preferredCron;
    if (cron == null || cron.isBlank()) {
      return null;
    }
    CronExpression expression = CronExpression.parse(cron);
    ZonedDateTime next = expression.next(now);
    return next == null ? null : next.toLocalDateTime();
  }

  private HousekeepingRunResponse loadLatestRunResponse(
      String jobType, HousekeepingDailyRow daily, HousekeepingSnapshot snapshot) {
    if (daily == null) {
      return buildSkippedResponse(jobType, null, snapshot);
    }
    if (daily.lastRunId() == null) {
      return buildSkippedResponse(jobType, daily, snapshot);
    }
    List<HousekeepingRunRow> runs =
        repository.loadRunsForDate(jobType, daily.eventKey(), daily.runDate());
    if (runs.isEmpty()) {
      return buildSkippedResponse(jobType, daily, snapshot);
    }
    HousekeepingRunRow latest = runs.get(runs.size() - 1);
    List<HousekeepingRunItemResponse> items = loadRunItems(latest.id());
    return new HousekeepingRunResponse(
        latest.id(),
        latest.jobType(),
        latest.eventKey(),
        latest.triggerType(),
        latest.runDate(),
        latest.attempt(),
        latest.status(),
        latest.cutoffDate(),
        latest.startedAt(),
        latest.completedAt(),
        latest.durationMs(),
        latest.deletedSuccess(),
        latest.deletedFailure(),
        latest.deletedTotal(),
        latest.errorMessage(),
        items);
  }

  private HousekeepingRunResponse buildSkippedResponse(
      String jobType, HousekeepingDailyRow daily, HousekeepingSnapshot snapshot) {
    return new HousekeepingRunResponse(
        daily == null || daily.lastRunId() == null ? "SKIPPED" : daily.lastRunId(),
        jobType,
        daily == null ? snapshot.eventKey() : daily.eventKey(),
        "SYSTEM",
        snapshot.runDate(),
        daily == null ? 0 : daily.lastAttempt(),
        STATUS_SKIPPED,
        snapshot.cutoffDate(),
        snapshot.snapshotAt(),
        snapshot.snapshotAt(),
        0L,
        0,
        0,
        0,
        daily == null ? null : daily.lastError(),
        List.of());
  }

  private List<HousekeepingRunItemResponse> loadRunItems(String runId) {
    List<HousekeepingRunItemRow> rows = repository.loadRunItems(runId);
    List<HousekeepingRunItemResponse> items = new ArrayList<>();
    for (HousekeepingRunItemRow row : rows) {
      items.add(
          new HousekeepingRunItemResponse(
              row.eventKey(), row.deletedSuccess(), row.deletedFailure(), row.deletedTotal()));
    }
    return items;
  }

  private RunDecision beginRun(String jobType, String triggerType, HousekeepingSnapshot snapshot) {
    LocalDate runDate = snapshot.runDate();
    HousekeepingDailyRow daily = repository.lockDaily(jobType, snapshot.eventKey(), runDate);
    if (daily == null) {
      daily = repository.loadDaily(jobType, snapshot.eventKey(), runDate);
    }
    if (daily == null) {
      daily =
          new HousekeepingDailyRow(
              jobType,
              snapshot.eventKey(),
              runDate,
              snapshot.retentionDays(),
              snapshot.cutoffDate(),
              snapshot.snapshotAt(),
              snapshot.eligibleSuccess(),
              snapshot.eligibleFailure(),
              snapshot.eligibleTotal(),
              STATUS_READY,
              null,
              0,
              null,
              null,
              null);
    }

    boolean hasEligible = daily.eligibleTotal() > 0;
    if (STATUS_RUNNING.equals(daily.lastStatus())) {
      return RunDecision.skip(daily);
    }
    boolean allowRun =
        STATUS_FAILED.equals(daily.lastStatus()) || (hasEligible && !STATUS_RUNNING.equals(daily.lastStatus()));
    if (!allowRun) {
      return RunDecision.skip(daily);
    }

    int attempt = Math.max(daily.lastAttempt(), 0) + 1;
    String id = UUID.randomUUID().toString();
    LocalDateTime start = LocalDateTime.now();
    repository.insertRun(
        new HousekeepingRunRecord(
            id,
            jobType,
            snapshot.eventKey(),
            triggerType,
            runDate,
            attempt,
            STATUS_RUNNING,
            snapshot.cutoffDate(),
            start,
            0,
            0,
            0));
    repository.updateDailyStatus(
        new HousekeepingDailyUpdate(
            jobType, snapshot.eventKey(), runDate, STATUS_RUNNING, id, attempt, start, null, null));
    return RunDecision.start(daily, id, attempt, start);
  }

  private record HousekeepingSnapshot(
      String jobType,
      String eventKey,
      LocalDate runDate,
      int retentionDays,
      LocalDate cutoffDate,
      LocalDateTime snapshotAt,
      long eligibleSuccess,
      long eligibleFailure,
      long eligibleTotal,
      List<HousekeepingPreviewEvent> events) {}

  private record ReplayAuditDeletion(long jobsDeleted, long itemsDeleted) {}

  private record JobAuditDeletion(long runsDeleted, long itemsDeleted, long dailyDeleted) {}

  private record RunDecision(
      boolean shouldRun,
      HousekeepingDailyRow daily,
      String runId,
      int attempt,
      LocalDateTime startTime) {
    static RunDecision skip(HousekeepingDailyRow daily) {
      return new RunDecision(false, daily, null, 0, null);
    }

    static RunDecision start(
        HousekeepingDailyRow daily, String runId, int attempt, LocalDateTime startTime) {
      return new RunDecision(true, daily, runId, attempt, startTime);
    }
  }
}

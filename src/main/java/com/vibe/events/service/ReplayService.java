package com.vibe.events.service;

import com.vibe.events.dto.ReplayAuditResponse;
import com.vibe.events.dto.ReplayExternalRequest;
import com.vibe.events.dto.ReplayExternalResponse;
import com.vibe.events.dto.ReplayExternalResult;
import com.vibe.events.dto.ReplayFilterRequest;
import com.vibe.events.dto.ReplayFilters;
import com.vibe.events.dto.ReplayJobItemsResponse;
import com.vibe.events.dto.ReplayJobListResponse;
import com.vibe.events.dto.ReplayItemUpdateRequest;
import com.vibe.events.dto.ReplayItemUpdateRow;
import com.vibe.events.dto.ReplayJobResponse;
import com.vibe.events.dto.ReplayRequest;
import com.vibe.events.dto.ReplayResponse;
import com.vibe.events.error.BadRequestException;
import com.vibe.events.registry.EventRegistry;
import com.vibe.events.repo.ReplayAuditRepository;
import com.vibe.events.repo.ReplayAuditRepository.ReplayItemRecord;
import com.vibe.events.repo.ReplayAuditRepository.ReplayItemUpdate;
import com.vibe.events.repo.ReplayAuditRepository.ReplayJobRecord;
import com.vibe.events.repo.ReplayAuditRepository.ReplayJobUpdate;
import com.vibe.events.repo.RecordsRepository;
import com.vibe.events.util.DayValidator;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ReplayService {
  private static final Logger log = LoggerFactory.getLogger(ReplayService.class);
  private static final int MAX_IDS = 50;
  private static final String REQUESTED_BY = "event-monitor-service";
  private static final String REPLAY_REASON = "manual-replay";
  private static final String JOB_STATUS_RUNNING = "RUNNING";
  private static final String JOB_STATUS_COMPLETED = "COMPLETED";
  private static final String JOB_STATUS_FAILED = "FAILED";
  private static final String JOB_STATUS_PARTIAL = "PARTIAL";
  private static final String ITEM_STATUS_QUEUED = "QUEUED";
  private static final String ITEM_STATUS_REPLAYED = "REPLAYED";
  private static final String ITEM_STATUS_FAILED = "FAILED";
  private static final String ITEM_STATUS_NOT_FOUND = "NOT_FOUND";
  private static final int DEFAULT_SIZE = 50;
  private static final int MAX_SIZE = 200;

  private final EventRegistry registry;
  private final RecordsRepository repository;
  private final ReplayExternalClient externalClient;
  private final ReplayAuditRepository auditRepository;

  public ReplayService(
      EventRegistry registry,
      RecordsRepository repository,
      ReplayExternalClient externalClient,
      ReplayAuditRepository auditRepository) {
    this.registry = registry;
    this.repository = repository;
    this.externalClient = externalClient;
    this.auditRepository = auditRepository;
  }

  public ReplayResponse replay(ReplayRequest request) {
    if (request == null) {
      throw new BadRequestException("Replay request is required.");
    }
    if (request.mode() == null || request.mode().isBlank()) {
      throw new BadRequestException("Replay mode is required.");
    }
    if (request.eventKey() == null || request.eventKey().isBlank()) {
      throw new BadRequestException("eventKey is required.");
    }
    LocalDate day = DayValidator.parseDay(request.day());
    String eventKey = request.eventKey();
    String replayUrl = registry.replayUrl(eventKey);

    String mode = request.mode().trim().toUpperCase();
    if ("ID".equals(mode)) {
      if (request.id() == null) {
        throw new BadRequestException("id is required for mode=ID.");
      }
      return replayIds(eventKey, replayUrl, day, List.of(request.id()), "ids", null);
    }
    if ("IDS".equals(mode)) {
      if (request.ids() == null || request.ids().isEmpty()) {
        throw new BadRequestException("ids is required for mode=IDS.");
      }
      if (request.ids().stream().anyMatch(Objects::isNull)) {
        throw new BadRequestException("ids cannot contain null values.");
      }
      if (request.ids().size() > MAX_IDS) {
        throw new BadRequestException("ids cannot exceed " + MAX_IDS + ".");
      }
      return replayIds(eventKey, replayUrl, day, request.ids(), "ids", null);
    }
    throw new BadRequestException("Unsupported replay mode: " + request.mode());
  }

  public ReplayJobResponse replayFilters(ReplayFilterRequest request) {
    if (request == null) {
      throw new BadRequestException("Replay filter request is required.");
    }
    if (request.eventKey() == null || request.eventKey().isBlank()) {
      throw new BadRequestException("eventKey is required.");
    }
    LocalDate day = DayValidator.parseDay(request.day());
    String eventKey = request.eventKey();
    String replayUrl = registry.replayUrl(eventKey);

    ReplayFilters filters =
        request.filters() == null
            ? new ReplayFilters(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null)
            : request.filters();

    DateTimeRange range =
        resolveRange(
            day,
            filters.fromDate(),
            filters.toDate(),
            filters.fromTime(),
            filters.toTime());
    validateRetryBounds(filters.retryAttemptMin(), filters.retryAttemptMax());
    validateLatencyBounds(filters.latencyMin(), filters.latencyMax(), "latency");
    validateLatencyBounds(
        filters.receivedLatencyMin(), filters.receivedLatencyMax(), "receivedLatency");

    String failureTable = registry.failureTable(eventKey);
    long total =
        repository.loadFailureRowCount(
            failureTable,
            range.startTimestamp(),
            range.endTimestamp(),
            range.endInclusive(),
            filters.traceId(),
            filters.messageKey(),
            filters.accountNumber(),
            filters.latencyMin(),
            filters.latencyMax(),
            filters.receivedLatencyMin(),
            filters.receivedLatencyMax(),
            filters.exceptionType(),
            filters.retriable(),
            filters.retryAttemptMin(),
            filters.retryAttemptMax());

    List<Map<String, Object>> rows =
        repository.loadFailureRowsForReplay(
            failureTable,
            range.startTimestamp(),
            range.endTimestamp(),
            range.endInclusive(),
            filters.traceId(),
            filters.messageKey(),
            filters.accountNumber(),
            filters.latencyMin(),
            filters.latencyMax(),
            filters.receivedLatencyMin(),
            filters.receivedLatencyMax(),
            filters.exceptionType(),
            filters.retriable(),
            filters.retryAttemptMin(),
            filters.retryAttemptMax());

    ExtractedIds extracted = extractIds(rows);
    Map<Long, Map<String, Object>> detailById = mapRowsById(rows);
    String filtersJson = toFiltersJson(filters);
    String jobId =
        createReplayJob(
            eventKey, day, "filters", filtersJson, (int) total, LocalDateTime.now());
    insertReplayItems(jobId, eventKey, extracted.ids(), List.of(), false, detailById);

    ReplayResult result = sendReplayBatches(jobId, replayUrl, extracted.ids());
    List<Long> failedIds = new ArrayList<>(extracted.invalidIds());
    failedIds.addAll(result.failedIds());
    int failed = failedIds.size();
    int succeeded = extracted.ids().size() - result.failedIds().size();
    String status = resolveJobStatus(extracted.ids().size(), succeeded, failed);
    updateReplayJob(jobId, (int) total, status, LocalDateTime.now());
    return new ReplayJobResponse(
        jobId,
        status,
        (int) total,
        succeeded,
        failed,
        failedIds,
        Instant.now());
  }

  public void updateReplayItems(String replayId, ReplayItemUpdateRequest request) {
    if (replayId == null || replayId.isBlank()) {
      throw new BadRequestException("replayId is required.");
    }
    if (request == null || request.items() == null || request.items().isEmpty()) {
      return;
    }
    LocalDateTime now = LocalDateTime.now();
    for (ReplayItemUpdateRow row : request.items()) {
      if (row == null || row.recordId() == null) {
        continue;
      }
      String status = row.status() == null ? ITEM_STATUS_FAILED : row.status();
      int attempts = row.attemptCount() == null ? 1 : row.attemptCount();
      auditRepository.updateItemStatus(
          new ReplayItemUpdate(
              replayId,
              row.recordId(),
              status,
              attempts,
              now,
              row.lastError(),
              row.emittedId(),
              now));
    }
  }

  public ReplayAuditResponse getReplayAudit(
      String eventKey,
      String status,
      String requestedBy,
      String search,
      Integer page,
      Integer size) {
    int resolvedSize = resolveSize(size);
    int resolvedPage = resolvePage(page);
    int offset = resolvedPage * resolvedSize;
    String normalizedStatus =
        status == null || status.isBlank() ? null : status.trim().toUpperCase();
    String normalizedEvent = eventKey == null || eventKey.isBlank() ? null : eventKey.trim();
    String normalizedRequestedBy =
        requestedBy == null || requestedBy.isBlank() ? null : requestedBy.trim();
    String normalizedSearch = search == null || search.isBlank() ? null : search.trim();

    List<ReplayAuditRepository.ReplayAuditItemRow> rows =
        auditRepository.loadReplayAuditItems(
            normalizedEvent, normalizedStatus, normalizedRequestedBy, normalizedSearch, resolvedSize, offset);
    ReplayAuditRepository.ReplayAuditStatsRow statsRow =
        auditRepository.loadReplayAuditStats(
            normalizedEvent, normalizedStatus, normalizedRequestedBy, normalizedSearch);
    List<String> operators = auditRepository.loadReplayAuditOperators();
    List<String> eventKeys = auditRepository.loadReplayAuditEventKeys();

    List<ReplayAuditResponse.ReplayAuditItemResponse> items =
        rows.stream()
            .map(
                (row) ->
                    new ReplayAuditResponse.ReplayAuditItemResponse(
                        row.replayId(),
                        row.recordId(),
                        row.eventKey(),
                        row.traceId(),
                        row.status(),
                        row.replayedAt(),
                        row.durationMs(),
                        row.requestedBy(),
                        row.reason(),
                        resolveResultDetail(row.status(), row.lastError(), row.emittedId())))
            .toList();
    ReplayAuditResponse.ReplayAuditStatsResponse stats =
        new ReplayAuditResponse.ReplayAuditStatsResponse(
            statsRow.total(),
            statsRow.replayed(),
            statsRow.failed(),
            statsRow.queued(),
            statsRow.avgDurationMs(),
            statsRow.latestAt());
    return new ReplayAuditResponse(items, stats, operators, eventKeys);
  }

  public ReplayJobListResponse getReplayJobs(
      String eventKey,
      String status,
      String requestedBy,
      String search,
      Integer page,
      Integer size) {
    int resolvedSize = resolveSize(size);
    int resolvedPage = resolvePage(page);
    int offset = resolvedPage * resolvedSize;
    String normalizedStatus =
        status == null || status.isBlank() ? null : status.trim().toUpperCase();
    String normalizedEvent = eventKey == null || eventKey.isBlank() ? null : eventKey.trim();
    String normalizedRequestedBy =
        requestedBy == null || requestedBy.isBlank() ? null : requestedBy.trim();
    String normalizedSearch = search == null || search.isBlank() ? null : search.trim();

    List<ReplayAuditRepository.ReplayJobSummaryRow> jobs =
        auditRepository.loadReplayJobs(
            normalizedEvent, normalizedStatus, normalizedRequestedBy, normalizedSearch, resolvedSize, offset);
    long total =
        auditRepository.loadReplayJobCount(
            normalizedEvent, normalizedStatus, normalizedRequestedBy, normalizedSearch);
    ReplayAuditRepository.ReplayAuditStatsRow statsRow =
        auditRepository.loadReplayAuditStats(
            normalizedEvent, normalizedStatus, normalizedRequestedBy, normalizedSearch);
    List<String> operators = auditRepository.loadReplayAuditOperators();
    List<String> eventKeys = auditRepository.loadReplayAuditEventKeys();

    List<ReplayJobListResponse.ReplayJobSummaryResponse> jobResponses =
        jobs.stream()
            .map(
                (row) ->
                    new ReplayJobListResponse.ReplayJobSummaryResponse(
                        row.id(),
                        row.eventKey(),
                        row.selectionType(),
                        row.totalRequested(),
                        row.status(),
                        row.requestedBy(),
                        row.reason(),
                        row.createdAt(),
                        row.completedAt(),
                        row.succeeded(),
                        row.failed(),
                        row.queued()))
            .toList();
    ReplayAuditResponse.ReplayAuditStatsResponse stats =
        new ReplayAuditResponse.ReplayAuditStatsResponse(
            statsRow.total(),
            statsRow.replayed(),
            statsRow.failed(),
            statsRow.queued(),
            statsRow.avgDurationMs(),
            statsRow.latestAt());
    return new ReplayJobListResponse(
        jobResponses, resolvedPage, resolvedSize, total, stats, operators, eventKeys);
  }

  public ReplayJobItemsResponse getReplayJobItems(String replayId) {
    if (replayId == null || replayId.isBlank()) {
      throw new BadRequestException("replayId is required.");
    }
    List<ReplayAuditRepository.ReplayJobItemRow> rows = auditRepository.loadReplayJobItems(replayId);
    List<ReplayJobItemsResponse.ReplayJobItemResponse> items =
        rows.stream()
            .map(
                (row) ->
                    new ReplayJobItemsResponse.ReplayJobItemResponse(
                        row.recordId(),
                        row.status(),
                        row.attemptCount(),
                        row.lastAttemptAt(),
                        row.lastError(),
                        row.emittedId(),
                        row.traceId(),
                        row.messageKey(),
                        row.accountNumber(),
                        row.exceptionType(),
                        row.eventDatetime()))
            .toList();
    return new ReplayJobItemsResponse(items);
  }

  private ReplayResponse replayIds(
      String eventKey,
      String replayUrl,
      LocalDate day,
      List<Long> ids,
      String selectionType,
      String filtersJson) {
    String table = registry.failureTable(eventKey);
    List<Map<String, Object>> rows = repository.loadFailureRowsByIds(table, ids);
    ExtractedIds extracted = extractIds(rows);
    Map<Long, Map<String, Object>> detailById = mapRowsById(rows);
    Set<Long> resolved = new HashSet<>(extracted.ids());
    List<Long> missing = new ArrayList<>();
    for (Long id : ids) {
      if (id != null && !resolved.contains(id)) {
        missing.add(id);
      }
    }

    String jobId =
        createReplayJob(
            eventKey, day, selectionType, filtersJson, ids.size(), LocalDateTime.now());
    insertReplayItems(jobId, eventKey, extracted.ids(), missing, true, detailById);

    ReplayResult result = sendReplayBatches(jobId, replayUrl, extracted.ids());
    List<Long> failedIds = new ArrayList<>(extracted.invalidIds());
    failedIds.addAll(missing);
    failedIds.addAll(result.failedIds());
    int failed = failedIds.size();
    int succeeded = ids.size() - failed;
    String status = resolveJobStatus(ids.size(), succeeded, failed);
    updateReplayJob(jobId, ids.size(), status, LocalDateTime.now());
    return new ReplayResponse(jobId, ids.size(), succeeded, failed, failedIds);
  }

  private ReplayResult sendReplayBatches(String jobId, String replayUrl, List<Long> ids) {
    if (ids == null || ids.isEmpty()) {
      return new ReplayResult(0, 0, List.of());
    }
    int succeeded = 0;
    List<Long> failedIds = new ArrayList<>();
    for (int i = 0; i < ids.size(); i += MAX_IDS) {
      List<Long> batch = ids.subList(i, Math.min(i + MAX_IDS, ids.size()));
      ReplayResult result = sendReplayIds(jobId, replayUrl, batch);
      succeeded += result.succeeded();
      failedIds.addAll(result.failedIds());
    }
    return new ReplayResult(succeeded, failedIds.size(), failedIds);
  }

  private ReplayResult sendReplayIds(String jobId, String replayUrl, List<Long> ids) {
    if (ids == null || ids.isEmpty()) {
      return new ReplayResult(0, 0, List.of());
    }
    ReplayExternalRequest request = buildExternalRequest(jobId, ids);
    try {
      log.info(
          "Replay request sent replayId={} requestId={} url={} idsCount={}",
          request.replayId(),
          request.requestId(),
          replayUrl,
          ids.size());
      if (log.isDebugEnabled()) {
        log.debug("Replay request ids replayId={} ids={}", request.replayId(), ids);
      }
      ReplayExternalResponse response = externalClient.replay(replayUrl, request);
      ReplayResult result = applyReplayResponse(jobId, ids, response);
      log.info(
          "Replay response received replayId={} succeeded={} failed={}",
          request.replayId(),
          result.succeeded(),
          result.failed());
      return result;
    } catch (Exception ex) {
      log.warn("Replay external call failed for replayId={}", request.replayId(), ex);
      markItemsFailed(jobId, ids, "Replay call failed");
      return new ReplayResult(0, ids.size(), new ArrayList<>(ids));
    }
  }

  private ReplayExternalRequest buildExternalRequest(String replayId, List<Long> ids) {
    return new ReplayExternalRequest(
        replayId,
        ids,
        UUID.randomUUID().toString(),
        REQUESTED_BY,
        REPLAY_REASON,
        false);
  }

  private String createReplayJob(
      String eventKey,
      LocalDate day,
      String selectionType,
      String filtersJson,
      int totalRequested,
      LocalDateTime now) {
    String jobId = UUID.randomUUID().toString();
    auditRepository.insertJob(
        new ReplayJobRecord(
            jobId,
            eventKey,
            day,
            selectionType,
            filtersJson,
            now,
            REQUESTED_BY,
            REPLAY_REASON,
            totalRequested,
            JOB_STATUS_RUNNING,
            now));
    return jobId;
  }

  private void updateReplayJob(
      String jobId, int totalRequested, String status, LocalDateTime completedAt) {
    auditRepository.updateJobSummary(new ReplayJobUpdate(jobId, totalRequested, status, completedAt));
  }

  private void insertReplayItems(
      String jobId,
      String eventKey,
      List<Long> foundIds,
      List<Long> missingIds,
      boolean includeMissing,
      Map<Long, Map<String, Object>> detailById) {
    LocalDateTime now = LocalDateTime.now();
    List<ReplayItemRecord> items = new ArrayList<>();
    if (foundIds != null) {
      for (Long id : foundIds) {
        if (id == null) {
          continue;
        }
        Map<String, Object> row = detailById == null ? null : detailById.get(id);
        items.add(
            new ReplayItemRecord(
                jobId,
                id,
                eventKey,
                ITEM_STATUS_QUEUED,
                0,
                null,
                null,
                null,
                now,
                toStringValue(row, "event_trace_id"),
                toStringValue(row, "message_key"),
                toStringValue(row, "account_number"),
                toStringValue(row, "exception_type"),
                toLocalDateTime(row == null ? null : row.get("event_datetime")),
                toStringValue(row, "source_payload")));
      }
    }
    if (includeMissing && missingIds != null) {
      for (Long id : missingIds) {
        if (id == null) {
          continue;
        }
        items.add(
            new ReplayItemRecord(
                jobId,
                id,
                eventKey,
                ITEM_STATUS_NOT_FOUND,
                0,
                null,
                "Record not found",
                null,
                now,
                null,
                null,
                null,
                null,
                null,
                null));
      }
    }
    auditRepository.insertItems(items);
  }

  private ReplayResult applyReplayResponse(
      String jobId, List<Long> ids, ReplayExternalResponse response) {
    if (response == null || response.results() == null || response.results().isEmpty()) {
      markItemsFailed(jobId, ids, "Empty replay response");
      return new ReplayResult(0, ids.size(), new ArrayList<>(ids));
    }
    Map<Long, ReplayExternalResult> resultMap = new java.util.HashMap<>();
    for (ReplayExternalResult result : response.results()) {
      if (result.id() != null) {
        resultMap.put(result.id(), result);
      }
    }
    int succeeded = 0;
    List<Long> failedIds = new ArrayList<>();
    LocalDateTime now = LocalDateTime.now();
    for (Long id : ids) {
      if (id == null) {
        continue;
      }
      ReplayExternalResult result = resultMap.get(id);
      if (result == null) {
        failedIds.add(id);
        auditRepository.updateItemStatus(
            new ReplayItemUpdate(
                jobId,
                id,
                ITEM_STATUS_FAILED,
                1,
                now,
                "No response",
                null,
                now));
        continue;
      }
      boolean success = isSuccessStatus(result.status());
      String status = success ? ITEM_STATUS_REPLAYED : ITEM_STATUS_FAILED;
      int attemptCount = result.attemptCount() == null ? 1 : result.attemptCount();
      auditRepository.updateItemStatus(
          new ReplayItemUpdate(
              jobId,
              id,
              status,
              attemptCount,
              now,
              result.error(),
              result.emittedId(),
              now));
      if (success) {
        succeeded++;
      } else {
        failedIds.add(id);
      }
    }
    return new ReplayResult(succeeded, failedIds.size(), failedIds);
  }

  private String resolveResultDetail(String status, String lastError, String emittedId) {
    if (status == null) {
      return lastError;
    }
    if ("REPLAYED".equalsIgnoreCase(status) && emittedId != null && !emittedId.isBlank()) {
      return "Emitted " + emittedId;
    }
    if (lastError != null && !lastError.isBlank()) {
      return lastError;
    }
    return null;
  }

  private Map<Long, Map<String, Object>> mapRowsById(List<Map<String, Object>> rows) {
    if (rows == null || rows.isEmpty()) {
      return Map.of();
    }
    Map<Long, Map<String, Object>> mapped = new java.util.HashMap<>();
    for (Map<String, Object> row : rows) {
      if (row == null) {
        continue;
      }
      Object value = row.get("id");
      Long id = toLongValue(value);
      if (id == null) {
        continue;
      }
      mapped.put(id, row);
    }
    return mapped;
  }

  private Long toLongValue(Object value) {
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value instanceof String text) {
      try {
        return Long.parseLong(text);
      } catch (NumberFormatException ex) {
        return null;
      }
    }
    return null;
  }

  private String toStringValue(Map<String, Object> row, String key) {
    if (row == null || key == null) {
      return null;
    }
    Object value = row.get(key);
    if (value == null) {
      return null;
    }
    String text = String.valueOf(value);
    return text.isBlank() ? null : text;
  }

  private LocalDateTime toLocalDateTime(Object value) {
    if (value instanceof LocalDateTime dateTime) {
      return dateTime;
    }
    if (value instanceof java.sql.Timestamp ts) {
      return ts.toLocalDateTime();
    }
    if (value instanceof java.util.Date date) {
      return LocalDateTime.ofInstant(date.toInstant(), java.time.ZoneId.systemDefault());
    }
    return null;
  }

  private int resolvePage(Integer page) {
    if (page == null || page < 0) {
      return 0;
    }
    return page;
  }

  private int resolveSize(Integer size) {
    if (size == null || size <= 0) {
      return DEFAULT_SIZE;
    }
    return Math.min(size, MAX_SIZE);
  }

  private void markItemsFailed(String jobId, List<Long> ids, String error) {
    LocalDateTime now = LocalDateTime.now();
    for (Long id : ids) {
      if (id == null) {
        continue;
      }
      auditRepository.updateItemStatus(
          new ReplayItemUpdate(jobId, id, ITEM_STATUS_FAILED, 1, now, error, null, now));
    }
  }

  private String resolveJobStatus(int requested, int succeeded, int failed) {
    if (requested == 0) {
      return JOB_STATUS_COMPLETED;
    }
    if (succeeded == 0 && failed > 0) {
      return JOB_STATUS_FAILED;
    }
    if (failed > 0) {
      return JOB_STATUS_PARTIAL;
    }
    return JOB_STATUS_COMPLETED;
  }

  private String toFiltersJson(ReplayFilters filters) {
    if (filters == null) {
      return null;
    }
    return filters.toString();
  }

  private boolean isSuccessStatus(String status) {
    if (status == null) {
      return false;
    }
    String normalized = status.trim().toUpperCase();
    return "REPLAYED".equals(normalized)
        || "SUCCESS".equals(normalized)
        || "COMPLETED".equals(normalized)
        || "OK".equals(normalized);
  }

  private ExtractedIds extractIds(List<Map<String, Object>> rows) {
    List<Long> ids = new ArrayList<>();
    List<Long> invalid = new ArrayList<>();
    if (rows == null || rows.isEmpty()) {
      return new ExtractedIds(ids, invalid);
    }
    for (Map<String, Object> row : rows) {
      Long id = parseId(row.get("id"));
      if (id == null) {
        invalid.add(-1L);
        continue;
      }
      ids.add(id);
    }
    return new ExtractedIds(ids, invalid);
  }

  private Long parseId(Object value) {
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value instanceof String text) {
      try {
        return Long.parseLong(text);
      } catch (NumberFormatException ex) {
        return null;
      }
    }
    return null;
  }

  private DateTimeRange resolveRange(
      LocalDate day, String fromDate, String toDate, String fromTime, String toTime) {
    LocalDate start = fromDate == null ? null : DayValidator.parseDay(fromDate);
    LocalDate end = toDate == null ? null : DayValidator.parseDay(toDate);
    if (start == null && end == null) {
      start = day;
      end = day;
    } else if (start == null) {
      start = end;
    } else if (end == null) {
      end = start;
    }
    if (start.isAfter(end)) {
      throw new BadRequestException("fromDate must be <= toDate.");
    }

    LocalTime startTime = DayValidator.parseTime(fromTime);
    LocalTime endTime = DayValidator.parseTime(toTime);
    LocalTime resolvedStart = startTime == null ? LocalTime.MIDNIGHT : startTime;
    LocalDateTime startTimestamp = LocalDateTime.of(start, resolvedStart);
    boolean endInclusive = endTime != null;
    LocalDateTime endTimestamp =
        endInclusive ? LocalDateTime.of(end, endTime) : end.plusDays(1).atStartOfDay();
    if (startTimestamp.isAfter(endTimestamp)) {
      throw new BadRequestException("fromTime must be <= toTime.");
    }
    return new DateTimeRange(startTimestamp, endTimestamp, endInclusive);
  }

  private void validateRetryBounds(Integer min, Integer max) {
    if (min != null && max != null && min > max) {
      throw new BadRequestException("retryAttemptMin cannot be greater than retryAttemptMax.");
    }
  }

  private void validateLatencyBounds(Long min, Long max, String label) {
    if (min != null && max != null && min > max) {
      throw new BadRequestException(label + "Min cannot be greater than " + label + "Max.");
    }
  }

  private record DateTimeRange(
      LocalDateTime startTimestamp, LocalDateTime endTimestamp, boolean endInclusive) {}

  private record ReplayResult(int succeeded, int failed, List<Long> failedIds) {}

  private record ExtractedIds(List<Long> ids, List<Long> invalidIds) {}
}

package com.vibe.events.service;

import com.vibe.events.dto.ReplayExternalRequest;
import com.vibe.events.dto.ReplayExternalResponse;
import com.vibe.events.dto.ReplayExternalResult;
import com.vibe.events.dto.ReplayFilterRequest;
import com.vibe.events.dto.ReplayFilters;
import com.vibe.events.dto.ReplayJobResponse;
import com.vibe.events.dto.ReplayRequest;
import com.vibe.events.dto.ReplayResponse;
import com.vibe.events.error.BadRequestException;
import com.vibe.events.registry.EventRegistry;
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

  private final EventRegistry registry;
  private final RecordsRepository repository;
  private final ReplayExternalClient externalClient;

  public ReplayService(
      EventRegistry registry, RecordsRepository repository, ReplayExternalClient externalClient) {
    this.registry = registry;
    this.repository = repository;
    this.externalClient = externalClient;
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
    DayValidator.parseDay(request.day());
    String eventKey = request.eventKey();
    String replayUrl = registry.replayUrl(eventKey);

    String mode = request.mode().trim().toUpperCase();
    if ("ID".equals(mode)) {
      if (request.id() == null) {
        throw new BadRequestException("id is required for mode=ID.");
      }
      return replayIds(eventKey, replayUrl, List.of(request.id()));
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
      return replayIds(eventKey, replayUrl, request.ids());
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
    ReplayResult result = sendReplayBatches(replayUrl, extracted.ids());
    List<Long> failedIds = new ArrayList<>(extracted.invalidIds());
    failedIds.addAll(result.failedIds());
    int failed = failedIds.size();
    int succeeded = extracted.ids().size() - result.failedIds().size();
    String jobId = UUID.randomUUID().toString();
    return new ReplayJobResponse(
        jobId,
        "completed",
        (int) total,
        succeeded,
        failed,
        failedIds,
        Instant.now());
  }

  private ReplayResponse replayIds(String eventKey, String replayUrl, List<Long> ids) {
    String table = registry.failureTable(eventKey);
    List<Map<String, Object>> rows = repository.loadFailureRowsByIds(table, ids);
    ExtractedIds extracted = extractIds(rows);
    Set<Long> resolved = new HashSet<>(extracted.ids());
    List<Long> missing = new ArrayList<>();
    for (Long id : ids) {
      if (id != null && !resolved.contains(id)) {
        missing.add(id);
      }
    }
    ReplayResult result = sendReplayBatches(replayUrl, extracted.ids());
    List<Long> failedIds = new ArrayList<>(extracted.invalidIds());
    failedIds.addAll(missing);
    failedIds.addAll(result.failedIds());
    int failed = failedIds.size();
    int succeeded = ids.size() - failed;
    return new ReplayResponse(ids.size(), succeeded, failed, failedIds);
  }

  private ReplayResult sendReplayBatches(String replayUrl, List<Long> ids) {
    if (ids == null || ids.isEmpty()) {
      return new ReplayResult(0, 0, List.of());
    }
    int succeeded = 0;
    List<Long> failedIds = new ArrayList<>();
    for (int i = 0; i < ids.size(); i += MAX_IDS) {
      List<Long> batch = ids.subList(i, Math.min(i + MAX_IDS, ids.size()));
      ReplayResult result = sendReplayIds(replayUrl, batch);
      succeeded += result.succeeded();
      failedIds.addAll(result.failedIds());
    }
    return new ReplayResult(succeeded, failedIds.size(), failedIds);
  }

  private ReplayResult sendReplayIds(String replayUrl, List<Long> ids) {
    if (ids == null || ids.isEmpty()) {
      return new ReplayResult(0, 0, List.of());
    }
    ReplayExternalRequest request = buildExternalRequest(ids);
    try {
      ReplayExternalResponse response = externalClient.replay(replayUrl, request);
      return parseExternalResponse(ids, response);
    } catch (Exception ex) {
      log.warn("Replay external call failed for requestId={}", request.requestId(), ex);
      return new ReplayResult(0, ids.size(), new ArrayList<>(ids));
    }
  }

  private ReplayExternalRequest buildExternalRequest(List<Long> ids) {
    return new ReplayExternalRequest(
        ids, UUID.randomUUID().toString(), REQUESTED_BY, REPLAY_REASON, false);
  }

  private ReplayResult parseExternalResponse(List<Long> ids, ReplayExternalResponse response) {
    if (response == null || response.results() == null || response.results().isEmpty()) {
      return new ReplayResult(0, ids.size(), new ArrayList<>(ids));
    }
    Set<Long> responded = new HashSet<>();
    List<Long> failedIds = new ArrayList<>();
    int succeeded = 0;
    for (ReplayExternalResult result : response.results()) {
      Long id = result.id();
      if (id == null || responded.contains(id)) {
        continue;
      }
      responded.add(id);
      if (isSuccessStatus(result.status())) {
        succeeded++;
      } else {
        failedIds.add(id);
      }
    }
    for (Long id : ids) {
      if (id != null && !responded.contains(id)) {
        failedIds.add(id);
      }
    }
    return new ReplayResult(succeeded, failedIds.size(), failedIds);
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

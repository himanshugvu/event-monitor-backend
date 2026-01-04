package com.vibe.events.service;

import com.vibe.events.dto.PagedRowsResponse;
import com.vibe.events.error.BadRequestException;
import com.vibe.events.registry.EventRegistry;
import com.vibe.events.repo.RecordsRepository;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;

@Service
public class RecordsService {
  private static final int DEFAULT_SIZE = 50;
  private static final int MAX_SIZE = 200;

  private final RecordsRepository repository;
  private final EventRegistry registry;

  public RecordsService(RecordsRepository repository, EventRegistry registry) {
    this.repository = repository;
    this.registry = registry;
  }

  public PagedRowsResponse loadSuccessRows(
      LocalDate day,
      LocalDate fromDate,
      LocalDate toDate,
      LocalTime fromTime,
      LocalTime toTime,
      String eventKey,
      Integer page,
      Integer size,
      String traceId,
      String messageKey,
      String accountNumber,
      Long latencyMin,
      Long latencyMax,
      Long receivedLatencyMin,
      Long receivedLatencyMax) {
    int resolvedSize = resolveSize(size);
    int resolvedPage = resolvePage(page);
    int offset = resolvedPage * resolvedSize;
    String table = registry.successTable(eventKey);
    DateTimeRange range = resolveRange(day, fromDate, toDate, fromTime, toTime);
    if (latencyMin != null && latencyMax != null && latencyMin > latencyMax) {
      throw new BadRequestException("latencyMin cannot be greater than latencyMax.");
    }
    if (receivedLatencyMin != null
        && receivedLatencyMax != null
        && receivedLatencyMin > receivedLatencyMax) {
      throw new BadRequestException("receivedLatencyMin cannot be greater than receivedLatencyMax.");
    }

    List<Map<String, Object>> rows =
        repository.loadSuccessRows(
            table,
            range.startDay(),
            range.endDay(),
            range.startTimestamp(),
            range.endTimestamp(),
            traceId,
            messageKey,
            accountNumber,
            latencyMin,
            latencyMax,
            receivedLatencyMin,
            receivedLatencyMax,
            offset,
            resolvedSize);
    long total =
        repository.loadSuccessRowCount(
            table,
            range.startDay(),
            range.endDay(),
            range.startTimestamp(),
            range.endTimestamp(),
            traceId,
            messageKey,
            accountNumber,
            latencyMin,
            latencyMax,
            receivedLatencyMin,
            receivedLatencyMax);
    return new PagedRowsResponse(resolvedPage, resolvedSize, total, rows);
  }

  public PagedRowsResponse loadFailureRows(
      LocalDate day,
      LocalDate fromDate,
      LocalDate toDate,
      LocalTime fromTime,
      LocalTime toTime,
      String eventKey,
      Integer page,
      Integer size,
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
    int resolvedSize = resolveSize(size);
    int resolvedPage = resolvePage(page);
    int offset = resolvedPage * resolvedSize;

    DateTimeRange range = resolveRange(day, fromDate, toDate, fromTime, toTime);
    if (retryAttemptMin != null
        && retryAttemptMax != null
        && retryAttemptMin > retryAttemptMax) {
      throw new BadRequestException("retryAttemptMin cannot be greater than retryAttemptMax.");
    }
    if (latencyMin != null && latencyMax != null && latencyMin > latencyMax) {
      throw new BadRequestException("latencyMin cannot be greater than latencyMax.");
    }
    if (receivedLatencyMin != null
        && receivedLatencyMax != null
        && receivedLatencyMin > receivedLatencyMax) {
      throw new BadRequestException("receivedLatencyMin cannot be greater than receivedLatencyMax.");
    }

    String table = registry.failureTable(eventKey);
    List<Map<String, Object>> rows =
        repository.loadFailureRows(
            table,
            range.startDay(),
            range.endDay(),
            range.startTimestamp(),
            range.endTimestamp(),
            traceId,
            messageKey,
            accountNumber,
            latencyMin,
            latencyMax,
            receivedLatencyMin,
            receivedLatencyMax,
            exceptionType,
            retriable,
            retryAttemptMin,
            retryAttemptMax,
            offset,
            resolvedSize);
    long total =
        repository.loadFailureRowCount(
            table,
            range.startDay(),
            range.endDay(),
            range.startTimestamp(),
            range.endTimestamp(),
            traceId,
            messageKey,
            accountNumber,
            latencyMin,
            latencyMax,
            receivedLatencyMin,
            receivedLatencyMax,
            exceptionType,
            retriable,
            retryAttemptMin,
            retryAttemptMax);
    return new PagedRowsResponse(resolvedPage, resolvedSize, total, rows);
  }

  public List<String> loadFailureExceptionTypes(
      LocalDate day,
      LocalDate fromDate,
      LocalDate toDate,
      LocalTime fromTime,
      LocalTime toTime,
      String eventKey) {
    DateTimeRange range = resolveRange(day, fromDate, toDate, fromTime, toTime);
    String table = registry.failureTable(eventKey);
    return repository.loadFailureExceptionTypes(
        table,
        range.startDay(),
        range.endDay(),
        range.startTimestamp(),
        range.endTimestamp());
  }

  private DateTimeRange resolveRange(
      LocalDate day, LocalDate fromDate, LocalDate toDate, LocalTime fromTime, LocalTime toTime) {
    LocalDate start = fromDate;
    LocalDate end = toDate;
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

    LocalTime startTime = fromTime == null ? LocalTime.MIDNIGHT : fromTime;
    LocalTime endTime = toTime == null ? LocalTime.of(23, 59, 59) : toTime;
    LocalDateTime startTimestamp = LocalDateTime.of(start, startTime);
    LocalDateTime endTimestamp = LocalDateTime.of(end, endTime);
    if (startTimestamp.isAfter(endTimestamp)) {
      throw new BadRequestException("fromTime must be <= toTime.");
    }
    return new DateTimeRange(start, end, startTimestamp, endTimestamp);
  }

  private record DateTimeRange(
      LocalDate startDay,
      LocalDate endDay,
      LocalDateTime startTimestamp,
      LocalDateTime endTimestamp) {}

  private int resolvePage(Integer page) {
    int resolved = page == null ? 0 : page;
    if (resolved < 0) {
      throw new BadRequestException("page must be >= 0.");
    }
    return resolved;
  }

  private int resolveSize(Integer size) {
    int resolved = size == null ? DEFAULT_SIZE : size;
    if (resolved <= 0) {
      throw new BadRequestException("size must be > 0.");
    }
    if (resolved > MAX_SIZE) {
      throw new BadRequestException("size must be <= " + MAX_SIZE + ".");
    }
    return resolved;
  }
}

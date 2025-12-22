package com.vibe.events.service;

import com.vibe.events.dto.PagedRowsResponse;
import com.vibe.events.error.BadRequestException;
import com.vibe.events.registry.EventRegistry;
import com.vibe.events.repo.RecordsRepository;
import java.time.LocalDate;
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
      String eventKey,
      Integer page,
      Integer size,
      String traceId,
      String messageKey,
      String accountNumber) {
    int resolvedSize = resolveSize(size);
    int resolvedPage = resolvePage(page);
    int offset = resolvedPage * resolvedSize;
    String table = registry.successTable(eventKey);

    List<Map<String, Object>> rows =
        repository.loadSuccessRows(
            table, day, traceId, messageKey, accountNumber, offset, resolvedSize);
    return new PagedRowsResponse(resolvedPage, resolvedSize, rows);
  }

  public PagedRowsResponse loadFailureRows(
      LocalDate day,
      String eventKey,
      Integer page,
      Integer size,
      String traceId,
      String messageKey,
      String accountNumber,
      String exceptionType,
      Boolean retriable,
      Integer retryAttemptMin,
      Integer retryAttemptMax) {
    int resolvedSize = resolveSize(size);
    int resolvedPage = resolvePage(page);
    int offset = resolvedPage * resolvedSize;

    if (retryAttemptMin != null
        && retryAttemptMax != null
        && retryAttemptMin > retryAttemptMax) {
      throw new BadRequestException("retryAttemptMin cannot be greater than retryAttemptMax.");
    }

    String table = registry.failureTable(eventKey);
    List<Map<String, Object>> rows =
        repository.loadFailureRows(
            table,
            day,
            traceId,
            messageKey,
            accountNumber,
            exceptionType,
            retriable,
            retryAttemptMin,
            retryAttemptMax,
            offset,
            resolvedSize);
    return new PagedRowsResponse(resolvedPage, resolvedSize, rows);
  }

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

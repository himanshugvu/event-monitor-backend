package com.vibe.events.dto;

import java.time.LocalDateTime;
import java.util.List;

public record ReplayJobItemsResponse(List<ReplayJobItemResponse> items) {
  public record ReplayJobItemResponse(
      long recordId,
      String status,
      int attemptCount,
      LocalDateTime lastAttemptAt,
      String lastError,
      String emittedId,
      String traceId,
      String messageKey,
      String accountNumber,
      String exceptionType,
      LocalDateTime eventDatetime) {}
}

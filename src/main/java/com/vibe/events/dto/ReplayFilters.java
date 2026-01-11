package com.vibe.events.dto;

public record ReplayFilters(
    String traceId,
    String messageKey,
    String accountNumber,
    String exceptionType,
    Boolean retriable,
    Integer retryAttemptMin,
    Integer retryAttemptMax,
    Long latencyMin,
    Long latencyMax,
    Long receivedLatencyMin,
    Long receivedLatencyMax,
    String fromDate,
    String toDate,
    String fromTime,
    String toTime) {}

package com.vibe.events.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public record HousekeepingStatusResponse(
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
    List<HousekeepingRunItemResponse> items) {}

package com.vibe.events.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;

public record HousekeepingRunSummaryResponse(
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

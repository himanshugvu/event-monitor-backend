package com.vibe.events.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;

public record HousekeepingDailyResponse(
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

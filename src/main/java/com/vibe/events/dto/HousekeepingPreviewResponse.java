package com.vibe.events.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public record HousekeepingPreviewResponse(
    LocalDate cutoffDate,
    int retentionDays,
    LocalDateTime snapshotAt,
    long deletedSuccess,
    long deletedFailure,
    long deletedTotal,
    LocalDateTime nextRunAt,
    List<HousekeepingPreviewEvent> events) {}

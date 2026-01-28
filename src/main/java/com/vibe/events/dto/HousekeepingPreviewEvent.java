package com.vibe.events.dto;

public record HousekeepingPreviewEvent(
    String eventKey,
    long deletedSuccess,
    long deletedFailure,
    long deletedTotal,
    java.time.LocalDateTime nextRunAt) {}

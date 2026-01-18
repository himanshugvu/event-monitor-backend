package com.vibe.events.dto;

public record ReplayItemUpdateRow(
    Long recordId,
    String status,
    Integer attemptCount,
    String lastError,
    String emittedId) {}

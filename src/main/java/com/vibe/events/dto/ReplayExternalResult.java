package com.vibe.events.dto;

public record ReplayExternalResult(
    Long id, String status, String emittedId, String error, Integer attemptCount) {}

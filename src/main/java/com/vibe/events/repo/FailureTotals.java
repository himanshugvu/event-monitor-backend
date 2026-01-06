package com.vibe.events.repo;

public record FailureTotals(
    long failureCount, long retriableCount, Double avgLatencyMs, Double avgReceivedLatencyMs) {}

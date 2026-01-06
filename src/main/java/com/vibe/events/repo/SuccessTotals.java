package com.vibe.events.repo;

public record SuccessTotals(
    long successCount, Double avgLatencyMs, Double avgReceivedLatencyMs, Double avgSentLatencyMs) {}

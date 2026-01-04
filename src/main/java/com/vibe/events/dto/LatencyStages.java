package com.vibe.events.dto;

public record LatencyStages(
    double avgReceivedLatencyMs,
    double p95ReceivedLatencyMs,
    double p99ReceivedLatencyMs,
    double maxReceivedLatencyMs,
    double avgSentLatencyMs,
    double p95SentLatencyMs,
    double p99SentLatencyMs,
    double maxSentLatencyMs) {}

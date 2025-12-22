package com.vibe.events.dto;

public record Kpis(
    long total,
    long success,
    long failure,
    double successRate,
    long retriableFailures,
    double avgLatencyMs) {}

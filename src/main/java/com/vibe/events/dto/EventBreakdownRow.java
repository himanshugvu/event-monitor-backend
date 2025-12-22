package com.vibe.events.dto;

public record EventBreakdownRow(
    String eventKey,
    long total,
    long success,
    long failure,
    double successRate,
    long retriableFailures,
    double avgLatencyMs) {}

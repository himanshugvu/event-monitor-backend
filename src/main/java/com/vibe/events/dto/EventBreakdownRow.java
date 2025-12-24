package com.vibe.events.dto;

public record EventBreakdownRow(
    String eventKey,
    String eventName,
    String category,
    long total,
    long success,
    long failure,
    double successRate,
    long retriableFailures,
    double avgLatencyMs) {}

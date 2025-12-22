package com.vibe.events.dto;

import java.time.LocalDateTime;

public record BucketPoint(
    LocalDateTime bucketStart,
    long success,
    long failure,
    long total,
    double successRate,
    long retriableFailures,
    double avgLatencyMs) {}

package com.vibe.events.dto;

import java.time.LocalDateTime;
import java.util.List;

public record BucketPoint(
    LocalDateTime bucketStart,
    long success,
    long failure,
    long total,
    double successRate,
    long retriableFailures,
    double avgLatencyMs,
    List<String> failureSources) {}

package com.vibe.events.dto;

import java.time.Instant;
import java.util.List;

public record ReplayJobResponse(
    String jobId,
    String status,
    int requested,
    int succeeded,
    int failed,
    List<Long> failedIds,
    Instant createdAt) {}

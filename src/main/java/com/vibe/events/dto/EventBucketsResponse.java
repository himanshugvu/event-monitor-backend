package com.vibe.events.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public record EventBucketsResponse(
    LocalDate day,
    String eventKey,
    int intervalMinutes,
    LocalDateTime generatedAt,
    List<BucketPoint> buckets) {}

package com.vibe.events.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public record HomeBucketsResponse(
    LocalDate day, int intervalMinutes, LocalDateTime generatedAt, List<BucketPoint> buckets) {}

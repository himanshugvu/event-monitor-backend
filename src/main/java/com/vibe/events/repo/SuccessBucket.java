package com.vibe.events.repo;

public record SuccessBucket(int hourOfDay, Integer quarter, long successCount, Double avgLatencyMs) {}

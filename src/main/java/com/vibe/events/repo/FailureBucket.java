package com.vibe.events.repo;

public record FailureBucket(int hourOfDay, Integer quarter, long failureCount, long retriableCount) {}

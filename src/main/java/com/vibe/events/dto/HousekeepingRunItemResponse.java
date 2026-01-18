package com.vibe.events.dto;

public record HousekeepingRunItemResponse(
    String eventKey, long deletedSuccess, long deletedFailure, long deletedTotal) {}

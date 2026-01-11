package com.vibe.events.dto;

public record ReplayFilterRequest(String eventKey, String day, ReplayFilters filters) {}

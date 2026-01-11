package com.vibe.events.dto;

import java.util.List;

public record ReplayExternalResponse(
    String requestId,
    String status,
    List<ReplayExternalResult> results,
    ReplayExternalSummary summary) {}

package com.vibe.events.dto;

import java.util.List;

public record ReplayExternalRequest(
    List<Long> ids, String requestId, String requestedBy, String reason, Boolean dryRun) {}

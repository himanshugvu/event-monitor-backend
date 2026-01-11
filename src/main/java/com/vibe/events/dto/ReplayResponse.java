package com.vibe.events.dto;

import java.util.List;

public record ReplayResponse(int requested, int succeeded, int failed, List<Long> failedIds) {}

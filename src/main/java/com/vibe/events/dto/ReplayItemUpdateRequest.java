package com.vibe.events.dto;

import java.util.List;

public record ReplayItemUpdateRequest(String replayId, List<ReplayItemUpdateRow> items) {}

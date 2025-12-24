package com.vibe.events.dto;

import java.util.List;
import java.util.Map;

public record PagedRowsResponse(int page, int size, long total, List<Map<String, Object>> rows) {}

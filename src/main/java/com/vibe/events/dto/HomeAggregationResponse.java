package com.vibe.events.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public record HomeAggregationResponse(
    LocalDate day, LocalDateTime generatedAt, Kpis kpis, List<EventBreakdownRow> events) {}

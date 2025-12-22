package com.vibe.events.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;

public record EventSummaryResponse(
    LocalDate day, String eventKey, LocalDateTime generatedAt, Kpis kpis) {}

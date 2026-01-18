package com.vibe.events.dto;

import java.time.LocalDateTime;
import java.util.List;

public record ReplayJobListResponse(
    List<ReplayJobSummaryResponse> jobs,
    int page,
    int size,
    long total,
    ReplayAuditResponse.ReplayAuditStatsResponse stats,
    List<String> operators,
    List<String> eventKeys) {
  public record ReplayJobSummaryResponse(
      String replayId,
      String eventKey,
      String selectionType,
      int totalRequested,
      String status,
      String requestedBy,
      String reason,
      LocalDateTime createdAt,
      LocalDateTime completedAt,
      long succeeded,
      long failed,
      long queued) {}
}

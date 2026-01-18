package com.vibe.events.dto;

import java.time.LocalDateTime;
import java.util.List;

public record ReplayAuditResponse(
    List<ReplayAuditItemResponse> items,
    ReplayAuditStatsResponse stats,
    List<String> operators,
    List<String> eventKeys) {
  public record ReplayAuditItemResponse(
      String replayId,
      long recordId,
      String eventKey,
      String traceId,
      String status,
      LocalDateTime replayedAt,
      Long durationMs,
      String requestedBy,
      String reason,
      String resultDetail) {}

  public record ReplayAuditStatsResponse(
      long total,
      long replayed,
      long failed,
      long queued,
      Double avgDurationMs,
      LocalDateTime latestAt) {}
}

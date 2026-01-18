package com.vibe.events.controller;

import com.vibe.events.dto.HousekeepingDailyResponse;
import com.vibe.events.dto.HousekeepingPreviewResponse;
import com.vibe.events.dto.HousekeepingRunHistoryResponse;
import com.vibe.events.dto.HousekeepingRunResponse;
import com.vibe.events.dto.HousekeepingRunSummaryResponse;
import com.vibe.events.dto.HousekeepingStatusResponse;
import com.vibe.events.error.BadRequestException;
import com.vibe.events.service.HousekeepingService;
import java.time.LocalDate;
import java.util.List;
import java.util.Set;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/housekeeping")
public class HousekeepingController {
  private static final Set<String> JOB_TYPES =
      Set.of("RETENTION", "REPLAY_AUDIT", "HOUSEKEEPING_AUDIT");
  private final HousekeepingService service;

  public HousekeepingController(HousekeepingService service) {
    this.service = service;
  }

  @PostMapping("/run")
  public HousekeepingRunResponse runNow(
      @RequestParam(name = "jobType", required = false) String jobType,
      @RequestParam(name = "eventKey", required = false) String eventKey,
      @RequestParam(name = "date", required = false) LocalDate date) {
    String normalizedJob = normalizeJobType(jobType);
    String normalizedEvent = normalizeEventKey(normalizedJob, eventKey, false);
    if (isRetention(normalizedJob)) {
      return service.runRetention(normalizedEvent, "MANUAL", date);
    }
    return service.runAudit(normalizedJob, "MANUAL", date);
  }

  @GetMapping("/preview")
  public HousekeepingPreviewResponse preview(
      @RequestParam(name = "jobType", required = false) String jobType,
      @RequestParam(name = "eventKey", required = false) String eventKey) {
    String normalizedJob = normalizeJobType(jobType);
    String normalizedEvent = normalizeEventKey(normalizedJob, eventKey, true);
    return service.preview(normalizedJob, normalizedEvent);
  }

  @GetMapping("/status")
  public ResponseEntity<HousekeepingStatusResponse> status(
      @RequestParam(name = "jobType", required = false) String jobType,
      @RequestParam(name = "eventKey", required = false) String eventKey,
      @RequestParam(name = "date", required = false) LocalDate date) {
    String normalizedJob = normalizeJobType(jobType);
    String normalizedEvent = normalizeEventKey(normalizedJob, eventKey, true);
    HousekeepingStatusResponse status = service.status(normalizedJob, normalizedEvent, date);
    if (status == null) {
      return ResponseEntity.noContent().build();
    }
    return ResponseEntity.ok(status);
  }

  @GetMapping("/daily")
  public List<HousekeepingDailyResponse> listDaily(
      @RequestParam(name = "jobType", required = false) String jobType,
      @RequestParam(name = "eventKey", required = false) String eventKey,
      @RequestParam(defaultValue = "14") int limit) {
    int bounded = Math.max(1, Math.min(limit, 90));
    String normalizedJob = normalizeJobType(jobType);
    String normalizedEvent = normalizeEventKey(normalizedJob, eventKey, true);
    return service.listDaily(normalizedJob, normalizedEvent, bounded);
  }

  @GetMapping("/daily/{date}/runs")
  public List<HousekeepingRunResponse> listRunsForDate(
      @RequestParam(name = "jobType", required = false) String jobType,
      @RequestParam(name = "eventKey", required = false) String eventKey,
      @PathVariable("date") LocalDate date) {
    String normalizedJob = normalizeJobType(jobType);
    String normalizedEvent = normalizeEventKey(normalizedJob, eventKey, false);
    return service.listRunsForDate(normalizedJob, normalizedEvent, date);
  }

  @GetMapping("/runs")
  public List<HousekeepingRunHistoryResponse> listRuns(
      @RequestParam(name = "jobType", required = false) String jobType,
      @RequestParam(defaultValue = "30") int limit) {
    int bounded = Math.max(1, Math.min(limit, 200));
    return service.listRuns(normalizeJobType(jobType), bounded);
  }

  @GetMapping("/runs/summary")
  public List<HousekeepingRunSummaryResponse> listRunSummary(
      @RequestParam(name = "jobType", required = false) String jobType,
      @RequestParam(name = "eventKey", required = false) String eventKey,
      @RequestParam(defaultValue = "20") int limit,
      @RequestParam(defaultValue = "0") int offset) {
    int boundedLimit = Math.max(1, Math.min(limit, 100));
    int boundedOffset = Math.max(0, offset);
    String normalizedJob = normalizeJobType(jobType);
    String normalizedEvent = normalizeEventKey(normalizedJob, eventKey, true);
    return service.listRunSummary(normalizedJob, normalizedEvent, boundedLimit, boundedOffset);
  }

  private String normalizeJobType(String jobType) {
    if (jobType == null || jobType.isBlank()) {
      return "RETENTION";
    }
    String normalized = jobType.trim().toUpperCase();
    return JOB_TYPES.contains(normalized) ? normalized : "RETENTION";
  }

  private boolean isRetention(String jobType) {
    return "RETENTION".equals(jobType);
  }

  private String normalizeEventKey(String jobType, String eventKey, boolean allowAll) {
    if (!isRetention(jobType)) {
      return "__audit__";
    }
    if (eventKey == null || eventKey.isBlank()) {
      if (allowAll) {
        return "ALL";
      }
      throw new BadRequestException("eventKey is required for RETENTION jobs.");
    }
    if (allowAll && "ALL".equalsIgnoreCase(eventKey)) {
      return "ALL";
    }
    return eventKey.trim();
  }
}

package com.vibe.events.controller;

import com.vibe.events.dto.ReplayAuditResponse;
import com.vibe.events.dto.ReplayExternalRequest;
import com.vibe.events.dto.ReplayExternalResponse;
import com.vibe.events.dto.ReplayExternalResult;
import com.vibe.events.dto.ReplayExternalSummary;
import com.vibe.events.dto.ReplayFilterRequest;
import com.vibe.events.dto.ReplayItemUpdateRequest;
import com.vibe.events.dto.ReplayJobItemsResponse;
import com.vibe.events.dto.ReplayJobListResponse;
import com.vibe.events.dto.ReplayJobResponse;
import com.vibe.events.dto.ReplayRequest;
import com.vibe.events.dto.ReplayResponse;
import java.util.List;
import com.vibe.events.service.ReplayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.GetMapping;

@RestController
@RequestMapping("/api/v1")
public class ReplayController {
  private static final Logger log = LoggerFactory.getLogger(ReplayController.class);

  private final ReplayService replayService;

  public ReplayController(ReplayService replayService) {
    this.replayService = replayService;
  }

  @PostMapping("/replay")
  public ReplayResponse replay(@RequestBody ReplayRequest request) {
    return replayService.replay(request);
  }

  @PostMapping("/replay-jobs")
  public ReplayJobResponse replayJob(@RequestBody ReplayFilterRequest request) {
    return replayService.replayFilters(request);
  }

  @GetMapping("/replay-audit")
  public ReplayAuditResponse replayAudit(
      @RequestParam(required = false) String eventKey,
      @RequestParam(required = false) String status,
      @RequestParam(required = false) String requestedBy,
      @RequestParam(required = false) String search,
      @RequestParam(required = false) Integer page,
      @RequestParam(required = false) Integer size) {
    return replayService.getReplayAudit(eventKey, status, requestedBy, search, page, size);
  }

  @GetMapping("/replay-jobs")
  public ReplayJobListResponse replayJobs(
      @RequestParam(required = false) String eventKey,
      @RequestParam(required = false) String status,
      @RequestParam(required = false) String requestedBy,
      @RequestParam(required = false) String search,
      @RequestParam(required = false) Integer page,
      @RequestParam(required = false) Integer size) {
    return replayService.getReplayJobs(eventKey, status, requestedBy, search, page, size);
  }

  @GetMapping("/replay-jobs/{replayId}/items")
  public ReplayJobItemsResponse replayJobItems(
      @org.springframework.web.bind.annotation.PathVariable String replayId) {
    return replayService.getReplayJobItems(replayId);
  }

  @PostMapping("/replays/{replayId}/items")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public void updateReplayItems(
      @org.springframework.web.bind.annotation.PathVariable String replayId,
      @RequestBody ReplayItemUpdateRequest request) {
    replayService.updateReplayItems(replayId, request);
  }

  @PostMapping("/mock/replay")
  @ResponseStatus(HttpStatus.OK)
  public ReplayExternalResponse mockReplay(@RequestBody ReplayExternalRequest request) {
    List<Long> ids = request == null || request.ids() == null ? List.of() : request.ids();
    String replayId = request == null ? "unknown" : request.replayId();
    log.info("Mock replay replayId={} ids={}", replayId, ids);
    List<ReplayExternalResult> results =
        ids.stream()
            .map((id) -> new ReplayExternalResult(id, "REPLAYED", null, null, 1))
            .toList();
    ReplayExternalSummary summary =
        new ReplayExternalSummary(ids.size(), ids.size(), 0);
    return new ReplayExternalResponse(
        request == null ? null : request.requestId(), "completed", results, summary);
  }
}

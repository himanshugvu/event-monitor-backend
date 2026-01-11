# Event App Replay Endpoint Sample

This sample shows a minimal replay endpoint for the event app. It matches the contract used by the
event monitor backend and includes controller, DTOs, repository lookup by ID, and processing logic.

## Request and response shape

Request:

```json
{
  "ids": ["123", "456"],
  "requestId": "b5eaf1d1-6f4e-4e2e-bccf-27f7d9e0b7c2",
  "requestedBy": "event-monitor-service",
  "reason": "manual-replay",
  "dryRun": false
}
```

Response:

```json
{
  "requestId": "b5eaf1d1-6f4e-4e2e-bccf-27f7d9e0b7c2",
  "status": "completed",
  "results": [
    { "id": 123, "status": "REPLAYED", "emittedId": "evt-789", "error": null },
    { "id": 456, "status": "NOT_FOUND", "emittedId": null, "error": "event missing" }
  ],
  "summary": { "requested": 2, "replayed": 1, "failed": 1 }
}
```

## Sample implementation (single file)

```java
package com.yourapp.replay;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1")
public class ReplayController {

  private final ReplayService replayService;

  public ReplayController(ReplayService replayService) {
    this.replayService = replayService;
  }

  @PostMapping("/replay")
  public ReplayResponse replay(@RequestBody ReplayRequest request) {
    return replayService.replay(request);
  }

  @ResponseStatus(HttpStatus.BAD_REQUEST)
  @ExceptionHandler(IllegalArgumentException.class)
  public ErrorResponse handleBadRequest(IllegalArgumentException ex) {
    return new ErrorResponse("BAD_REQUEST", ex.getMessage());
  }

  public record ErrorResponse(String code, String message) {}

  // ===== DTOs (contract) =====

  public record ReplayRequest(
      List<Long> ids,
      String requestId,
      String requestedBy,
      String reason,
      Boolean dryRun) {}

  public record ReplayResponse(
      String requestId,
      String status,
      List<ReplayResult> results,
      ReplaySummary summary) {}

  public record ReplayResult(Long id, String status, String emittedId, String error) {}

  public record ReplaySummary(Integer requested, Integer replayed, Integer failed) {}

  // ===== Service =====

  @org.springframework.stereotype.Service
  public static class ReplayService {
    private static final int MAX_IDS = 50;

    private final ReplayRepository repository;
    private final ReplayProcessor processor;

    public ReplayService(ReplayRepository repository, ReplayProcessor processor) {
      this.repository = repository;
      this.processor = processor;
    }

    public ReplayResponse replay(ReplayRequest request) {
      if (request == null || request.ids() == null || request.ids().isEmpty()) {
        throw new IllegalArgumentException("ids is required.");
      }
      if (request.ids().size() > MAX_IDS) {
        throw new IllegalArgumentException("ids cannot exceed " + MAX_IDS + ".");
      }

      List<ReplayResult> results = new ArrayList<>();
      int replayed = 0;

      for (Long id : request.ids()) {
        if (id == null) {
          results.add(new ReplayResult(null, "FAILED", null, "id is null"));
          continue;
        }

        EventRow row = repository.findById(id);
        if (row == null) {
          results.add(new ReplayResult(id, "NOT_FOUND", null, "event missing"));
          continue;
        }

        try {
          if (Boolean.TRUE.equals(request.dryRun())) {
            results.add(new ReplayResult(id, "DRY_RUN", null, null));
            continue;
          }

          // 1) Convert to target payload (same logic as your normal flow)
          String targetPayload = processor.convert(row);

          // 2) Publish to downstream system
          String emittedId = processor.publish(row, targetPayload);

          // 3) Persist audit if needed (optional)
          processor.auditReplay(row, request, emittedId);

          results.add(new ReplayResult(id, "REPLAYED", emittedId, null));
          replayed++;
        } catch (Exception ex) {
          results.add(new ReplayResult(id, "FAILED", null, ex.getMessage()));
        }
      }

      int failed = results.size() - replayed;
      return new ReplayResponse(
          request.requestId(),
          "completed",
          results,
          new ReplaySummary(results.size(), replayed, failed));
    }
  }

  // ===== Repository =====

  @org.springframework.stereotype.Repository
  public static class ReplayRepository {
    private final JdbcTemplate jdbcTemplate;

    public ReplayRepository(JdbcTemplate jdbcTemplate) {
      this.jdbcTemplate = jdbcTemplate;
    }

    public EventRow findById(Long id) {
      String sql =
          "SELECT id, event_key, trace_id, message_key, payload, headers, created_at "
              + "FROM event_store WHERE id = ?";
      List<EventRow> rows =
          jdbcTemplate.query(
              sql,
              (rs, rowNum) ->
                  new EventRow(
                      rs.getLong("id"),
                      rs.getString("event_key"),
                      rs.getString("trace_id"),
                      rs.getString("message_key"),
                      rs.getString("payload"),
                      rs.getString("headers"),
                      rs.getTimestamp("created_at").toInstant()),
              id);
      return rows.isEmpty() ? null : rows.get(0);
    }
  }

  // ===== Domain =====

  public record EventRow(
      Long id,
      String eventKey,
      String traceId,
      String messageKey,
      String payload,
      String headers,
      Instant createdAt) {}

  // ===== Processor =====

  @org.springframework.stereotype.Component
  public static class ReplayProcessor {
    public String convert(EventRow row) {
      // transform raw payload -> target payload
      // replace with your actual transformer
      return row.payload();
    }

    public String publish(EventRow row, String payload) {
      // publish to downstream (Kafka/HTTP/etc.)
      // return emitted event id from downstream if available
      return "emitted-" + row.id();
    }

    public void auditReplay(EventRow row, ReplayRequest request, String emittedId) {
      // optional: write audit record
      Objects.requireNonNull(row);
    }
  }
}
```

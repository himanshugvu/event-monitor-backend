package com.vibe.events.controller;

import com.vibe.events.dto.EventBucketsResponse;
import com.vibe.events.dto.EventSummaryResponse;
import com.vibe.events.dto.PagedRowsResponse;
import com.vibe.events.service.AggregationService;
import com.vibe.events.service.RecordsService;
import com.vibe.events.util.DayValidator;
import java.time.LocalDate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/days/{day}/events/{eventKey}")
public class EventDetailsController {
  private final AggregationService aggregationService;
  private final RecordsService recordsService;

  public EventDetailsController(
      AggregationService aggregationService, RecordsService recordsService) {
    this.aggregationService = aggregationService;
    this.recordsService = recordsService;
  }

  @GetMapping("/summary")
  public EventSummaryResponse getSummary(@PathVariable String day, @PathVariable String eventKey) {
    LocalDate parsedDay = DayValidator.parseDay(day);
    return aggregationService.getEventSummary(parsedDay, eventKey);
  }

  @GetMapping("/buckets")
  public EventBucketsResponse getBuckets(
      @PathVariable String day,
      @PathVariable String eventKey,
      @RequestParam(defaultValue = "60") int intervalMinutes) {
    LocalDate parsedDay = DayValidator.parseDay(day);
    return aggregationService.getEventBuckets(parsedDay, eventKey, intervalMinutes);
  }

  @GetMapping("/success")
  public PagedRowsResponse getSuccessRows(
      @PathVariable String day,
      @PathVariable String eventKey,
      @RequestParam(required = false) Integer page,
      @RequestParam(required = false) Integer size,
      @RequestParam(required = false) String traceId,
      @RequestParam(required = false) String messageKey,
      @RequestParam(required = false) String accountNumber) {
    LocalDate parsedDay = DayValidator.parseDay(day);
    return recordsService.loadSuccessRows(
        parsedDay, eventKey, page, size, traceId, messageKey, accountNumber);
  }

  @GetMapping("/failures")
  public PagedRowsResponse getFailureRows(
      @PathVariable String day,
      @PathVariable String eventKey,
      @RequestParam(required = false) Integer page,
      @RequestParam(required = false) Integer size,
      @RequestParam(required = false) String traceId,
      @RequestParam(required = false) String messageKey,
      @RequestParam(required = false) String accountNumber,
      @RequestParam(required = false) String exceptionType,
      @RequestParam(required = false) Boolean retriable,
      @RequestParam(required = false) Integer retryAttemptMin,
      @RequestParam(required = false) Integer retryAttemptMax) {
    LocalDate parsedDay = DayValidator.parseDay(day);
    return recordsService.loadFailureRows(
        parsedDay,
        eventKey,
        page,
        size,
        traceId,
        messageKey,
        accountNumber,
        exceptionType,
        retriable,
        retryAttemptMin,
        retryAttemptMax);
  }
}

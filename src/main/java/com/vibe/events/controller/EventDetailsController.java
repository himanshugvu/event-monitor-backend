package com.vibe.events.controller;

import com.vibe.events.dto.EventBucketsResponse;
import com.vibe.events.dto.EventSummaryResponse;
import com.vibe.events.dto.PagedRowsResponse;
import com.vibe.events.service.AggregationService;
import com.vibe.events.service.RecordsService;
import com.vibe.events.util.DayValidator;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
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
  public EventSummaryResponse getSummary(
      @PathVariable String day,
      @PathVariable String eventKey,
      @RequestParam(defaultValue = "false") boolean refresh) {
    LocalDate parsedDay = DayValidator.parseDay(day);
    if (refresh) {
      aggregationService.refreshEventSummary(parsedDay, eventKey);
    }
    return aggregationService.getEventSummary(parsedDay, eventKey);
  }

  @GetMapping("/buckets")
  public EventBucketsResponse getBuckets(
      @PathVariable String day,
      @PathVariable String eventKey,
      @RequestParam(defaultValue = "60") int intervalMinutes,
      @RequestParam(defaultValue = "false") boolean refresh) {
    LocalDate parsedDay = DayValidator.parseDay(day);
    if (refresh) {
      aggregationService.refreshEventBuckets(parsedDay, eventKey, intervalMinutes);
    }
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
      @RequestParam(required = false) String accountNumber,
      @RequestParam(required = false) Long latencyMin,
      @RequestParam(required = false) Long latencyMax,
      @RequestParam(required = false) Long receivedLatencyMin,
      @RequestParam(required = false) Long receivedLatencyMax,
      @RequestParam(required = false) String fromDate,
      @RequestParam(required = false) String toDate,
      @RequestParam(required = false) String fromTime,
      @RequestParam(required = false) String toTime) {
    LocalDate parsedDay = DayValidator.parseDay(day);
    LocalDate parsedFrom = fromDate == null ? null : DayValidator.parseDay(fromDate);
    LocalDate parsedTo = toDate == null ? null : DayValidator.parseDay(toDate);
    LocalTime parsedFromTime = DayValidator.parseTime(fromTime);
    LocalTime parsedToTime = DayValidator.parseTime(toTime);
    return recordsService.loadSuccessRows(
        parsedDay,
        parsedFrom,
        parsedTo,
        parsedFromTime,
        parsedToTime,
        eventKey,
        page,
        size,
        traceId,
        messageKey,
        accountNumber,
        latencyMin,
        latencyMax,
        receivedLatencyMin,
        receivedLatencyMax);
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
      @RequestParam(required = false) Long latencyMin,
      @RequestParam(required = false) Long latencyMax,
      @RequestParam(required = false) Long receivedLatencyMin,
      @RequestParam(required = false) Long receivedLatencyMax,
      @RequestParam(required = false) String exceptionType,
      @RequestParam(required = false) Boolean retriable,
      @RequestParam(required = false) Integer retryAttemptMin,
      @RequestParam(required = false) Integer retryAttemptMax,
      @RequestParam(required = false) String fromDate,
      @RequestParam(required = false) String toDate,
      @RequestParam(required = false) String fromTime,
      @RequestParam(required = false) String toTime) {
    LocalDate parsedDay = DayValidator.parseDay(day);
    LocalDate parsedFrom = fromDate == null ? null : DayValidator.parseDay(fromDate);
    LocalDate parsedTo = toDate == null ? null : DayValidator.parseDay(toDate);
    LocalTime parsedFromTime = DayValidator.parseTime(fromTime);
    LocalTime parsedToTime = DayValidator.parseTime(toTime);
    return recordsService.loadFailureRows(
        parsedDay,
        parsedFrom,
        parsedTo,
        parsedFromTime,
        parsedToTime,
        eventKey,
        page,
        size,
        traceId,
        messageKey,
        accountNumber,
        latencyMin,
        latencyMax,
        receivedLatencyMin,
        receivedLatencyMax,
        exceptionType,
        retriable,
        retryAttemptMin,
        retryAttemptMax);
  }

  @GetMapping("/exception-types")
  public List<String> getExceptionTypes(
      @PathVariable String day,
      @PathVariable String eventKey,
      @RequestParam(required = false) String fromDate,
      @RequestParam(required = false) String toDate,
      @RequestParam(required = false) String fromTime,
      @RequestParam(required = false) String toTime) {
    LocalDate parsedDay = DayValidator.parseDay(day);
    LocalDate parsedFrom = fromDate == null ? null : DayValidator.parseDay(fromDate);
    LocalDate parsedTo = toDate == null ? null : DayValidator.parseDay(toDate);
    LocalTime parsedFromTime = DayValidator.parseTime(fromTime);
    LocalTime parsedToTime = DayValidator.parseTime(toTime);
    return recordsService.loadFailureExceptionTypes(
        parsedDay, parsedFrom, parsedTo, parsedFromTime, parsedToTime, eventKey);
  }
}

package com.vibe.events.controller;

import com.vibe.events.config.AggregationProperties;
import com.vibe.events.service.AggregationService;
import com.vibe.events.service.AggregationWarmupService;
import com.vibe.events.service.RefreshGuard;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/refresh")
public class AggregationRefreshController {
  private final AggregationWarmupService warmupService;
  private final AggregationService aggregationService;
  private final AggregationProperties properties;
  private final RefreshGuard refreshGuard;

  public AggregationRefreshController(
      AggregationWarmupService warmupService,
      AggregationService aggregationService,
      AggregationProperties properties,
      RefreshGuard refreshGuard) {
    this.warmupService = warmupService;
    this.aggregationService = aggregationService;
    this.properties = properties;
    this.refreshGuard = refreshGuard;
  }

  @PostMapping("/home")
  public ResponseEntity<Void> refreshHome(@RequestParam(required = false) Integer days) {
    if (!refreshGuard.tryStart()) {
      throw new ResponseStatusException(HttpStatus.CONFLICT, "Refresh already in progress");
    }
    CompletableFuture<Void> future;
    try {
      future = warmupService.refreshRecentDays(resolveDays(days));
    } catch (RuntimeException ex) {
      refreshGuard.finish();
      throw ex;
    }
    if (future != null) {
      future.whenComplete((result, error) -> refreshGuard.finish());
    } else {
      refreshGuard.finish();
    }
    return ResponseEntity.accepted().build();
  }

  @PostMapping("/events/{eventKey}")
  public ResponseEntity<Void> refreshEvent(
      @PathVariable String eventKey, @RequestParam(required = false) Integer days) {
    if (!refreshGuard.tryStart()) {
      throw new ResponseStatusException(HttpStatus.CONFLICT, "Refresh already in progress");
    }
    CompletableFuture<Void> future;
    try {
      future = aggregationService.refreshEventAcrossDays(eventKey, resolveDays(days));
    } catch (RuntimeException ex) {
      refreshGuard.finish();
      throw ex;
    }
    if (future != null) {
      future.whenComplete((result, error) -> refreshGuard.finish());
    } else {
      refreshGuard.finish();
    }
    return ResponseEntity.accepted().build();
  }

  private int resolveDays(Integer days) {
    if (days == null || days < 1) {
      return properties.getCache().getDaysToKeep();
    }
    return days;
  }
}

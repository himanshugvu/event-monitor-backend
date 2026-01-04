package com.vibe.events.controller;

import com.vibe.events.dto.HomeAggregationResponse;
import com.vibe.events.service.AggregationService;
import com.vibe.events.util.DayValidator;
import java.time.LocalDate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/days/{day}")
public class HomeAggregationController {
  private final AggregationService aggregationService;

  public HomeAggregationController(AggregationService aggregationService) {
    this.aggregationService = aggregationService;
  }

  @GetMapping("/home")
  public HomeAggregationResponse getHome(
      @PathVariable String day,
      @RequestParam(defaultValue = "false") boolean refresh) {
    LocalDate parsedDay = DayValidator.parseDay(day);
    if (refresh) {
      aggregationService.refreshHome(parsedDay);
    }
    return aggregationService.getHomeAggregation(parsedDay);
  }
}

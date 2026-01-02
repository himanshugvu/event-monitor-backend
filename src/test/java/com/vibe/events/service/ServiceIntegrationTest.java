package com.vibe.events.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.vibe.events.dto.BucketPoint;
import com.vibe.events.dto.EventBucketsResponse;
import com.vibe.events.dto.EventSummaryResponse;
import com.vibe.events.dto.HomeAggregationResponse;
import com.vibe.events.dto.PagedRowsResponse;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.jdbc.Sql.ExecutionPhase;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Sql(scripts = {"/schema-h2.sql", "/data-h2.sql"}, executionPhase = ExecutionPhase.BEFORE_TEST_METHOD)
class ServiceIntegrationTest {

  @Autowired
  private AggregationService aggregationService;

  @Autowired
  private RecordsService recordsService;

  @Test
  void homeAggregationReturnsTotals() {
    LocalDate day = LocalDate.now();
    HomeAggregationResponse response = aggregationService.getHomeAggregation(day);

    assertThat(response.kpis().total()).isEqualTo(6);
    assertThat(response.kpis().success()).isEqualTo(3);
    assertThat(response.kpis().failure()).isEqualTo(3);
    assertThat(response.kpis().retriableFailures()).isEqualTo(2);
    assertThat(response.events()).hasSize(2);
  }

  @Test
  void eventSummaryReturnsExpectedKpis() {
    LocalDate day = LocalDate.now();
    EventSummaryResponse response = aggregationService.getEventSummary(day, "payments.in");

    assertThat(response.kpis().total()).isEqualTo(3);
    assertThat(response.kpis().success()).isEqualTo(2);
    assertThat(response.kpis().failure()).isEqualTo(1);
    assertThat(response.kpis().retriableFailures()).isEqualTo(1);
    assertThat(response.kpis().successRate()).isCloseTo(66.67, within(0.01));
  }

  @Test
  void eventBucketsRollUpCounts() {
    LocalDate day = LocalDate.now();
    EventBucketsResponse response = aggregationService.getEventBuckets(day, "payments.in", 60);

    long success = response.buckets().stream().mapToLong(BucketPoint::success).sum();
    long failure = response.buckets().stream().mapToLong(BucketPoint::failure).sum();
    assertThat(success).isEqualTo(2);
    assertThat(failure).isEqualTo(1);
  }

  @Test
  void successRowsFilterByTraceId() {
    LocalDate day = LocalDate.now();
    PagedRowsResponse response =
        recordsService.loadSuccessRows(
            day, null, null, null, null, "payments.in", 0, 50, "pay-trace-2", null, null);

    assertThat(response.total()).isEqualTo(1);
    assertThat(response.rows()).hasSize(1);
    Map<String, Object> row = (Map<String, Object>) response.rows().get(0);
    assertThat(row.get("event_trace_id")).isEqualTo("pay-trace-2");
  }

  @Test
  void failureRowsFilterByExceptionType() {
    LocalDate day = LocalDate.now();
    PagedRowsResponse response =
        recordsService.loadFailureRows(
            day,
            null,
            null,
            null,
            null,
            "loans.in",
            0,
            50,
            null,
            null,
            null,
            "ValidationException",
            null,
            null,
            null);

    assertThat(response.total()).isEqualTo(1);
  }

  @Test
  void exceptionTypesReturnsDistinctValues() {
    LocalDate day = LocalDate.now();
    List<String> response =
        recordsService.loadFailureExceptionTypes(day, null, null, null, null, "loans.in");

    assertThat(response).containsExactly("TimeoutException", "ValidationException");
  }
}

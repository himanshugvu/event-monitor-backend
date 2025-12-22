# CODEX — Event Monitoring Backend (Java 21, Spring Boot 4, MariaDB, JdbcClient/JdbcTemplate, Caffeine Cache)

## 1) Objective
Build a Spring Boot 4 backend (Java 21) for a **day-based** event monitoring UI:

- **Home**: global aggregation across **all events** for a selected **day**.
- **Event Details**: select an event → show **aggregation on top** + tabs to switch **Success** / **Failures** row lists.
- Each event has **two physical tables**: `<event>_success` and `<event>_failure`.
- Use **JdbcClient** (preferred) or **JdbcTemplate** (no JPA) because row/POJO shape is dynamic.
- DB: **MariaDB** with **Docker Compose** for local testing.
- Cache aggregations **in Caffeine**, keep **last 7 days** cached, and **precompute** hourly (and optional 15m) buckets.

---

## 2) Key decisions

### 2.1 Dynamic rows, JSON payload shown as String
- List endpoints return each row as `Map<String, Object>` (dynamic schema).
- JSON payload columns (e.g., `source_payload`, `transformed_payload`) are returned as **String** because UI only displays JSON text.

> Implementation detail: while mapping a row, force these columns to `String` using `Objects.toString(value, null)`.

### 2.2 Day-based semantics
All aggregation endpoints are scoped by `{day}` (`YYYY-MM-DD`).  
Recommended approach:
- Add `day DATE NOT NULL` column to success/failure tables
- Filter by `WHERE day = :day`
- Index by `(day, event_received_timestamp)` and `(day, event_trace_id)`.

### 2.3 Table-per-event requires strict allowlist
Because table names cannot be bound as SQL parameters:
- Maintain an **EventRegistry allowlist**: `eventKey -> successTable, failureTable`
- Reject unknown eventKey with **404**
- Never accept table name from client input.

### 2.4 Caching strategy (Caffeine, last 7 days)
Problem: N events => 2N table scans for aggregation (success + failure).
Solution:
- Precompute aggregations into Caffeine cache.
- Keep last 7 days cached.
- Refresh today frequently (every minute).

**Important cache cardinality note**
- Start by caching **unfiltered** aggregations (day + event only).
- Avoid caching arbitrary filters (`accountNumber`, etc.) initially; compute those on-demand or use a small TTL and strict max size.

---

## 3) Dependencies & project setup

### 3.1 Dependencies
- Java 21
- Spring Boot 4.x
- `spring-boot-starter-web`
- `spring-boot-starter-jdbc`
- MariaDB driver (e.g., `org.mariadb.jdbc:mariadb-java-client`)
- `spring-boot-starter-cache`
- Caffeine (`com.github.ben-manes.caffeine:caffeine`)
- (Optional) `springdoc-openapi-starter-webmvc-ui`

### 3.2 Suggested package structure
```
com.vibe.events
  ├─ EventsApplication.java
  ├─ config/
  │   ├─ DataSourceConfig.java
  │   ├─ CacheConfig.java
  │   └─ SchedulerConfig.java
  ├─ registry/
  │   ├─ EventRegistry.java
  │   └─ EventDefinition.java
  ├─ controller/
  │   ├─ HomeAggregationController.java
  │   └─ EventDetailsController.java
  ├─ service/
  │   ├─ AggregationWarmupService.java
  │   ├─ AggregationService.java
  │   └─ RecordsService.java
  ├─ repo/
  │   ├─ AggregationRepository.java
  │   └─ RecordsRepository.java
  ├─ dto/
  │   ├─ HomeAggregationResponse.java
  │   ├─ EventSummaryResponse.java
  │   ├─ EventBucketsResponse.java
  │   ├─ BucketPoint.java
  │   ├─ EventBreakdownRow.java
  │   ├─ Kpis.java
  │   ├─ PagedRowsResponse.java
  │   └─ ErrorResponse.java
  └─ util/
      ├─ DayValidator.java
      ├─ CacheKeys.java
      └─ RowMapperUtil.java
```

---

## 4) Database expectations (tables per event)

### 4.1 Success table (minimum columns)
- `id` (PK)
- `day` (DATE) ✅ recommended
- `event_trace_id` (VARCHAR)
- `account_number` (VARCHAR)
- `customer_type` (VARCHAR)
- `event_received_timestamp` (DATETIME)
- `source_topic` (VARCHAR)
- `source_partition_id` (INT)
- `source_offset` (BIGINT)
- `message_key` (VARCHAR)
- `source_payload` (TEXT/JSON)
- `transformed_payload` (TEXT/JSON)
- `event_sent_timestamp` (DATETIME)
- `target_topic` (VARCHAR)
- `target_partition_id` (INT)
- `target_offset` (BIGINT)

### 4.2 Failure table (minimum columns)
- all shared columns (some target fields may be null)
- `exception_type` (VARCHAR)
- `exception_message` (TEXT)
- `exception_stack` (LONGTEXT)
- `retriable` (TINYINT/BOOLEAN)
- `retry_attempt` (INT)

### 4.3 Index recommendations
For each success/failure table:
- `INDEX idx_day_received (day, event_received_timestamp)`
- `INDEX idx_day_trace (day, event_trace_id)`
- optional: `INDEX idx_day_account (day, account_number)`
- optional: `INDEX idx_day_messagekey (day, message_key)`

---

## 5) REST API (v1)

### 5.1 Home: global aggregation across all events
**GET** `/api/v1/days/{day}/home`

Response shape:
```json
{
  "day": "2025-12-21",
  "generatedAt": "2025-12-21T10:11:12",
  "kpis": {
    "total": 1540,
    "success": 1502,
    "failure": 38,
    "successRate": 97.53,
    "retriableFailures": 21,
    "avgLatencyMs": 41.2
  },
  "events": [
    {
      "eventKey": "payments.in",
      "total": 1200,
      "success": 1180,
      "failure": 20,
      "successRate": 98.33,
      "retriableFailures": 12,
      "avgLatencyMs": 32.0
    }
  ]
}
```

### 5.2 Event summary: aggregation for a selected event
**GET** `/api/v1/days/{day}/events/{eventKey}/summary`

Response:
```json
{
  "day": "2025-12-21",
  "eventKey": "payments.in",
  "generatedAt": "2025-12-21T10:11:12",
  "kpis": { "...": "same KPI structure but scoped to this event" }
}
```

### 5.3 Event buckets: trend points for a selected event
**GET** `/api/v1/days/{day}/events/{eventKey}/buckets?intervalMinutes=60`

- `intervalMinutes`: allowed values `60` (hourly) and optional `15`.

Response:
```json
{
  "day": "2025-12-21",
  "eventKey": "payments.in",
  "intervalMinutes": 60,
  "generatedAt": "2025-12-21T10:11:12",
  "buckets": [
    {
      "bucketStart": "2025-12-21T00:00:00",
      "success": 120,
      "failure": 2,
      "total": 122,
      "successRate": 98.36,
      "retriableFailures": 1,
      "avgLatencyMs": 35.3
    }
  ]
}
```

### 5.4 Success rows (dynamic maps, paginated)
**GET** `/api/v1/days/{day}/events/{eventKey}/success?page=0&size=50&traceId=...`

Query params:
- `page` (0..), `size` (default 50; enforce max 200)
- optional: `traceId`, `messageKey`, `accountNumber`

Response:
```json
{
  "page": 0,
  "size": 50,
  "rows": [ { "id": 1, "event_trace_id": "...", "source_payload": "{...}" } ]
}
```

### 5.5 Failure rows (dynamic maps, paginated)
**GET** `/api/v1/days/{day}/events/{eventKey}/failures?page=0&size=50`

Query params:
- `page`, `size`
- optional: `traceId`, `messageKey`, `accountNumber`, `exceptionType`, `retriable`, `retryAttemptMin`, `retryAttemptMax`

Response:
```json
{
  "page": 0,
  "size": 50,
  "rows": [ { "id": 11, "exception_type": "...", "exception_stack": "..." } ]
}
```

> Aggregation endpoints should return cached/precomputed results; list endpoints query raw tables directly.

---

## 6) SQL templates (MariaDB)

> NOTE: `<SUCCESS_TABLE>` and `<FAILURE_TABLE>` are resolved from EventRegistry (allowlist). Do NOT accept arbitrary table names.

### 6.1 Success day totals + average latency
```sql
SELECT
  COUNT(*) AS success_count,
  AVG(TIMESTAMPDIFF(MICROSECOND, event_received_timestamp, event_sent_timestamp)) / 1000 AS avg_latency_ms
FROM <SUCCESS_TABLE>
WHERE day = :day;
```

### 6.2 Failure day totals + retriable failures  ✅
```sql
SELECT
  COUNT(*) AS failure_count,
  SUM(CASE WHEN retriable = 1 THEN 1 ELSE 0 END) AS retriable_count
FROM <FAILURE_TABLE>
WHERE day = :day;
```

### 6.3 Hourly buckets — success
```sql
SELECT
  HOUR(event_received_timestamp) AS hour_of_day,
  COUNT(*) AS success_count,
  AVG(TIMESTAMPDIFF(MICROSECOND, event_received_timestamp, event_sent_timestamp)) / 1000 AS avg_latency_ms
FROM <SUCCESS_TABLE>
WHERE day = :day
GROUP BY HOUR(event_received_timestamp)
ORDER BY hour_of_day;
```

### 6.4 Hourly buckets — failures
```sql
SELECT
  HOUR(event_received_timestamp) AS hour_of_day,
  COUNT(*) AS failure_count,
  SUM(CASE WHEN retriable = 1 THEN 1 ELSE 0 END) AS retriable_count
FROM <FAILURE_TABLE>
WHERE day = :day
GROUP BY HOUR(event_received_timestamp)
ORDER BY hour_of_day;
```

### 6.5 15-minute buckets (optional)
Compute bucket identity in SQL; compute bucketStart in Java:
```sql
SELECT
  HOUR(event_received_timestamp) AS hour_of_day,
  FLOOR(MINUTE(event_received_timestamp) / 15) AS quarter,
  COUNT(*) AS success_count,
  AVG(TIMESTAMPDIFF(MICROSECOND, event_received_timestamp, event_sent_timestamp)) / 1000 AS avg_latency_ms
FROM <SUCCESS_TABLE>
WHERE day = :day
GROUP BY HOUR(event_received_timestamp), FLOOR(MINUTE(event_received_timestamp) / 15)
ORDER BY hour_of_day, quarter;
```

---

## 7) JdbcClient / JdbcTemplate usage

### 7.1 Prefer JdbcClient
- Inject `JdbcClient` in repositories.
- Use named parameters for values, but table name must be embedded after registry validation.

### 7.2 Map row mapper utility
Implement `RowMapperUtil` that:
- Reads `ResultSetMetaData`
- Builds `LinkedHashMap<String,Object>` for each row
- Converts payload columns to String:
  - if column name in `{source_payload, transformed_payload}` → `Objects.toString(val, null)`

---

## 8) EventRegistry (allowlist)

### 8.1 Configuration-based registry (initial)
`application.yml`:
```yaml
events:
  registry:
    - key: payments.in
      successTable: payments_in_success
      failureTable: payments_in_failure
    - key: loans.in
      successTable: loans_in_success
      failureTable: loans_in_failure
```

### 8.2 Registry API (internal)
- `List<EventDefinition> all()`
- `EventDefinition getRequired(eventKey)` → throws NotFound if missing
- `String successTable(eventKey)`
- `String failureTable(eventKey)`

---

## 9) Caffeine caching (7 days) + precomputation

### 9.1 Caches & keys
Create 3 caches:

1) `homeAgg`
- key: `day`
- value: `HomeAggregationResponse`

2) `eventSummary`
- key: `day|eventKey`
- value: `EventSummaryResponse`

3) `eventBuckets`
- key: `day|eventKey|intervalMinutes`
- value: `EventBucketsResponse`

### 9.2 Expiry, size, and memory
- `expireAfterWrite = 7 days`
- `maximumSize` (tune per environment):
  - `homeAgg`: 16–32
  - `eventSummary`: `eventsCount * 7` (e.g., 20 * 7 = 140)
  - `eventBuckets`: `eventsCount * 7 * intervalsCount` (e.g., 20 * 7 * 2 = 280)

> Keep values compact: store only counts/averages and bucket arrays; avoid storing raw rows in cache.

### 9.3 Read path
- Controllers call service
- Service checks cache first:
  - if present → return
  - else → compute + put (or let warmup fill)

### 9.4 Precompute / warmup logic
Maintain cache for last 7 days:
- Days: `today, today-1, ..., today-6`

Warmup phases:
1) **Startup warmup**
- compute all caches for last 7 days (or at least today + yesterday)
2) **Scheduled refresh**
- every **1 minute**: recompute **today** for:
  - `homeAgg(today)`
  - all `eventSummary(today, eventKey)`
  - all `eventBuckets(today, eventKey, intervalMinutes)`
- optional: every **1 hour** recompute yesterday (late events)

### 9.5 Filtered aggregation (later)
If you add filters (account/customer) to aggregation:
- DO NOT put them into the 7-day caches initially (explodes cache keys).
- Options later:
  - compute on-demand, no cache
  - small TTL cache (e.g., 60s) with strict max size
  - or move to persistent snapshot tables if needed

---

## 10) Services and responsibilities

### 10.1 AggregationRepository
- Runs SQL for:
  - success totals
  - failure totals
  - success buckets
  - failure buckets

### 10.2 AggregationService
- Combines success + failure results into:
  - `Kpis` (total, success, failure, successRate, retriableFailures, avgLatencyMs)
  - `EventBreakdownRow` per event
- Creates bucket points (hourly or 15m) by merging success and failure buckets.

### 10.3 AggregationWarmupService
- Computes and stores results in caches based on schedule rules.

### 10.4 RecordsRepository / RecordsService
- Queries raw success/failure tables with pagination and optional filters.
- Returns list of `Map<String,Object>` rows.

---

## 11) Validation, safety, observability

### 11.1 Validation
- Validate `{day}` strictly (regex `^\d{4}-\d{2}-\d{2}$`) + parse as `LocalDate`.
- Validate `intervalMinutes` ∈ {60, 15}.
- Enforce maximum page size (e.g., 200).
- Validate `eventKey` exists in registry.

### 11.2 Safety
- Table names are only sourced from registry.
- Never concatenate user-provided strings into SQL except known-safe constants.

### 11.3 Observability
- Log aggregation compute duration by:
  - day
  - eventKey
  - intervalMinutes
- Add metrics (optional):
  - cache hit/miss
  - aggregation compute time
  - query latency

---

## 12) Docker Compose (MariaDB + Adminer)

### 12.1 docker-compose.yml
```yaml
services:
  mariadb:
    image: mariadb:11
    container_name: vibe-mariadb
    ports:
      - "3306:3306"
    environment:
      MARIADB_ROOT_PASSWORD: rootpass
      MARIADB_DATABASE: eventsdb
      MARIADB_USER: appuser
      MARIADB_PASSWORD: apppass
    volumes:
      - mariadb_data:/var/lib/mysql
      - ./db/init:/docker-entrypoint-initdb.d

  adminer:
    image: adminer:latest
    container_name: vibe-adminer
    ports:
      - "8081:8080"
    depends_on:
      - mariadb

volumes:
  mariadb_data:
```

### 12.2 DB init scripts
Create directory: `./db/init/`

- `001_schema.sql`
  - create sample success/failure tables for 1–2 events
  - include `day` column
  - create indexes

- `002_seed.sql`
  - insert sample rows for today and yesterday (some success + some failure)

---

## 13) Spring configuration

### 13.1 application.yml (minimal)
```yaml
server:
  port: 8080

spring:
  datasource:
    url: jdbc:mariadb://localhost:3306/eventsdb
    username: appuser
    password: apppass
    driver-class-name: org.mariadb.jdbc.Driver

  cache:
    type: caffeine

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics

aggregation:
  cache:
    daysToKeep: 7
  buckets:
    intervalsMinutes: [60, 15]  # use [60] for hourly only
```

---

## 14) Deliverables checklist
- [ ] Spring Boot 4 app (Java 21) with Web + JDBC + Cache + Caffeine
- [ ] MariaDB docker-compose + init scripts + sample seed data
- [ ] EventRegistry allowlist (config-driven)
- [ ] JdbcClient repositories for aggregation + list queries
- [ ] AggregationWarmupService (startup warmup + scheduled refresh)
- [ ] Controllers for:
  - home aggregation
  - event summary
  - event buckets
  - success/failure paged rows
- [ ] Day validation + eventKey validation + interval validation
- [ ] Bounded cache sizes + 7-day expiry

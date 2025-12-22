# Event Monitoring API

Base URL: `/api/v1`

## Conventions
- `day` format: `YYYY-MM-DD`
- `eventKey`: must exist in the registry allowlist
- Pagination: `page` (0-based), `size` (default 50, max 200)
- Aggregation endpoints are cached; list endpoints query raw tables

## Error Response
```json
{
  "timestamp": "2025-12-21T12:00:00",
  "status": 400,
  "error": "Bad Request",
  "message": "Invalid day format, expected YYYY-MM-DD.",
  "path": "/api/v1/days/2025-12-XX/home"
}
```

## Endpoints

## Try It With Curl

```bash
# Set day and event
DAY=$(date +%F)
EVENT=payments.in

# Home aggregation
curl -s "http://localhost:8080/api/v1/days/$DAY/home"

# Event summary
curl -s "http://localhost:8080/api/v1/days/$DAY/events/$EVENT/summary"

# Buckets (hourly)
curl -s "http://localhost:8080/api/v1/days/$DAY/events/$EVENT/buckets?intervalMinutes=60"

# Buckets (15-min)
curl -s "http://localhost:8080/api/v1/days/$DAY/events/$EVENT/buckets?intervalMinutes=15"

# Success rows (paged)
curl -s "http://localhost:8080/api/v1/days/$DAY/events/$EVENT/success?page=0&size=50"

# Failure rows (paged)
curl -s "http://localhost:8080/api/v1/days/$DAY/events/$EVENT/failures?page=0&size=50"

# Success filter
curl -s "http://localhost:8080/api/v1/days/$DAY/events/$EVENT/success?traceId=trace-payments-000001"

# Failure filter
curl -s "http://localhost:8080/api/v1/days/$DAY/events/$EVENT/failures?exceptionType=TimeoutException&retriable=true&retryAttemptMin=0&retryAttemptMax=1"
```

### Home Aggregation
`GET /api/v1/days/{day}/home`

Response:
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

### Event Summary
`GET /api/v1/days/{day}/events/{eventKey}/summary`

Response:
```json
{
  "day": "2025-12-21",
  "eventKey": "payments.in",
  "generatedAt": "2025-12-21T10:11:12",
  "kpis": {
    "total": 1200,
    "success": 1180,
    "failure": 20,
    "successRate": 98.33,
    "retriableFailures": 12,
    "avgLatencyMs": 32.0
  }
}
```

### Event Buckets
`GET /api/v1/days/{day}/events/{eventKey}/buckets?intervalMinutes=60`

Query params:
- `intervalMinutes`: allowed values `60` or `15`

Response:
```json
{
  "day": "2025-12-21",
  "eventKey": "payments.in",
  "intervalMinutes": 60,
  "generatedAt": "2025-12-21T10:11:12",
  "buckets": [
    {
      "bucketStart": "2025-12-21T08:00:00",
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

### Success Rows
`GET /api/v1/days/{day}/events/{eventKey}/success`

Query params:
- `page` (optional)
- `size` (optional)
- `traceId` (optional)
- `messageKey` (optional)
- `accountNumber` (optional)

Response:
```json
{
  "page": 0,
  "size": 50,
  "rows": [
    {
      "id": 1,
      "event_trace_id": "trace-001",
      "source_payload": "{\"amount\":100}"
    }
  ]
}
```

### Failure Rows
`GET /api/v1/days/{day}/events/{eventKey}/failures`

Query params:
- `page` (optional)
- `size` (optional)
- `traceId` (optional)
- `messageKey` (optional)
- `accountNumber` (optional)
- `exceptionType` (optional)
- `retriable` (optional, `true` or `false`)
- `retryAttemptMin` (optional)
- `retryAttemptMax` (optional)

Response:
```json
{
  "page": 0,
  "size": 50,
  "rows": [
    {
      "id": 11,
      "exception_type": "IllegalStateException",
      "exception_message": "Invalid state",
      "exception_stack": "stack..."
    }
  ]
}
```

USE eventsdb;
SET SESSION FOREIGN_KEY_CHECKS = 0;
SET SESSION UNIQUE_CHECKS = 0;

DROP TEMPORARY TABLE IF EXISTS seq;
DROP TEMPORARY TABLE IF EXISTS days;
CREATE TEMPORARY TABLE seq (n INT PRIMARY KEY);
INSERT INTO seq (n)
SELECT a.n + b.n * 10 + c.n * 100 + d.n * 1000 + e.n * 10000 + f.n * 100000
FROM (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c
CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d
CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) e
CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) f;

CREATE TEMPORARY TABLE days (day_start DATETIME);
INSERT INTO days (day_start) VALUES (CAST(CURDATE() AS DATETIME)), (CAST(DATE_SUB(CURDATE(), INTERVAL 1 DAY) AS DATETIME));

INSERT INTO payments_in_success (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  latency_event_sent_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CONCAT('trace-payments-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'payments.raw',
  (n % 10),
  n,
  CONCAT('payments-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),
  1000,
  250,
  750,
  'payments.processed',
  (n % 10),
  n
FROM seq CROSS JOIN days;

INSERT INTO payments_in_failure (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  target_topic,
  target_partition_id,
  target_offset,
  exception_type,
  exception_message,
  exception_stack,
  retriable,
  retry_attempt
)
SELECT
  CONCAT('trace-payments-fail-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'payments.raw',
  (n % 10),
  n,
  CONCAT('payments-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),
  1200,
  300,
  'payments.processed',
  (n % 10),
  n,
  CASE
    WHEN n % 4 = 0 THEN 'TimeoutException'
    WHEN n % 4 = 1 THEN 'IllegalStateException'
    WHEN n % 4 = 2 THEN 'ValidationException'
    ELSE 'IOException'
  END,
  CONCAT('Failure ', n),
  CONCAT('stack-', n),
  CASE WHEN n % 5 = 0 THEN 1 ELSE 0 END,
  (n % 3)
FROM seq CROSS JOIN days;

INSERT INTO loans_in_success (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  latency_event_sent_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CONCAT('trace-loans-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'loans.raw',
  (n % 10),
  n,
  CONCAT('loans-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),
  1000,
  250,
  750,
  'loans.processed',
  (n % 10),
  n
FROM seq CROSS JOIN days;

INSERT INTO loans_in_failure (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  target_topic,
  target_partition_id,
  target_offset,
  exception_type,
  exception_message,
  exception_stack,
  retriable,
  retry_attempt
)
SELECT
  CONCAT('trace-loans-fail-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'loans.raw',
  (n % 10),
  n,
  CONCAT('loans-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),
  1200,
  300,
  'loans.processed',
  (n % 10),
  n,
  CASE
    WHEN n % 4 = 0 THEN 'TimeoutException'
    WHEN n % 4 = 1 THEN 'IllegalStateException'
    WHEN n % 4 = 2 THEN 'ValidationException'
    ELSE 'IOException'
  END,
  CONCAT('Failure ', n),
  CONCAT('stack-', n),
  CASE WHEN n % 5 = 0 THEN 1 ELSE 0 END,
  (n % 3)
FROM seq CROSS JOIN days;

INSERT INTO cards_in_success (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  latency_event_sent_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CONCAT('trace-cards-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'cards.raw',
  (n % 10),
  n,
  CONCAT('cards-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),
  1000,
  250,
  750,
  'cards.processed',
  (n % 10),
  n
FROM seq CROSS JOIN days;

INSERT INTO cards_in_failure (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  target_topic,
  target_partition_id,
  target_offset,
  exception_type,
  exception_message,
  exception_stack,
  retriable,
  retry_attempt
)
SELECT
  CONCAT('trace-cards-fail-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'cards.raw',
  (n % 10),
  n,
  CONCAT('cards-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),
  1200,
  300,
  'cards.processed',
  (n % 10),
  n,
  CASE
    WHEN n % 4 = 0 THEN 'TimeoutException'
    WHEN n % 4 = 1 THEN 'IllegalStateException'
    WHEN n % 4 = 2 THEN 'ValidationException'
    ELSE 'IOException'
  END,
  CONCAT('Failure ', n),
  CONCAT('stack-', n),
  CASE WHEN n % 5 = 0 THEN 1 ELSE 0 END,
  (n % 3)
FROM seq CROSS JOIN days;

INSERT INTO accounts_in_success (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  latency_event_sent_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CONCAT('trace-accounts-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'accounts.raw',
  (n % 10),
  n,
  CONCAT('accounts-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),
  1000,
  250,
  750,
  'accounts.processed',
  (n % 10),
  n
FROM seq CROSS JOIN days;

INSERT INTO accounts_in_failure (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  target_topic,
  target_partition_id,
  target_offset,
  exception_type,
  exception_message,
  exception_stack,
  retriable,
  retry_attempt
)
SELECT
  CONCAT('trace-accounts-fail-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'accounts.raw',
  (n % 10),
  n,
  CONCAT('accounts-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),
  1200,
  300,
  'accounts.processed',
  (n % 10),
  n,
  CASE
    WHEN n % 4 = 0 THEN 'TimeoutException'
    WHEN n % 4 = 1 THEN 'IllegalStateException'
    WHEN n % 4 = 2 THEN 'ValidationException'
    ELSE 'IOException'
  END,
  CONCAT('Failure ', n),
  CONCAT('stack-', n),
  CASE WHEN n % 5 = 0 THEN 1 ELSE 0 END,
  (n % 3)
FROM seq CROSS JOIN days;

INSERT INTO transfers_in_success (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  latency_event_sent_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CONCAT('trace-transfers-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'transfers.raw',
  (n % 10),
  n,
  CONCAT('transfers-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),
  1000,
  250,
  750,
  'transfers.processed',
  (n % 10),
  n
FROM seq CROSS JOIN days;

INSERT INTO transfers_in_failure (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  target_topic,
  target_partition_id,
  target_offset,
  exception_type,
  exception_message,
  exception_stack,
  retriable,
  retry_attempt
)
SELECT
  CONCAT('trace-transfers-fail-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'transfers.raw',
  (n % 10),
  n,
  CONCAT('transfers-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),
  1200,
  300,
  'transfers.processed',
  (n % 10),
  n,
  CASE
    WHEN n % 4 = 0 THEN 'TimeoutException'
    WHEN n % 4 = 1 THEN 'IllegalStateException'
    WHEN n % 4 = 2 THEN 'ValidationException'
    ELSE 'IOException'
  END,
  CONCAT('Failure ', n),
  CONCAT('stack-', n),
  CASE WHEN n % 5 = 0 THEN 1 ELSE 0 END,
  (n % 3)
FROM seq CROSS JOIN days;

INSERT INTO alerts_in_success (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  latency_event_sent_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CONCAT('trace-alerts-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'alerts.raw',
  (n % 10),
  n,
  CONCAT('alerts-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),
  1000,
  250,
  750,
  'alerts.processed',
  (n % 10),
  n
FROM seq CROSS JOIN days;

INSERT INTO alerts_in_failure (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  target_topic,
  target_partition_id,
  target_offset,
  exception_type,
  exception_message,
  exception_stack,
  retriable,
  retry_attempt
)
SELECT
  CONCAT('trace-alerts-fail-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'alerts.raw',
  (n % 10),
  n,
  CONCAT('alerts-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),
  1200,
  300,
  'alerts.processed',
  (n % 10),
  n,
  CASE
    WHEN n % 4 = 0 THEN 'TimeoutException'
    WHEN n % 4 = 1 THEN 'IllegalStateException'
    WHEN n % 4 = 2 THEN 'ValidationException'
    ELSE 'IOException'
  END,
  CONCAT('Failure ', n),
  CONCAT('stack-', n),
  CASE WHEN n % 5 = 0 THEN 1 ELSE 0 END,
  (n % 3)
FROM seq CROSS JOIN days;

INSERT INTO kyc_in_success (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  latency_event_sent_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CONCAT('trace-kyc-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'kyc.raw',
  (n % 10),
  n,
  CONCAT('kyc-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),
  1000,
  250,
  750,
  'kyc.processed',
  (n % 10),
  n
FROM seq CROSS JOIN days;

INSERT INTO kyc_in_failure (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  target_topic,
  target_partition_id,
  target_offset,
  exception_type,
  exception_message,
  exception_stack,
  retriable,
  retry_attempt
)
SELECT
  CONCAT('trace-kyc-fail-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'kyc.raw',
  (n % 10),
  n,
  CONCAT('kyc-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),
  1200,
  300,
  'kyc.processed',
  (n % 10),
  n,
  CASE
    WHEN n % 4 = 0 THEN 'TimeoutException'
    WHEN n % 4 = 1 THEN 'IllegalStateException'
    WHEN n % 4 = 2 THEN 'ValidationException'
    ELSE 'IOException'
  END,
  CONCAT('Failure ', n),
  CONCAT('stack-', n),
  CASE WHEN n % 5 = 0 THEN 1 ELSE 0 END,
  (n % 3)
FROM seq CROSS JOIN days;

INSERT INTO fraud_in_success (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  latency_event_sent_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CONCAT('trace-fraud-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'fraud.raw',
  (n % 10),
  n,
  CONCAT('fraud-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),
  1000,
  250,
  750,
  'fraud.processed',
  (n % 10),
  n
FROM seq CROSS JOIN days;

INSERT INTO fraud_in_failure (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  target_topic,
  target_partition_id,
  target_offset,
  exception_type,
  exception_message,
  exception_stack,
  retriable,
  retry_attempt
)
SELECT
  CONCAT('trace-fraud-fail-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'fraud.raw',
  (n % 10),
  n,
  CONCAT('fraud-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),
  1200,
  300,
  'fraud.processed',
  (n % 10),
  n,
  CASE
    WHEN n % 4 = 0 THEN 'TimeoutException'
    WHEN n % 4 = 1 THEN 'IllegalStateException'
    WHEN n % 4 = 2 THEN 'ValidationException'
    ELSE 'IOException'
  END,
  CONCAT('Failure ', n),
  CONCAT('stack-', n),
  CASE WHEN n % 5 = 0 THEN 1 ELSE 0 END,
  (n % 3)
FROM seq CROSS JOIN days;

INSERT INTO statements_in_success (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  latency_event_sent_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CONCAT('trace-statements-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'statements.raw',
  (n % 10),
  n,
  CONCAT('statements-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),
  1000,
  250,
  750,
  'statements.processed',
  (n % 10),
  n
FROM seq CROSS JOIN days;

INSERT INTO statements_in_failure (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  target_topic,
  target_partition_id,
  target_offset,
  exception_type,
  exception_message,
  exception_stack,
  retriable,
  retry_attempt
)
SELECT
  CONCAT('trace-statements-fail-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'statements.raw',
  (n % 10),
  n,
  CONCAT('statements-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),
  1200,
  300,
  'statements.processed',
  (n % 10),
  n,
  CASE
    WHEN n % 4 = 0 THEN 'TimeoutException'
    WHEN n % 4 = 1 THEN 'IllegalStateException'
    WHEN n % 4 = 2 THEN 'ValidationException'
    ELSE 'IOException'
  END,
  CONCAT('Failure ', n),
  CONCAT('stack-', n),
  CASE WHEN n % 5 = 0 THEN 1 ELSE 0 END,
  (n % 3)
FROM seq CROSS JOIN days;

INSERT INTO limits_in_success (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  latency_event_sent_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CONCAT('trace-limits-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'limits.raw',
  (n % 10),
  n,
  CONCAT('limits-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),
  1000,
  250,
  750,
  'limits.processed',
  (n % 10),
  n
FROM seq CROSS JOIN days;

INSERT INTO limits_in_failure (
  event_trace_id,
  account_number,
  customer_type,
  event_datetime,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,
  latency_ms,
  latency_event_received_ms,
  target_topic,
  target_partition_id,
  target_offset,
  exception_type,
  exception_message,
  exception_stack,
  retriable,
  retry_attempt
)
SELECT
  CONCAT('trace-limits-fail-', DATE_FORMAT(day_start, '%Y%m%d'), '-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), day_start),
  'limits.raw',
  (n % 10),
  n,
  CONCAT('limits-key-', DATE_FORMAT(day_start, '%Y%m%d'), '-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),
  1200,
  300,
  'limits.processed',
  (n % 10),
  n,
  CASE
    WHEN n % 4 = 0 THEN 'TimeoutException'
    WHEN n % 4 = 1 THEN 'IllegalStateException'
    WHEN n % 4 = 2 THEN 'ValidationException'
    ELSE 'IOException'
  END,
  CONCAT('Failure ', n),
  CONCAT('stack-', n),
  CASE WHEN n % 5 = 0 THEN 1 ELSE 0 END,
  (n % 3)
FROM seq CROSS JOIN days;

SET SESSION FOREIGN_KEY_CHECKS = 1;
SET SESSION UNIQUE_CHECKS = 1;

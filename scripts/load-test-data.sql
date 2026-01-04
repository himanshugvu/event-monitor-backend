USE eventsdb;

SET SESSION FOREIGN_KEY_CHECKS = 0;
SET SESSION UNIQUE_CHECKS = 0;

CREATE TABLE IF NOT EXISTS cards_in_success LIKE payments_in_success;
CREATE TABLE IF NOT EXISTS cards_in_failure LIKE payments_in_failure;
CREATE TABLE IF NOT EXISTS accounts_in_success LIKE payments_in_success;
CREATE TABLE IF NOT EXISTS accounts_in_failure LIKE payments_in_failure;
CREATE TABLE IF NOT EXISTS transfers_in_success LIKE payments_in_success;
CREATE TABLE IF NOT EXISTS transfers_in_failure LIKE payments_in_failure;
CREATE TABLE IF NOT EXISTS alerts_in_success LIKE payments_in_success;
CREATE TABLE IF NOT EXISTS alerts_in_failure LIKE payments_in_failure;
CREATE TABLE IF NOT EXISTS kyc_in_success LIKE payments_in_success;
CREATE TABLE IF NOT EXISTS kyc_in_failure LIKE payments_in_failure;
CREATE TABLE IF NOT EXISTS fraud_in_success LIKE payments_in_success;
CREATE TABLE IF NOT EXISTS fraud_in_failure LIKE payments_in_failure;
CREATE TABLE IF NOT EXISTS statements_in_success LIKE payments_in_success;
CREATE TABLE IF NOT EXISTS statements_in_failure LIKE payments_in_failure;
CREATE TABLE IF NOT EXISTS limits_in_success LIKE payments_in_success;
CREATE TABLE IF NOT EXISTS limits_in_failure LIKE payments_in_failure;

TRUNCATE TABLE payments_in_success;
TRUNCATE TABLE payments_in_failure;
TRUNCATE TABLE loans_in_success;
TRUNCATE TABLE loans_in_failure;
TRUNCATE TABLE cards_in_success;
TRUNCATE TABLE cards_in_failure;
TRUNCATE TABLE accounts_in_success;
TRUNCATE TABLE accounts_in_failure;
TRUNCATE TABLE transfers_in_success;
TRUNCATE TABLE transfers_in_failure;
TRUNCATE TABLE alerts_in_success;
TRUNCATE TABLE alerts_in_failure;
TRUNCATE TABLE kyc_in_success;
TRUNCATE TABLE kyc_in_failure;
TRUNCATE TABLE fraud_in_success;
TRUNCATE TABLE fraud_in_failure;
TRUNCATE TABLE statements_in_success;
TRUNCATE TABLE statements_in_failure;
TRUNCATE TABLE limits_in_success;
TRUNCATE TABLE limits_in_failure;

CREATE TEMPORARY TABLE seq (n INT PRIMARY KEY);
INSERT INTO seq (n)
SELECT a.n + b.n * 10 + c.n * 100 + d.n * 1000 + e.n * 10000 + f.n * 100000
FROM (
  SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
  UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
) a
CROSS JOIN (
  SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
  UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
) b
CROSS JOIN (
  SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
  UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
) c
CROSS JOIN (
  SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
  UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
) d
CROSS JOIN (
  SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
  UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
) e
CROSS JOIN (
  SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
  UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
) f;

INSERT INTO payments_in_success (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CURDATE(),
  CONCAT('trace-payments-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'payments.raw',
  (n % 10),
  n,
  CONCAT('payments-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),  1000,
  'payments.processed',
  (n % 10),
  n
FROM seq;

INSERT INTO payments_in_failure (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
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
  CURDATE(),
  CONCAT('trace-payments-fail-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'payments.raw',
  (n % 10),
  n,
  CONCAT('payments-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),  1000,
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
FROM seq;

INSERT INTO loans_in_success (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CURDATE(),
  CONCAT('trace-loans-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'loans.raw',
  (n % 10),
  n,
  CONCAT('loans-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),  1000,
  'loans.processed',
  (n % 10),
  n
FROM seq;

INSERT INTO loans_in_failure (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
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
  CURDATE(),
  CONCAT('trace-loans-fail-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'loans.raw',
  (n % 10),
  n,
  CONCAT('loans-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),  1000,
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
FROM seq;

INSERT INTO cards_in_success (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CURDATE(),
  CONCAT('trace-cards-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'cards.raw',
  (n % 10),
  n,
  CONCAT('cards-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),  1000,
  'cards.processed',
  (n % 10),
  n
FROM seq;

INSERT INTO cards_in_failure (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
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
  CURDATE(),
  CONCAT('trace-cards-fail-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'cards.raw',
  (n % 10),
  n,
  CONCAT('cards-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),  1000,
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
FROM seq;

INSERT INTO accounts_in_success (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CURDATE(),
  CONCAT('trace-accounts-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'accounts.raw',
  (n % 10),
  n,
  CONCAT('accounts-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),  1000,
  'accounts.processed',
  (n % 10),
  n
FROM seq;

INSERT INTO accounts_in_failure (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
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
  CURDATE(),
  CONCAT('trace-accounts-fail-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'accounts.raw',
  (n % 10),
  n,
  CONCAT('accounts-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),  1000,
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
FROM seq;

INSERT INTO transfers_in_success (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CURDATE(),
  CONCAT('trace-transfers-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'transfers.raw',
  (n % 10),
  n,
  CONCAT('transfers-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),  1000,
  'transfers.processed',
  (n % 10),
  n
FROM seq;

INSERT INTO transfers_in_failure (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
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
  CURDATE(),
  CONCAT('trace-transfers-fail-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'transfers.raw',
  (n % 10),
  n,
  CONCAT('transfers-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),  1000,
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
FROM seq;

INSERT INTO alerts_in_success (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CURDATE(),
  CONCAT('trace-alerts-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'alerts.raw',
  (n % 10),
  n,
  CONCAT('alerts-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),  1000,
  'alerts.processed',
  (n % 10),
  n
FROM seq;

INSERT INTO alerts_in_failure (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
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
  CURDATE(),
  CONCAT('trace-alerts-fail-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'alerts.raw',
  (n % 10),
  n,
  CONCAT('alerts-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),  1000,
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
FROM seq;

INSERT INTO kyc_in_success (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CURDATE(),
  CONCAT('trace-kyc-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'kyc.raw',
  (n % 10),
  n,
  CONCAT('kyc-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),  1000,
  'kyc.processed',
  (n % 10),
  n
FROM seq;

INSERT INTO kyc_in_failure (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
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
  CURDATE(),
  CONCAT('trace-kyc-fail-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'kyc.raw',
  (n % 10),
  n,
  CONCAT('kyc-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),  1000,
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
FROM seq;

INSERT INTO fraud_in_success (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CURDATE(),
  CONCAT('trace-fraud-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'fraud.raw',
  (n % 10),
  n,
  CONCAT('fraud-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),  1000,
  'fraud.processed',
  (n % 10),
  n
FROM seq;

INSERT INTO fraud_in_failure (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
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
  CURDATE(),
  CONCAT('trace-fraud-fail-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'fraud.raw',
  (n % 10),
  n,
  CONCAT('fraud-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),  1000,
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
FROM seq;

INSERT INTO statements_in_success (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CURDATE(),
  CONCAT('trace-statements-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'statements.raw',
  (n % 10),
  n,
  CONCAT('statements-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),  1000,
  'statements.processed',
  (n % 10),
  n
FROM seq;

INSERT INTO statements_in_failure (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
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
  CURDATE(),
  CONCAT('trace-statements-fail-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'statements.raw',
  (n % 10),
  n,
  CONCAT('statements-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),  1000,
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
FROM seq;

INSERT INTO limits_in_success (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
  target_topic,
  target_partition_id,
  target_offset
)
SELECT
  CURDATE(),
  CONCAT('trace-limits-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'limits.raw',
  (n % 10),
  n,
  CONCAT('limits-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), ',"status":"ok"}'),  1000,
  'limits.processed',
  (n % 10),
  n
FROM seq;

INSERT INTO limits_in_failure (
  event_date,
  event_trace_id,
  account_number,
  customer_type,
  event_date_time,
  source_topic,
  source_partition_id,
  source_offset,
  message_key,
  source_payload,
  transformed_payload,  latency_ms,
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
  CURDATE(),
  CONCAT('trace-limits-fail-', LPAD(n, 6, '0')),
  CONCAT('ACC-', LPAD(n % 1000000, 6, '0')),
  CASE WHEN n % 3 = 0 THEN 'RETAIL' WHEN n % 3 = 1 THEN 'SMB' ELSE 'CORP' END,
  TIMESTAMPADD(SECOND, (n % 86400), CAST(CURDATE() AS DATETIME)),
  'limits.raw',
  (n % 10),
  n,
  CONCAT('limits-key-', n),
  CONCAT('{"amount":', (n % 10000), '}'),
  CONCAT('{"amount":', (n % 10000), '}'),  1000,
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
FROM seq;

DROP TEMPORARY TABLE seq;

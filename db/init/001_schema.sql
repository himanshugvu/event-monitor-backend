CREATE TABLE IF NOT EXISTS payments_in_success (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_datetime DATETIME NOT NULL,
  event_trace_id VARCHAR(64),
  account_number VARCHAR(64),
  customer_type VARCHAR(32),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  source_topic VARCHAR(255),
  source_partition_id INT,
  source_offset BIGINT,
  message_key VARCHAR(255),
  source_payload LONGTEXT,
  transformed_payload LONGTEXT,
  latency_ms BIGINT NOT NULL DEFAULT 0,
  latency_event_received_ms BIGINT NOT NULL DEFAULT 0,
  latency_event_sent_ms BIGINT NOT NULL DEFAULT 0,
  target_topic VARCHAR(255),
  target_partition_id INT,
  target_offset BIGINT
);

CREATE TABLE IF NOT EXISTS payments_in_failure (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_datetime DATETIME NOT NULL,
  event_trace_id VARCHAR(64),
  account_number VARCHAR(64),
  customer_type VARCHAR(32),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  source_topic VARCHAR(255),
  source_partition_id INT,
  source_offset BIGINT,
  message_key VARCHAR(255),
  source_payload LONGTEXT,
  transformed_payload LONGTEXT,
  latency_ms BIGINT NOT NULL DEFAULT 0,
  latency_event_received_ms BIGINT NOT NULL DEFAULT 0,
  target_topic VARCHAR(255),
  target_partition_id INT,
  target_offset BIGINT,
  exception_type VARCHAR(255),
  exception_message TEXT,
  exception_stack LONGTEXT,
  retriable TINYINT,
  retry_attempt INT
);

CREATE TABLE IF NOT EXISTS loans_in_success (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_datetime DATETIME NOT NULL,
  event_trace_id VARCHAR(64),
  account_number VARCHAR(64),
  customer_type VARCHAR(32),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  source_topic VARCHAR(255),
  source_partition_id INT,
  source_offset BIGINT,
  message_key VARCHAR(255),
  source_payload LONGTEXT,
  transformed_payload LONGTEXT,
  latency_ms BIGINT NOT NULL DEFAULT 0,
  latency_event_received_ms BIGINT NOT NULL DEFAULT 0,
  latency_event_sent_ms BIGINT NOT NULL DEFAULT 0,
  target_topic VARCHAR(255),
  target_partition_id INT,
  target_offset BIGINT
);

CREATE TABLE IF NOT EXISTS loans_in_failure (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_datetime DATETIME NOT NULL,
  event_trace_id VARCHAR(64),
  account_number VARCHAR(64),
  customer_type VARCHAR(32),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  source_topic VARCHAR(255),
  source_partition_id INT,
  source_offset BIGINT,
  message_key VARCHAR(255),
  source_payload LONGTEXT,
  transformed_payload LONGTEXT,
  latency_ms BIGINT NOT NULL DEFAULT 0,
  latency_event_received_ms BIGINT NOT NULL DEFAULT 0,
  target_topic VARCHAR(255),
  target_partition_id INT,
  target_offset BIGINT,
  exception_type VARCHAR(255),
  exception_message TEXT,
  exception_stack LONGTEXT,
  retriable TINYINT,
  retry_attempt INT
);

CREATE INDEX idx_payments_success_event_datetime
  ON payments_in_success (event_datetime, event_trace_id);
CREATE INDEX idx_payments_failure_event_datetime
  ON payments_in_failure (event_datetime, event_trace_id);

CREATE INDEX idx_loans_success_event_datetime
  ON loans_in_success (event_datetime, event_trace_id);
CREATE INDEX idx_loans_failure_event_datetime
  ON loans_in_failure (event_datetime, event_trace_id);

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


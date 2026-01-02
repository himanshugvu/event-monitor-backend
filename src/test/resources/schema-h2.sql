DROP TABLE IF EXISTS payments_in_failure;
DROP TABLE IF EXISTS payments_in_success;
DROP TABLE IF EXISTS loans_in_failure;
DROP TABLE IF EXISTS loans_in_success;

CREATE TABLE payments_in_success (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_date DATE NOT NULL,
  event_trace_id VARCHAR(64),
  account_number VARCHAR(64),
  customer_type VARCHAR(32),
  event_received_timestamp DATETIME,
  source_topic VARCHAR(255),
  source_partition_id INT,
  source_offset BIGINT,
  message_key VARCHAR(255),
  source_payload TEXT,
  transformed_payload TEXT,
  event_sent_timestamp DATETIME,
  target_topic VARCHAR(255),
  target_partition_id INT,
  target_offset BIGINT
);

CREATE TABLE payments_in_failure (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_date DATE NOT NULL,
  event_trace_id VARCHAR(64),
  account_number VARCHAR(64),
  customer_type VARCHAR(32),
  event_received_timestamp DATETIME,
  source_topic VARCHAR(255),
  source_partition_id INT,
  source_offset BIGINT,
  message_key VARCHAR(255),
  source_payload TEXT,
  transformed_payload TEXT,
  event_sent_timestamp DATETIME,
  target_topic VARCHAR(255),
  target_partition_id INT,
  target_offset BIGINT,
  exception_type VARCHAR(255),
  exception_message TEXT,
  exception_stack TEXT,
  retriable TINYINT,
  retry_attempt INT
);

CREATE TABLE loans_in_success (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_date DATE NOT NULL,
  event_trace_id VARCHAR(64),
  account_number VARCHAR(64),
  customer_type VARCHAR(32),
  event_received_timestamp DATETIME,
  source_topic VARCHAR(255),
  source_partition_id INT,
  source_offset BIGINT,
  message_key VARCHAR(255),
  source_payload TEXT,
  transformed_payload TEXT,
  event_sent_timestamp DATETIME,
  target_topic VARCHAR(255),
  target_partition_id INT,
  target_offset BIGINT
);

CREATE TABLE loans_in_failure (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_date DATE NOT NULL,
  event_trace_id VARCHAR(64),
  account_number VARCHAR(64),
  customer_type VARCHAR(32),
  event_received_timestamp DATETIME,
  source_topic VARCHAR(255),
  source_partition_id INT,
  source_offset BIGINT,
  message_key VARCHAR(255),
  source_payload TEXT,
  transformed_payload TEXT,
  event_sent_timestamp DATETIME,
  target_topic VARCHAR(255),
  target_partition_id INT,
  target_offset BIGINT,
  exception_type VARCHAR(255),
  exception_message TEXT,
  exception_stack TEXT,
  retriable TINYINT,
  retry_attempt INT
);

ALTER TABLE replay_items
  ADD COLUMN trace_id VARCHAR(64) NULL,
  ADD COLUMN message_key VARCHAR(255) NULL,
  ADD COLUMN account_number VARCHAR(64) NULL,
  ADD COLUMN exception_type VARCHAR(255) NULL,
  ADD COLUMN event_datetime DATETIME NULL,
  ADD COLUMN source_payload LONGTEXT NULL;

CREATE TABLE IF NOT EXISTS replay_jobs (
  id VARCHAR(64) PRIMARY KEY,
  event_key VARCHAR(64) NOT NULL,
  day DATE NOT NULL,
  selection_type VARCHAR(32) NOT NULL,
  filters_json LONGTEXT,
  snapshot_at DATETIME NOT NULL,
  requested_by VARCHAR(128),
  reason VARCHAR(255),
  total_requested INT NOT NULL DEFAULT 0,
  status VARCHAR(32) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  completed_at DATETIME NULL
);

CREATE TABLE IF NOT EXISTS replay_items (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  job_id VARCHAR(64) NOT NULL,
  record_id BIGINT NOT NULL,
  event_key VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL,
  attempt_count INT NOT NULL DEFAULT 0,
  last_attempt_at DATETIME NULL,
  last_error TEXT NULL,
  emitted_id VARCHAR(128) NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NULL,
  KEY idx_replay_items_job (job_id),
  KEY idx_replay_items_record (record_id),
  KEY idx_replay_items_event (event_key)
);

CREATE TABLE IF NOT EXISTS housekeeping_run_items (
  run_id VARCHAR(36) NOT NULL,
  event_key VARCHAR(64) NOT NULL,
  deleted_success BIGINT NOT NULL,
  deleted_failure BIGINT NOT NULL,
  deleted_total BIGINT NOT NULL,
  created_at DATETIME NOT NULL,
  PRIMARY KEY (run_id, event_key),
  INDEX idx_housekeeping_run_items_event (event_key),
  CONSTRAINT fk_housekeeping_run_items_run
    FOREIGN KEY (run_id) REFERENCES housekeeping_runs(id)
);

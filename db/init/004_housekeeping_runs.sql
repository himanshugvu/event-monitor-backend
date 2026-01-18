CREATE TABLE IF NOT EXISTS housekeeping_runs (
  id VARCHAR(64) PRIMARY KEY,
  job_type VARCHAR(32) NOT NULL,
  trigger_type VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  cutoff_date DATE NOT NULL,
  started_at DATETIME NOT NULL,
  completed_at DATETIME NULL,
  duration_ms BIGINT NULL,
  deleted_success BIGINT NOT NULL DEFAULT 0,
  deleted_failure BIGINT NOT NULL DEFAULT 0,
  deleted_total BIGINT NOT NULL DEFAULT 0,
  error_message TEXT NULL
);

CREATE INDEX idx_housekeeping_runs_started
  ON housekeeping_runs (started_at);

ALTER TABLE housekeeping_runs ADD COLUMN run_date DATE NULL;
ALTER TABLE housekeeping_runs ADD COLUMN attempt INT NOT NULL DEFAULT 1;
UPDATE housekeeping_runs SET run_date = DATE(started_at) WHERE run_date IS NULL;
ALTER TABLE housekeeping_runs MODIFY run_date DATE NOT NULL;

CREATE INDEX idx_housekeeping_runs_date
  ON housekeeping_runs (run_date, attempt);

CREATE TABLE IF NOT EXISTS housekeeping_daily (
  run_date DATE PRIMARY KEY,
  retention_days INT NOT NULL,
  cutoff_date DATE NOT NULL,
  snapshot_at DATETIME NOT NULL,
  eligible_success BIGINT NOT NULL,
  eligible_failure BIGINT NOT NULL,
  eligible_total BIGINT NOT NULL,
  last_status VARCHAR(32) NOT NULL,
  last_run_id VARCHAR(64) NULL,
  last_attempt INT NOT NULL,
  last_started_at DATETIME NULL,
  last_completed_at DATETIME NULL,
  last_error TEXT NULL
);

CREATE INDEX idx_housekeeping_daily_status
  ON housekeeping_daily (last_status, run_date);

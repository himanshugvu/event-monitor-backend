ALTER TABLE housekeeping_daily
  ADD COLUMN job_type VARCHAR(32) NOT NULL DEFAULT 'RETENTION';

UPDATE housekeeping_daily
  SET job_type = 'RETENTION'
  WHERE job_type IS NULL OR job_type = '';

ALTER TABLE housekeeping_daily
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (job_type, run_date);

CREATE INDEX idx_housekeeping_daily_job_status
  ON housekeeping_daily (job_type, last_status, run_date);

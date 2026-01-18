ALTER TABLE housekeeping_runs
  ADD COLUMN event_key VARCHAR(64) NOT NULL DEFAULT 'ALL';

UPDATE housekeeping_runs
  SET event_key = 'ALL'
  WHERE event_key IS NULL OR event_key = '';

CREATE INDEX idx_housekeeping_runs_job_event_date
  ON housekeeping_runs (job_type, event_key, run_date, attempt);

ALTER TABLE housekeeping_daily
  ADD COLUMN event_key VARCHAR(64) NOT NULL DEFAULT 'ALL';

UPDATE housekeeping_daily
  SET event_key = 'ALL'
  WHERE event_key IS NULL OR event_key = '';

ALTER TABLE housekeeping_daily
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (job_type, event_key, run_date);

CREATE INDEX idx_housekeeping_daily_job_event_status
  ON housekeeping_daily (job_type, event_key, last_status, run_date);

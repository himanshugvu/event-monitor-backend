ALTER TABLE replay_jobs
  ADD COLUMN succeeded_count INT NOT NULL DEFAULT 0,
  ADD COLUMN failed_count INT NOT NULL DEFAULT 0,
  ADD COLUMN queued_count INT NOT NULL DEFAULT 0;

UPDATE replay_jobs j
LEFT JOIN (
  SELECT job_id,
         SUM(CASE WHEN status = 'REPLAYED' THEN 1 ELSE 0 END) AS succeeded,
         SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
         SUM(CASE WHEN status = 'QUEUED' THEN 1 ELSE 0 END) AS queued
  FROM replay_items
  GROUP BY job_id
) c ON c.job_id = j.id
SET j.succeeded_count = COALESCE(c.succeeded, 0),
    j.failed_count = COALESCE(c.failed, 0),
    j.queued_count = COALESCE(c.queued, 0);

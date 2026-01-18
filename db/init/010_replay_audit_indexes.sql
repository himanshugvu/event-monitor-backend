CREATE INDEX IF NOT EXISTS idx_replay_jobs_created_at ON replay_jobs (created_at);
CREATE INDEX IF NOT EXISTS idx_replay_jobs_event_key ON replay_jobs (event_key);
CREATE INDEX IF NOT EXISTS idx_replay_jobs_requested_by ON replay_jobs (requested_by);

CREATE INDEX IF NOT EXISTS idx_replay_items_job_status ON replay_items (job_id, status);
CREATE INDEX IF NOT EXISTS idx_replay_items_job_record ON replay_items (job_id, record_id);
CREATE INDEX IF NOT EXISTS idx_replay_items_job_trace ON replay_items (job_id, trace_id);

-- 004_create_indexes.sql — Indexes (Task 6: run_id, item IDs, timestamps, trend key)

-- Spark incremental: ambil run baru yang completed
CREATE INDEX IF NOT EXISTS idx_collection_run_status_completed
    ON ops.collection_run (status, completed_at);

CREATE INDEX IF NOT EXISTS idx_collection_run_started
    ON ops.collection_run (started_at);

-- Item ID lookups (comment -> story joins)
CREATE INDEX IF NOT EXISTS idx_comment_story_id
    ON raw.hn_comment_snapshot (story_id);

-- Timestamps untuk analytics range scan
CREATE INDEX IF NOT EXISTS idx_story_time
    ON raw.hn_story_snapshot (story_time_utc);

CREATE INDEX IF NOT EXISTS idx_comment_time
    ON raw.hn_comment_snapshot (comment_time_utc);

-- Trend key across runs (Gold aggregations)
CREATE INDEX IF NOT EXISTS idx_trend_key
    ON ai.trend_analysis (trend_key);

CREATE INDEX IF NOT EXISTS idx_trend_created
    ON ai.trend_analysis (created_at);

-- Pipeline run monitoring per job
CREATE INDEX IF NOT EXISTS idx_pipeline_run_job_started
    ON ops.pipeline_run (job_name, started_at);
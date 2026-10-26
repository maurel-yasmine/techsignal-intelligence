-- 002_create_tables.sql — Enam tables + constraints (Section 5.2)

-- 1. ops.collection_run ---------------------------------------------
CREATE TABLE IF NOT EXISTS ops.collection_run (
    run_id              UUID        PRIMARY KEY,
    trigger_type        TEXT        NOT NULL,
    requested_by_hash   TEXT,
    started_at          TIMESTAMPTZ NOT NULL,
    completed_at        TIMESTAMPTZ,
    status              TEXT        NOT NULL DEFAULT 'RUNNING'
                        CHECK (status IN ('RUNNING','SUCCEEDED','PARTIAL','FAILED')),
    stories_requested   INTEGER     NOT NULL DEFAULT 0 CHECK (stories_requested >= 0),
    stories_loaded      INTEGER     NOT NULL DEFAULT 0 CHECK (stories_loaded >= 0),
    comments_requested  INTEGER     NOT NULL DEFAULT 0 CHECK (comments_requested >= 0),
    comments_loaded     INTEGER     NOT NULL DEFAULT 0 CHECK (comments_loaded >= 0),
    trends_loaded       INTEGER     NOT NULL DEFAULT 0 CHECK (trends_loaded >= 0),
    error_category      TEXT,
    schema_version      TEXT        NOT NULL
);

-- 2. raw.hn_story_snapshot (grain: 1 story per run) ------------------
CREATE TABLE IF NOT EXISTS raw.hn_story_snapshot (
    run_id          UUID        NOT NULL REFERENCES ops.collection_run(run_id) ON DELETE CASCADE,
    story_id        BIGINT      NOT NULL,
    title           TEXT,
    url             TEXT,
    author          TEXT,
    score           INTEGER,
    descendants     INTEGER,
    story_time_utc  TIMESTAMPTZ,
    collected_at    TIMESTAMPTZ NOT NULL,
    kids_json       JSONB,
    deleted         BOOLEAN     NOT NULL DEFAULT FALSE,
    dead            BOOLEAN     NOT NULL DEFAULT FALSE,
    raw_payload     JSONB       NOT NULL,
    content_hash    TEXT,
    PRIMARY KEY (run_id, story_id),
    CONSTRAINT chk_story_score_nonneg CHECK (score IS NULL OR score >= 0),
    CONSTRAINT chk_story_desc_nonneg  CHECK (descendants IS NULL OR descendants >= 0)
);

-- 3. raw.hn_comment_snapshot (grain: 1 comment per run) --------------
CREATE TABLE IF NOT EXISTS raw.hn_comment_snapshot (
    run_id           UUID        NOT NULL REFERENCES ops.collection_run(run_id) ON DELETE CASCADE,
    comment_id       BIGINT      NOT NULL,
    story_id         BIGINT      NOT NULL,
    parent_id        BIGINT,
    author           TEXT,
    comment_html     TEXT,
    comment_text     TEXT,
    comment_time_utc TIMESTAMPTZ,
    collected_at     TIMESTAMPTZ NOT NULL,
    deleted          BOOLEAN     NOT NULL DEFAULT FALSE,
    dead             BOOLEAN     NOT NULL DEFAULT FALSE,
    raw_payload      JSONB       NOT NULL,
    content_hash     TEXT,
    PRIMARY KEY (run_id, comment_id),
    CONSTRAINT fk_comment_story FOREIGN KEY (run_id, story_id)
        REFERENCES raw.hn_story_snapshot(run_id, story_id) ON DELETE CASCADE
);

-- 4. ai.trend_analysis (grain: 1 trend per run) ----------------------
CREATE TABLE IF NOT EXISTS ai.trend_analysis (
    run_id                UUID          NOT NULL REFERENCES ops.collection_run(run_id) ON DELETE CASCADE,
    trend_key             TEXT          NOT NULL,
    analysis_id           TEXT          NOT NULL,
    trend_label           TEXT          NOT NULL,
    summary               TEXT          NOT NULL,
    sentiment_label       TEXT          NOT NULL
                          CHECK (sentiment_label IN ('positive','neutral','negative','mixed')),
    sentiment_score       NUMERIC(5,4)  NOT NULL CHECK (sentiment_score BETWEEN -1 AND 1),
    confidence            NUMERIC(5,4)  NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    evidence_count        INTEGER       NOT NULL CHECK (evidence_count >= 0),
    supporting_story_ids  JSONB         NOT NULL DEFAULT '[]'::jsonb,
    keywords              JSONB         NOT NULL DEFAULT '[]'::jsonb,
    model_version         TEXT          NOT NULL,
    prompt_version        TEXT          NOT NULL,
    llm_processing_ms     BIGINT,
    usage_metadata        JSONB,
    created_at            TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, trend_key)
);

-- 5. ops.pipeline_run -------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.pipeline_run (
    pipeline_run_id   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_name          TEXT        NOT NULL,
    watermark_start   TIMESTAMPTZ,
    watermark_end     TIMESTAMPTZ,
    source_row_count  INTEGER,
    bronze_row_count  INTEGER,
    silver_row_count  INTEGER,
    gold_row_count    INTEGER,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at      TIMESTAMPTZ,
    status            TEXT        NOT NULL DEFAULT 'RUNNING'
                      CHECK (status IN ('RUNNING','SUCCEEDED','PARTIAL','FAILED')),
    error_category    TEXT
);

-- 6. ops.pipeline_checkpoint ------------------------------------------
CREATE TABLE IF NOT EXISTS ops.pipeline_checkpoint (
    job_name                      TEXT        PRIMARY KEY,
    last_successful_completed_at  TIMESTAMPTZ,
    last_successful_run_id        UUID,
    updated_at                    TIMESTAMPTZ NOT NULL DEFAULT now()
);
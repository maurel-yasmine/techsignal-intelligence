-- 1. Owner group role; postgres (Maurel) bergabung sebagai admin
CREATE ROLE techtrend_owner NOLOGIN;
GRANT techtrend_owner TO postgres;

-- 2. Role untuk Gary (LOGIN) 
CREATE ROLE techtrend_ingest LOGIN PASSWORD 'Ing3stP@ss2026' CONNECTION LIMIT 5;

-- 3. Role untuk Spark (LOGIN) 
CREATE ROLE techtrend_spark LOGIN PASSWORD 'Sp@rkP@ss2026' CONNECTION LIMIT 3;

-- 4. Alihkan kepemilikan schema & table ke owner role
ALTER SCHEMA ops OWNER TO techtrend_owner;
ALTER SCHEMA raw OWNER TO techtrend_owner;
ALTER SCHEMA ai  OWNER TO techtrend_owner;
ALTER TABLE ops.collection_run      OWNER TO techtrend_owner;
ALTER TABLE raw.hn_story_snapshot   OWNER TO techtrend_owner;
ALTER TABLE raw.hn_comment_snapshot OWNER TO techtrend_owner;
ALTER TABLE ai.trend_analysis       OWNER TO techtrend_owner;
ALTER TABLE ops.pipeline_run        OWNER TO techtrend_owner;
ALTER TABLE ops.pipeline_checkpoint OWNER TO techtrend_owner;

-- 5. Higienitas: PUBLIC tidak punya akses apa pun ke schema kita
REVOKE ALL ON SCHEMA ops, raw, ai FROM PUBLIC;

-- 6. Grants techtrend_ingest (Gary)
GRANT USAGE ON SCHEMA ops, raw, ai TO techtrend_ingest;
GRANT SELECT, INSERT, UPDATE 
    ON ops.collection_run, 
       raw.hn_story_snapshot, 
       raw.hn_comment_snapshot, 
       ai.trend_analysis 
    TO techtrend_ingest;

-- 7. Grants techtrend_spark (Spark ETL Anda)
GRANT USAGE ON SCHEMA ops, raw, ai TO techtrend_spark;
GRANT SELECT ON ALL TABLES IN SCHEMA raw, ai TO techtrend_spark;
GRANT SELECT ON ops.collection_run TO techtrend_spark;
GRANT SELECT, INSERT, UPDATE 
    ON ops.pipeline_run, ops.pipeline_checkpoint 
    TO techtrend_spark;
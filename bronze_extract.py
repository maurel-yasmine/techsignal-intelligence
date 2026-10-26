# bronze_extract.py — Task 12-14: job id, checkpoint/watermark, incremental extract (read-only)
from pyspark.sql import SparkSession
from datetime import datetime, timezone
import os

JOB_NAME = "techtrend_hourly"
PG_HOST = os.environ["PG_POOLER_HOST"]
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:5432/postgres?options=endpoint%3Dusapduupljixqwcystla"

PROPS = {    
    "driver": "org.postgresql.Driver",    
    "sslmode": "require",    
    "user": "postgres.usapduupljixqwcystla",    
    "password": os.environ["PG_SPARK_PASSWORD"],    
    "prepareThreshold": "0",
}

JAR = "file:///D:/PROJECT/jars/postgresql-42.7.4.jar"

spark = (    
    SparkSession.builder    
    .master("local[*]")    
    .appName(JOB_NAME)    
    .config("spark.jars", JAR)    
    .getOrCreate()
)

def read_table(subquery):
    return spark.read.jdbc(url=JDBC_URL, table=f"({subquery}) t", properties=PROPS)

# --- Task 13: baca checkpoint -> watermark_start; fixed watermark_end ---
ckpt_df = read_table(
    "SELECT job_name, last_successful_completed_at, "
    "last_successful_run_id::text AS last_successful_run_id, updated_at "
    "FROM ops.pipeline_checkpoint WHERE job_name = 'techtrend_hourly'"
)

if ckpt_df.count() == 0:
    watermark_start = "1970-01-01 00:00:00+00"
    print("checkpoint kosong -> watermark_start = epoch")
else:
    row = ckpt_df.first()
    ts = row["last_successful_completed_at"]
    watermark_start = ts.strftime("%Y-%m-%d %H:%M:%S+00") if ts else "1970-01-01 00:00:00+00"
    print(f"checkpoint terakhir: {watermark_start}")

watermark_end = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00")
print(f"watermark_end (fixed): {watermark_end}")

# --- Task 14: extract completed runs in window ---
runs_df = read_table(
    "SELECT run_id::text AS run_id, trigger_type, requested_by_hash, status::text AS status, "
    "stories_requested, stories_loaded, comments_requested, comments_loaded, trends_loaded, "
    "error_category, schema_version, started_at, completed_at "
    "FROM ops.collection_run "
    "WHERE status IN ('SUCCEEDED','PARTIAL') "
    f"AND completed_at > '{watermark_start}'::timestamptz "
    f"AND completed_at <= '{watermark_end}'::timestamptz"
)

runs_df.cache()
run_ids = [r.run_id for r in runs_df.select("run_id").collect()]
print(f"collection_runs baru: {len(run_ids)} -> {run_ids}")

if run_ids:
    ids_csv = ",".join(f"'{r}'" for r in run_ids)
    
    stories_df = read_table(
        "SELECT run_id::text AS run_id, story_id, title, url, author, score, descendants, "
        "story_time_utc, collected_at, kids_json::text AS kids_json, deleted, dead, "
        "raw_payload::text AS raw_payload, content_hash "
        f"FROM raw.hn_story_snapshot WHERE run_id::text IN ({ids_csv})"
    )
    
    comments_df = read_table(
        "SELECT run_id::text AS run_id, comment_id, story_id, parent_id, author, "
        "comment_html, comment_text, comment_time_utc, collected_at, deleted, dead, "
        f"raw_payload::text AS raw_payload, content_hash "
        f"FROM raw.hn_comment_snapshot WHERE run_id::text IN ({ids_csv})"
    )
    
    trends_df = read_table(
        "SELECT run_id::text AS run_id, trend_key, analysis_id, trend_label, summary, "
        "sentiment_label, sentiment_score, confidence, evidence_count, "
        "supporting_story_ids::text AS supporting_story_ids, keywords::text AS keywords, "
        "model_version, prompt_version, llm_processing_ms, usage_metadata::text AS usage_metadata, created_at "
        f"FROM ai.trend_analysis WHERE run_id::text IN ({ids_csv})"
    )
    
    print("=== source counts (preview rekonsiliasi Bronze) ===")
    print(f"stories    : {stories_df.count()}")
    print(f"comments   : {comments_df.count()}")
    print(f"trends     : {trends_df.count()}")
    stories_df.printSchema()
else:
    print("tidak ada run baru untuk diproses")

spark.stop()
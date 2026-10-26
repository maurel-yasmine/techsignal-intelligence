# bq_ensure.py — ensure Silver/Gold tables exist (CREATE TABLE IF NOT EXISTS)
# Dipanggil di awal silver_transform.py & gold_transform.py sebelum early-exit.
import os
from google.cloud import bigquery

SILVER_SPECS = {
    "fact_story_snapshot": (
        "run_id STRING, story_id INT64, title STRING, url STRING, author STRING, "
        "score INT64, descendants INT64, story_time_utc TIMESTAMP, collected_at TIMESTAMP, "
        "story_age_hours FLOAT64, pipeline_run_id STRING, job_name STRING, "
        "watermark_start TIMESTAMP, watermark_end TIMESTAMP, loaded_at TIMESTAMP"
    ),
    "fact_comment_snapshot": (
        "run_id STRING, comment_id INT64, story_id INT64, parent_id INT64, author STRING, "
        "comment_text STRING, comment_time_utc TIMESTAMP, collected_at TIMESTAMP, "
        "pipeline_run_id STRING, job_name STRING, watermark_start TIMESTAMP, "
        "watermark_end TIMESTAMP, loaded_at TIMESTAMP"
    ),
    "fact_trend_sentiment": (
        "run_id STRING, trend_key STRING, analysis_id STRING, trend_label STRING, "
        "summary STRING, sentiment_label STRING, sentiment_score FLOAT64, "
        "confidence FLOAT64, evidence_count INT64, supporting_story_ids ARRAY<STRING>, "
        "keywords ARRAY<STRING>, model_version STRING, prompt_version STRING, "
        "llm_processing_ms INT64, created_at TIMESTAMP, pipeline_run_id STRING, "
        "job_name STRING, watermark_start TIMESTAMP, watermark_end TIMESTAMP, loaded_at TIMESTAMP"
    ),
    "dim_trend": (
        "trend_key STRING, trend_label STRING, model_version STRING, "
        "prompt_version STRING, first_seen_at TIMESTAMP"
    ),
    "data_quality_issue": (
        "pipeline_run_id STRING, table_name STRING, row_key STRING, "
        "issue_type STRING, severity STRING, issue_detail STRING"
    ),
}

GOLD_SPECS = {
    "trend_daily": (
        "trend_date DATE, trend_key STRING, trend_label STRING, story_count INT64, "
        "total_evidence_count INT64, avg_sentiment FLOAT64, weighted_sentiment FLOAT64, "
        "avg_confidence FLOAT64, total_hn_score INT64, avg_hn_score FLOAT64, "
        "total_comments INT64, trend_momentum FLOAT64"
    ),
    "story_performance_daily": (
        "story_date DATE, story_id INT64, latest_score INT64, latest_comment_count INT64, "
        "title STRING, url STRING, author STRING, assigned_trends ARRAY<STRING>, "
        "best_rank_in_trend INT64, score_delta INT64, comment_delta INT64"
    ),
    "trend_momentum_daily": (
        "trend_date DATE, trend_key STRING, trend_label STRING, "
        "score_growth FLOAT64, comment_growth FLOAT64, story_freq_growth FLOAT64, "
        "score_growth_norm FLOAT64, comment_growth_norm FLOAT64, "
        "story_freq_growth_norm FLOAT64, trend_momentum FLOAT64"
    ),
    "pipeline_quality_daily": (
        "quality_date DATE, total_source_rows INT64, total_bronze_rows INT64, "
        "total_silver_rows INT64, total_gold_rows INT64, total_runs INT64, "
        "successful_runs INT64, failed_runs INT64, freshness_timestamp TIMESTAMP, "
        "last_successful_run_id STRING, quarantine_count INT64, "
        "deleted_dead_count INT64, invalid_ai_count INT64, llm_success_rate FLOAT64"
    ),
}


def ensure_tables(dataset_name, specs):
    """Create dataset + empty tables if they don't exist."""
    project = os.environ["GCP_PROJECT_ID"]
    client = bigquery.Client(project=project)

    # pastikan dataset ada
    try:
        client.create_dataset(dataset_name, exists_ok=True)
    except Exception as e:
        print(f"(ensure: dataset {dataset_name} exists or skip: {type(e).__name__})")

    for table_name, columns_ddl in specs.items():
        table_id = f"{project}.{dataset_name}.{table_name}"
        ddl = f"CREATE TABLE IF NOT EXISTS `{table_id}` ({columns_ddl})"
        try:
            job = client.query(ddl)
            job.result()
            print(f"  ensure: {dataset_name}.{table_name} ready")
        except Exception as e:
            print(f"  ensure: {dataset_name}.{table_name} SKIP ({type(e).__name__}: {e})")

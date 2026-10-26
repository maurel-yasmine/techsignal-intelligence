# TechSignal Intelligence

An enterprise-grade, end-to-end Data Engineering pipeline and Business Intelligence solution designed to ingest, process, and analyze technology trend data from Hacker News in real time. Built using a structured Medallion Architecture (Bronze, Silver, and Gold layers), this project demonstrates scalable PySpark data processing, idempotent database interactions, automated data warehouse synchronization with Google BigQuery, and executive reporting via Tableau.

---

## Architecture & Data Pipeline Overview

The core objective of TechSignal Intelligence is to convert high-volume, unstructured external API data into structured, BI-ready analytics through a robust multi-stage pipeline.

Hacker News API ---> PostgreSQL (Bronze / Silver) ---> PySpark Processing ---> Google BigQuery ---> Tableau Dashboards


* **Bronze Layer (Ingestion)**: Captures raw payload data directly from source REST APIs and persists it with ingestion timestamps and metadata for lineage tracking.
* **Silver Layer (Cleansing & Feature Engineering)**: Cleans text formatting, strips HTML, filters invalid/deleted records, casts data types, and derives temporal metrics (such as story age in hours) using PySpark.
* **Gold Layer (Aggregation & BI Serving)**: Computes business-level metrics, daily volume trends, and momentum scores, feeding optimized `.hyper` data extracts into Tableau dashboards.

---

## Tech Stack & Infrastructure

* **Core Programming Language**: Python (Requests, PySpark, Pandas)
* **Distributed Computing Framework**: Apache Spark (PySpark with PostgreSQL JDBC driver integration)
* **Relational Database**: PostgreSQL (Schema organization, indexing, operational check-pointing)
* **Cloud Data Warehouse**: Google BigQuery
* **Business Intelligence & Analytics**: Tableau Desktop (`.twb` workbooks, `.hyper` extracts)
* **Containerization & Version Control**: Docker, Git (`.gitignore` for security and temporary artifact exclusion)

---

## Repository Structure

```text
techsignal-intelligence/
│
├── sql/                        # Relational database provisioning scripts
│   ├── 001_create_schemas.sql   # Schema definitions (bronze, silver, gold, ops)
│   ├── 002_create_tables.sql    # DDL for pipeline tables
│   ├── 003_create_roles_grants.sql # Role-Based Access Control (RBAC) definitions
│   └── 004_create_indexes.sql   # Performance tuning and indexing strategies
│
├── secret/                     # Secret keys and service accounts (Excluded via .gitignore)
├── jars/                       # JDBC drivers and external JAR dependencies
│
├── bronze_extract.py           # Core API extraction logic
├── bronze_write.py             # Raw data persistence into PostgreSQL Bronze layer
├── silver_transform.py         # PySpark data cleansing and validation rules
├── gold_transform.py           # Metric aggregation scripts for Gold layer
├── bq_ensure.py                # Automated BigQuery dataset and table provisioning
├── bq_verify.py                # BigQuery data integrity and schema validation
├── grant_iam.py                # Cloud IAM permission management
├── phase3_integration.py       # Full end-to-end pipeline execution driver
├── spark_jdbc_smoke.py         # Connectivity verification tests for Spark-JDBC
│
├── RUNBOOK.md                  # Detailed step-by-step operational execution runbook
├── PROJECT_PLANNING4.md        # Technical design, architectural specifications, and plan
├── Laporan_Analisis_Proyek.twb # Executive Tableau analytical workbook
└── .gitignore                  # Exclusion configurations for security and build artifacts
Key Technical Components Built
1. Raw Data Ingestion Layer (bronze_write.py)
Responsible for fetching story metadata from the Hacker News REST API, structuring the payload, and appending execution metadata (collected_at and run_id) for downstream processing:

Python
import requests
from datetime import datetime

def fetch_hacker_news_stories():
    top_stories_url = "[https://hacker-news.firebaseio.com/v0/topstories.json](https://hacker-news.firebaseio.com/v0/topstories.json)"
    response = requests.get(top_stories_url)
    story_ids = response.json()[:30]
    
    stories_data = []
    for s_id in story_ids:
        item_url = f"[https://hacker-news.firebaseio.com/v0/item/](https://hacker-news.firebaseio.com/v0/item/){s_id}.json"
        item_resp = requests.get(item_url)
        item = item_resp.json()
        if item:
            stories_data.append({
                "story_id": item.get("id"),
                "title": item.get("title"),
                "url": item.get("url"),
                "author": item.get("by"),
                "score": item.get("score"),
                "descendants": item.get("descendants", 0),
                "deleted": item.get("deleted", False),
                "dead": item.get("dead", False),
                "story_time_utc": datetime.utcfromtimestamp(item.get("time", 0)) if item.get("time") else None,
                "collected_at": datetime.utcnow()
            })
    return stories_data
2. PySpark Data Cleansing & Transformation (silver_transform.py)
Leverages Apache Spark to standardize data types, filter deleted or dead records, sanitize strings, and compute derived features like story_age_hours:

Python
from pyspark.sql import functions as F

def transform_stories(bronze):
    df = bronze
    df = (
        df.withColumn("story_id", F.col("story_id").cast("bigint"))
        .withColumn("score", F.col("score").cast("bigint"))
        .withColumn("descendants", F.col("descendants").cast("bigint"))
        .withColumn("title", F.trim(F.col("title")))
        .withColumn("story_time_utc", F.col("story_time_utc").cast("timestamp"))
        .withColumn("collected_at", F.col("collected_at").cast("timestamp"))
    )
    
    valid = df.filter(
        F.col("story_id").isNotNull()
        & F.col("run_id").isNotNull()
        & (F.col("deleted") != True)
        & (F.col("dead") != True)
    )
    
    valid = valid.withColumn(
        "story_age_hours",
        F.round(
            (F.col("collected_at").cast("long") - F.col("story_time_utc").cast("long")) / 3600.0, 2
        ),
    )
    return valid
3. Pipeline Reliability & Idempotent Checkpointing (upsert_checkpoint.py)
To prevent duplicate records during job retries, an operational checkpoint system tracks execution status using database-level UPSERT statements:

Python
import uuid

def upsert_checkpoint(completed_at_dt, last_run_id):
    run_uuid = uuid.UUID(last_run_id) if last_run_id else None
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ops.pipeline_checkpoint "
                "(job_name, last_successful_completed_at, last_successful_run_id, updated_at) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (job_name) DO UPDATE SET "
                "last_successful_completed_at = EXCLUDED.last_successful_completed_at, "
                "last_successful_run_id = COALESCE(EXCLUDED.last_successful_run_id, "
                "ops.pipeline_checkpoint.last_successful_run_id), "
                "updated_at = EXCLUDED.updated_at",
                (JOB_NAME, completed_at_dt, run_uuid, now_utc()),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
4. Gold Layer Metrics Aggregation (gold_transform.py)
Aggregates high-volume cleansed data into daily summary tables optimized for analytical reporting and Tableau dashboard consumption:

Python
from pyspark.sql import functions as F

def aggregate_gold_layer(spark_df):
    gold_df = spark_df.groupBy(
        F.to_date("story_time_utc").alias("trend_date"),
        "trend_key",
        "trend_label"
    ).agg(
        F.count("story_id").alias("story_count"),
        F.sum("score").alias("total_hn_score"),
        F.sum("descendants").alias("total_comments"),
        F.avg("sentiment_score").alias("avg_sentiment")
    )
    return gold_df
Operational Execution Guide
For detailed environment setup, database credential configurations, and execution commands across Bronze, Silver, and Gold pipeline phases, please refer to RUNBOOK.md.

License
This project is open-source software licensed under the MIT License.

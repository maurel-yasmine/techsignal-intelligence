# bronze_write.py — Task 15-16-25-26: Bronze write, reconcile, checkpoint, pipeline_run
# Refactored: no module-level spark/DataFrame globals to avoid PicklingError on Windows.
import os
import sys
import uuid
import psycopg
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- module-level constants (all picklable: strings, dicts) ---

JOB_NAME = "techtrend_hourly"
GCP_PROJECT = os.environ["GCP_PROJECT_ID"]
BRONZE_DATASET = "techtrend_bronze"
PG_HOST = os.environ["PG_POOLER_HOST"]
KEYFILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:5432/postgres?options=endpoint%3Dusapduupljixqwcystla"
JAR = "file:///D:/PROJECT/jars/postgresql-42.7.4.jar"
BQ_PACKAGES = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2"

PROPS = {
    "driver": "org.postgresql.Driver",
    "sslmode": "require",
    "user": "postgres.usapduupljixqwcystla",
    "password": os.environ["PG_SPARK_PASSWORD"],
    "prepareThreshold": "0",
}

_PG_REF = PROPS["user"].split(".", 1)[1] if "." in PROPS["user"] else ""

BRONZE_TABLES = {
    "stories": ("hn_story_snapshot_raw", ["run_id", "story_id"]),
    "comments": ("hn_comment_snapshot_raw", ["run_id", "comment_id"]),
    "trends": ("trend_analysis_raw", ["run_id", "trend_key"]),
    "runs": ("collection_run_raw", ["run_id"]),
}


# --- pure helpers (no spark reference) ---

def now_utc():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def utc_dt(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S+00")


def fmt_ts(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S+00")


# --- Spark helpers (spark passed as explicit param — no global capture) ---

def read_pg(spark, subquery):
    return spark.read.jdbc(url=JDBC_URL, table=f"({subquery}) t", properties=PROPS)


def bq_read(spark, table):
    return (
        spark.read.format("bigquery")
        .option("parentProject", GCP_PROJECT)
        .option("project", GCP_PROJECT)
        .option("dataset", BRONZE_DATASET)
        .option("table", table)
        .load()
    )


def write_bronze(spark, df, table):
    (
        df.write.format("bigquery")
        .option("parentProject", GCP_PROJECT)
        .option("project", GCP_PROJECT)
        .option("dataset", BRONZE_DATASET)
        .option("table", table)
        .option("writeMethod", "direct")
        .option("createDisposition", "CREATE_IF_NEEDED")
        .mode("append")
        .save()
    )


def dedup_new(spark, df, table, keys):
    try:
        existing = bq_read(spark, table).select(*keys)
        return df.join(existing, on=keys, how="left_anti")
    except Exception:
        return df


def tag_meta(df, prid, ws_dt, we_dt):
    ws_str = ws_dt.strftime("%Y-%m-%d %H:%M:%S")
    we_str = we_dt.strftime("%Y-%m-%d %H:%M:%S")
    return (
        df.withColumn("pipeline_run_id", F.lit(prid))
        .withColumn("job_name", F.lit(JOB_NAME))
        .withColumn("watermark_start", F.lit(ws_str).cast("timestamp"))
        .withColumn("watermark_end", F.lit(we_str).cast("timestamp"))
        .withColumn("loaded_at", F.current_timestamp())
    )


# --- psycopg DML (driver-side only, never serialized to workers) ---

def pg_conn():
    opts = "-c timezone=UTC"
    if _PG_REF:
        opts += f" -c endpoint={_PG_REF}"
    return psycopg.connect(
        host=PG_HOST,
        port=5432,
        dbname="postgres",
        user=PROPS["user"],
        password=PROPS["password"],
        sslmode="require",
        connect_timeout=30,
        options=opts,
    )


def insert_pipeline_run(prid, ws_dt, we_dt):
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ops.pipeline_run "
                "(pipeline_run_id, job_name, watermark_start, watermark_end, started_at, status) "
                "VALUES (%s, %s, %s, %s, %s, 'RUNNING')",
                (uuid.UUID(prid), JOB_NAME, ws_dt, we_dt, now_utc()),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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


def finalize_pipeline_run(prid, status, src_n, brz_n, error_category=None):
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ops.pipeline_run "
                "SET source_row_count = %s, bronze_row_count = %s, "
                "completed_at = %s, status = %s, "
                "error_category = %s "
                "WHERE pipeline_run_id = %s",
                (src_n, brz_n, now_utc(), status, error_category, uuid.UUID(prid)),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# --- main: all execution inside a function to prevent module-level pickle ---

def main():
    conf = {
        "spark.sql.session.timeZone": "UTC",
        "spark.jars": JAR,
        "spark.jars.packages": BQ_PACKAGES,
    }
    if KEYFILE:
        conf["spark.hadoop.google.cloud.auth.service.account.enable"] = "true"
        conf["spark.hadoop.google.cloud.auth.service.account.json.keyfile"] = KEYFILE

    builder = SparkSession.builder.master("local[*]").appName(JOB_NAME)
    for k, v in conf.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    prid = str(uuid.uuid4())

    try:
        # --- Task 13: checkpoint -> watermark ---
        ckpt_df = read_pg(
            spark,
            "SELECT job_name, last_successful_completed_at, "
            "last_successful_run_id::text AS last_successful_run_id, updated_at "
            "FROM ops.pipeline_checkpoint WHERE job_name = 'techtrend_hourly'",
        )

        if ckpt_df.count() == 0:
            watermark_start = "1970-01-01 00:00:00+00"
        else:
            ts = ckpt_df.first()["last_successful_completed_at"]
            watermark_start = fmt_ts(ts) if ts else "1970-01-01 00:00:00+00"

        we_dt = now_utc()
        watermark_end = fmt_ts(we_dt)
        print(f"pipeline_run_id={prid}  window=[{watermark_start} .. {watermark_end}]")

        # --- Task 26: buka row pipeline_run RUNNING ---
        insert_pipeline_run(prid, utc_dt(watermark_start), we_dt)

        # --- Task 14: extract completed runs in window ---
        runs_df = read_pg(
            spark,
            "SELECT run_id::text AS run_id, trigger_type, requested_by_hash, status, "
            "stories_requested, stories_loaded, comments_requested, comments_loaded, "
            "trends_loaded, error_category, schema_version, started_at, completed_at "
            "FROM ops.collection_run "
            "WHERE status IN ('SUCCEEDED','PARTIAL') "
            f"AND completed_at > '{watermark_start}'::timestamptz "
            f"AND completed_at <= '{watermark_end}'::timestamptz",
        ).cache()

        run_ids = [r.run_id for r in runs_df.select("run_id").collect()]
        print(f"collection_runs baru: {len(run_ids)}")

        src_counts = {"stories": 0, "comments": 0, "trends": 0, "runs": len(run_ids)}

        if not run_ids:
            # tidak ada run baru — checkpoint tetap maju
            upsert_checkpoint(we_dt, None)
            finalize_pipeline_run(prid, "SUCCEEDED", 0, 0)
            print("PIPELINE OK  no new runs — checkpoint advanced")
            return

        ids_csv = ",".join(f"'{r}'" for r in run_ids)

        # --- baca source tables (flat: no nested closures) ---
        stories_df = read_pg(
            spark,
            "SELECT run_id::text AS run_id, story_id, title, url, author, score, "
            "descendants, story_time_utc, collected_at, kids_json::text AS kids_json, "
            "deleted, dead, raw_payload::text AS raw_payload, content_hash "
            f"FROM raw.hn_story_snapshot WHERE run_id::text IN ({ids_csv})",
        ).cache()

        comments_df = read_pg(
            spark,
            "SELECT run_id::text AS run_id, comment_id, story_id, parent_id, author, "
            "comment_html, comment_text, comment_time_utc, collected_at, deleted, dead, "
            f"raw_payload::text AS raw_payload, content_hash "
            f"FROM raw.hn_comment_snapshot WHERE run_id::text IN ({ids_csv})",
        ).cache()

        trends_df = read_pg(
            spark,
            "SELECT run_id::text AS run_id, trend_key, analysis_id, trend_label, "
            "summary, sentiment_label, sentiment_score, confidence, evidence_count, "
            "supporting_story_ids::text AS supporting_story_ids, "
            "keywords::text AS keywords, model_version, prompt_version, "
            "llm_processing_ms, usage_metadata::text AS usage_metadata, created_at "
            f"FROM ai.trend_analysis WHERE run_id::text IN ({ids_csv})",
        ).cache()

        src_counts["stories"] = stories_df.count()
        src_counts["comments"] = comments_df.count()
        src_counts["trends"] = trends_df.count()

        # map key -> (df, table, dedup_keys)
        table_map = {
            "stories": (stories_df, "hn_story_snapshot_raw", ["run_id", "story_id"]),
            "comments": (comments_df, "hn_comment_snapshot_raw", ["run_id", "comment_id"]),
            "trends": (trends_df, "trend_analysis_raw", ["run_id", "trend_key"]),
            "runs": (runs_df, "collection_run_raw", ["run_id"]),
        }

        # --- Task 15: dedup + write Bronze ---
        for key, (df, table, keys) in table_map.items():
            new_df = dedup_new(spark, df, table, keys).cache()
            n_new = new_df.count()
            if n_new > 0:
                write_bronze(spark, tag_meta(new_df, prid, utc_dt(watermark_start), we_dt), table)
            src_counts[key] = src_counts.get(key, n_new)
            print(f"bronze.{table}: source={src_counts[key]} new_rows_written={n_new}")

        # --- Task 16: rekonsiliasi source-to-Bronze ---
        print("=== rekonsiliasi source -> bronze ===")
        mismatches = []
        brz_counts = {}
        for key, (df, table, keys) in table_map.items():
            b = bq_read(spark, table).filter(F.col("run_id").isin(run_ids)).count()
            brz_counts[key] = b
            ok = b == src_counts[key]
            print(f"{table:26s} source={src_counts[key]:5d} bronze={b:5d} {'OK' if ok else 'MISMATCH'}")
            if not ok:
                mismatches.append(table)

        src_total = sum(src_counts.values())
        brz_total = sum(brz_counts.values())

        if mismatches:
            finalize_pipeline_run(prid, "FAILED", src_total, brz_total, "RECONCILIATION_MISMATCH")
            print(f"REKONSILIASI GAGAL: {mismatches} — checkpoint TIDAK maju")
            return

        # --- Task 25: checkpoint advance hanya setelah semua write sukses ---
        rows = runs_df.select("run_id", "completed_at").collect()
        last_run_id = max(rows, key=lambda r: r.completed_at).run_id if rows else None

        upsert_checkpoint(we_dt, last_run_id)
        finalize_pipeline_run(prid, "SUCCEEDED", src_total, brz_total)
        print(f"PIPELINE OK  src={src_total} bronze={brz_total} checkpoint-> {watermark_end}")

    except Exception as e:
        print(f"PIPELINE GAGAL: {type(e).__name__}: {e}")
        try:
            finalize_pipeline_run(prid, "FAILED", 0, 0, type(e).__name__[:60])
        except Exception as e2:
            print(f"(gagal update pipeline_run status: {e2})")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

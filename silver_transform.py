# silver_transform.py — Task 17-20: Bronze -> Silver, cleaning/normalisasi/dedup, pipeline_run, checkpoint
import os
import sys
import uuid
import psycopg
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from bq_ensure import ensure_tables, SILVER_SPECS

JOB_NAME = "techtrend_silver"
GCP_PROJECT = os.environ["GCP_PROJECT_ID"]
BRONZE_DATASET = "techtrend_bronze"
SILVER_DATASET = "techtrend_silver"
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

SILVER_TABLES = {
    "stories": ("fact_story_snapshot", ["run_id", "story_id"]),
    "comments": ("fact_comment_snapshot", ["run_id", "comment_id"]),
    "trends": ("fact_trend_sentiment", ["run_id", "trend_key"]),
    "dim_trend": ("dim_trend", ["trend_key"]),
    "dq": ("data_quality_issue", ["pipeline_run_id", "table_name", "row_key", "issue_type"]),
}

conf = {
    "spark.sql.session.timeZone": "UTC",
    "spark.jars": JAR,
    "spark.jars.packages": BQ_PACKAGES,
}
if KEYFILE:
    conf["spark.hadoop.google.cloud.auth.service.account.enable"] = "true"
    conf["spark.hadoop.google.cloud.auth.service.account.json.keyfile"] = KEYFILE

builder = SparkSession.builder.master("local[*]").appName(f"{JOB_NAME}")
for k, v in conf.items():
    builder = builder.config(k, v)

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# --- helpers ---

def now_utc():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def utc_dt(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S+00")


def fmt_ts(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S+00")


def ts_lit(s):
    """Spark timestamp literal dari string — hindari Python mktime overflow di Windows."""
    return F.lit(s).cast("timestamp")


def bq_read(dataset, table):
    """Baca BigQuery dengan mode sesuai BQ_READ_METHOD env:
    - 'direct'  (default connector) butuh bigquery.readsessions.create
    - 'export'  (table export ke GCS) hanya butuh bigquery.tables.export
    """
    reader = (
        spark.read.format("bigquery")
        .option("parentProject", GCP_PROJECT)
        .option("project", GCP_PROJECT)
        .option("dataset", dataset)
        .option("table", table)
    )
    read_method = os.environ.get("BQ_READ_METHOD", "direct").lower()
    if read_method == "export":
        materialization = os.environ.get("BQ_MATERIALIZATION_DATASET", "spark_bigquery_mat")
        reader = reader.option("viewsEnabled", "true").option(
            "materializationProject", GCP_PROJECT
        ).option("materializationDataset", materialization)
    return reader.load()


def bq_read_safe(dataset, table):
    try:
        df = bq_read(dataset, table)
        df.count()
        return df
    except Exception as e:
        print(f"(skip {dataset}.{table}: {type(e).__name__}: {e})")
        return None


def bq_read_or_fail(dataset, table):
    """Baca Bronze — return df atau raise dengan pesan IAM yang jelas."""
    df = bq_read_safe(dataset, table)
    if df is None:
        raise PermissionError(
            f"Gagal baca {dataset}.{table}. "
            "Grant roles/bigquery.readSessionUser + roles/bigquery.dataViewer "
            "ke service account, atau set BQ_READ_METHOD=export."
        )
    return df


def write_bq(df, table):
    (
        df.write.format("bigquery")
        .option("parentProject", GCP_PROJECT)
        .option("project", GCP_PROJECT)
        .option("dataset", SILVER_DATASET)
        .option("table", table)
        .option("writeMethod", "direct")
        .option("createDisposition", "CREATE_IF_NEEDED")
        .mode("append")
        .save()
    )


def dedup_new(df, table, keys):
    df = df.dropDuplicates(keys)
    try:
        existing = bq_read(SILVER_DATASET, table).select(*keys)
        return df.join(existing, on=keys, how="left_anti")
    except Exception:
        return df


# --- Task 17: cleaning + normalisasi teks + pembersihan HTML ---

HTML_ENTITIES = {
    "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"',
    "&#x27;": "'", "&#39;": "'", "&nbsp;": " ", "&#x2F;": "/",
}


def strip_html(col):
    c = col
    for ent, rep in HTML_ENTITIES.items():
        c = F.regexp_replace(c, ent, rep)
    c = F.regexp_replace(c, r"<[a-zA-Z/][^>]*>", " ")
    c = F.regexp_replace(c, r"\s+", " ")
    return F.trim(c)


def normalize_text(col, max_len=20000):
    c = F.regexp_replace(F.trim(col), r"\s+", " ")
    return F.when(c.isNull() | (c == ""), None).otherwise(F.substring(c, 1, max_len))


def normalize_author(col):
    c = F.trim(col)
    return F.when(c.isNull() | (c == "") | (c == "[deleted]"), None).otherwise(c)


def valid_url(col):
    c = F.trim(col)
    return F.when(c.rlike(r"^https?://\S+$"), c).otherwise(None)


def flatten_json_array(col):
    return F.from_json(col, ArrayType(StringType()))


# --- psycopg: pipeline_run + checkpoint ---

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


def finalize_pipeline_run(prid, status, bronze_n, silver_n, error_category=None):
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ops.pipeline_run "
                "SET bronze_row_count = %s, silver_row_count = %s, "
                "completed_at = %s, status = %s, error_category = %s "
                "WHERE pipeline_run_id = %s",
                (bronze_n, silver_n, now_utc(), status, error_category, uuid.UUID(prid)),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# --- Task 17-19: transform Bronze -> Silver per tabel ---

def transform_stories(bronze):
    df = bronze
    df = (
        df.withColumn("story_id", F.col("story_id").cast("bigint"))
        .withColumn("score", F.col("score").cast("bigint"))
        .withColumn("descendants", F.col("descendants").cast("bigint"))
        .withColumn("title", strip_html(normalize_text(F.col("title"), 500)))
        .withColumn("url", valid_url(F.col("url")))
        .withColumn("author", normalize_author(F.col("author")))
        .withColumn("story_time_utc", F.col("story_time_utc").cast("timestamp"))
        .withColumn("collected_at", F.col("collected_at").cast("timestamp"))
        .withColumn("loaded_at", F.col("loaded_at").cast("timestamp"))
    )
    dq = df.filter(
        F.col("story_id").isNull()
        | F.col("run_id").isNull()
        | (F.col("deleted") == True)
        | (F.col("dead") == True)
    )
    dq_rows = dq.select(
        F.col("pipeline_run_id"),
        F.lit("fact_story_snapshot").alias("table_name"),
        F.concat_ws(":", F.col("run_id"), F.col("story_id")).alias("row_key"),
        F.when(F.col("story_id").isNull(), F.lit("NULL_KEY"))
        .when((F.col("deleted") == True) | (F.col("dead") == True), F.lit("DELETED_OR_DEAD"))
        .otherwise(F.lit("INVALID")).alias("issue_type"),
        F.lit("WARN").alias("severity"),
        F.to_json(F.struct("deleted", "dead", "score", "descendants")).alias("issue_detail"),
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
    cols = [
        "run_id", "story_id", "title", "url", "author", "score", "descendants",
        "story_time_utc", "collected_at", "story_age_hours",
        "pipeline_run_id", "job_name", "watermark_start", "watermark_end", "loaded_at",
    ]
    return valid.select(*cols), dq_rows


def transform_comments(bronze):
    df = bronze
    df = (
        df.withColumn("comment_id", F.col("comment_id").cast("bigint"))
        .withColumn("story_id", F.col("story_id").cast("bigint"))
        .withColumn("comment_text", strip_html(F.coalesce(F.col("comment_text"), F.col("comment_html"))))
        .withColumn("comment_text", normalize_text(F.col("comment_text")))
        .withColumn("author", normalize_author(F.col("author")))
        .withColumn("comment_time_utc", F.col("comment_time_utc").cast("timestamp"))
        .withColumn("collected_at", F.col("collected_at").cast("timestamp"))
        .withColumn("loaded_at", F.col("loaded_at").cast("timestamp"))
    )
    dq = df.filter(
        F.col("comment_id").isNull()
        | F.col("run_id").isNull()
        | (F.col("deleted") == True)
        | (F.col("dead") == True)
        | F.col("comment_text").isNull()
    )
    dq_rows = dq.select(
        F.col("pipeline_run_id"),
        F.lit("fact_comment_snapshot").alias("table_name"),
        F.concat_ws(":", F.col("run_id"), F.col("comment_id")).alias("row_key"),
        F.when(F.col("comment_id").isNull(), F.lit("NULL_KEY"))
        .when((F.col("deleted") == True) | (F.col("dead") == True), F.lit("DELETED_OR_DEAD"))
        .when(F.col("comment_text").isNull(), F.lit("EMPTY_TEXT"))
        .otherwise(F.lit("INVALID")).alias("issue_type"),
        F.lit("WARN").alias("severity"),
        F.to_json(F.struct("deleted", "dead", "author")).alias("issue_detail"),
    )
    valid = df.filter(
        F.col("comment_id").isNotNull()
        & F.col("run_id").isNotNull()
        & (F.col("deleted") != True)
        & (F.col("dead") != True)
        & F.col("comment_text").isNotNull()
    )
    cols = [
        "run_id", "comment_id", "story_id", "parent_id", "author", "comment_text",
        "comment_time_utc", "collected_at",
        "pipeline_run_id", "job_name", "watermark_start", "watermark_end", "loaded_at",
    ]
    return valid.select(*cols), dq_rows


def transform_trends(bronze, stories_valid):
    df = bronze
    df = (
        df.withColumn("analysis_id", F.col("analysis_id").cast("string"))
        .withColumn("sentiment_score", F.col("sentiment_score").cast("double"))
        .withColumn("confidence", F.col("confidence").cast("double"))
        .withColumn("evidence_count", F.col("evidence_count").cast("bigint"))
        .withColumn("llm_processing_ms", F.col("llm_processing_ms").cast("bigint"))
        .withColumn("created_at", F.col("created_at").cast("timestamp"))
        .withColumn("loaded_at", F.col("loaded_at").cast("timestamp"))
        .withColumn("supporting_story_ids_arr", flatten_json_array(F.col("supporting_story_ids")))
        .withColumn("keywords_arr", flatten_json_array(F.col("keywords")))
    )
    df = df.withColumn(
        "sentiment_score_valid", F.col("sentiment_score").between(-1.0, 1.0)
    ).withColumn(
        "confidence_valid", F.col("confidence").between(0.0, 1.0)
    )
    # Task 18: validasi evidence IDs — supporting_story_ids harus subset story di batch
    valid_sid = stories_valid.select(
        F.col("run_id"), F.col("story_id").cast("string").alias("sid")
    ).distinct()
    exploded = df.filter(F.col("supporting_story_ids_arr").isNotNull()).select(
        "run_id", "trend_key", F.explode("supporting_story_ids_arr").alias("ev_sid")
    )
    missing = exploded.join(
        valid_sid,
        (exploded["run_id"] == valid_sid["run_id"]) & (exploded["ev_sid"] == valid_sid["sid"]),
        how="left_anti",
    ).select("run_id", "trend_key").distinct()
    df = df.join(
        missing.withColumn("_has_missing", F.lit(True)),
        on=["run_id", "trend_key"], how="left"
    )
    df = df.withColumn(
        "evidence_valid",
        F.when(
            F.col("supporting_story_ids_arr").isNull() | (F.size("supporting_story_ids_arr") == 0),
            F.lit(True),
        ).otherwise(F.col("_has_missing").isNull()),
    ).drop("_has_missing")

    dq = df.filter(
        F.col("trend_key").isNull()
        | F.col("run_id").isNull()
        | ~F.col("sentiment_score_valid")
        | ~F.col("confidence_valid")
        | ~F.col("evidence_valid")
    )
    dq_rows = dq.select(
        F.col("pipeline_run_id"),
        F.lit("fact_trend_sentiment").alias("table_name"),
        F.concat_ws(":", F.col("run_id"), F.col("trend_key")).alias("row_key"),
        F.when(F.col("trend_key").isNull() | F.col("run_id").isNull(), F.lit("NULL_KEY"))
        .when(~F.col("sentiment_score_valid"), F.lit("SENTIMENT_OUT_OF_RANGE"))
        .when(~F.col("confidence_valid"), F.lit("CONFIDENCE_OUT_OF_RANGE"))
        .otherwise(F.lit("INVALID_EVIDENCE_IDS")).alias("issue_type"),
        F.lit("WARN").alias("severity"),
        F.to_json(F.struct("sentiment_score", "confidence", "supporting_story_ids_arr")).alias("issue_detail"),
    )
    valid = df.filter(
        F.col("trend_key").isNotNull()
        & F.col("run_id").isNotNull()
        & F.col("sentiment_score_valid")
        & F.col("confidence_valid")
        & F.col("evidence_valid")
    )
    cols = [
        "run_id", "trend_key", "analysis_id", "trend_label", "summary",
        "sentiment_label", "sentiment_score", "confidence", "evidence_count",
        "supporting_story_ids_arr", "keywords_arr", "model_version", "prompt_version",
        "llm_processing_ms", "created_at",
        "pipeline_run_id", "job_name", "watermark_start", "watermark_end", "loaded_at",
    ]
    out = valid.select(*cols).withColumnRenamed("supporting_story_ids_arr", "supporting_story_ids") \
        .withColumnRenamed("keywords_arr", "keywords")
    return out, dq_rows


def build_dim_trend(trends_valid):
    return trends_valid.select(
        F.col("trend_key"),
        F.col("trend_label"),
        F.col("model_version"),
        F.col("prompt_version"),
        F.col("created_at").alias("first_seen_at"),
    ).dropDuplicates(["trend_key"])


# --- Task 17-20: main Silver flow ---

# baca checkpoint Silver (job terpisah dari Bronze)
ckpt = spark.read.jdbc(
    url=JDBC_URL,
    table=(
        "(SELECT job_name, last_successful_completed_at, "
        "last_successful_run_id::text AS last_successful_run_id, updated_at "
        "FROM ops.pipeline_checkpoint WHERE job_name = 'techtrend_silver') t"
    ),
    properties=PROPS,
)

if ckpt.count() == 0:
    watermark_start = "1970-01-01 00:00:00+00"
else:
    ts = ckpt.first()["last_successful_completed_at"]
    watermark_start = ts.strftime("%Y-%m-%d %H:%M:%S+00") if ts else "1970-01-01 00:00:00+00"

we_dt = now_utc()
watermark_end = fmt_ts(we_dt)
prid = str(uuid.uuid4())
print(f"silver pipeline_run_id={prid}  window=[{watermark_start} .. {watermark_end}]")

try:
    insert_pipeline_run(prid, utc_dt(watermark_start), we_dt)

    # pastikan Silver tables ada walau window kosong (CREATE TABLE IF NOT EXISTS)
    print("ensure silver tables:")
    ensure_tables(SILVER_DATASET, SILVER_SPECS)

    ws_dt = utc_dt(watermark_start)
    ws_str = watermark_start.replace("+00", "")

    # guard: jika checkpoint di masa depan (clock skew / bypass bug), reset ke epoch
    if ws_dt > we_dt:
        ws_str = "1970-01-01 00:00:00"
        watermark_start = "1970-01-01 00:00:00+00"
        print("(checkpoint di masa depan — reset ke epoch)")

    # jika Silver tables masih kosong (bypass sebelumnya), proses semua Bronze
    silver_check = bq_read_safe(SILVER_DATASET, "fact_story_snapshot")
    if silver_check is not None and silver_check.count() == 0:
        ws_str = "1970-01-01 00:00:00"
        watermark_start = "1970-01-01 00:00:00+00"
        print("(Silver tables kosong — proses seluruh Bronze data)")

    we_str = watermark_end.replace("+00", "")

    # --- baca Bronze dari BigQuery (bq_read_safe: skip gracefully on permission error) ---
    bronze_stories = bq_read_safe(BRONZE_DATASET, "hn_story_snapshot_raw")
    bronze_comments = bq_read_safe(BRONZE_DATASET, "hn_comment_snapshot_raw")
    bronze_trends = bq_read_safe(BRONZE_DATASET, "trend_analysis_raw")

    # Jika SEMUA bacaan gagal karena permission → FAIL, checkpoint TIDAK maju
    if bronze_stories is None and bronze_comments is None and bronze_trends is None:
        err = "BQ_READ_PERMISSION_DENIED"
        print(f"SILVER SKIP: semua bacaan Bronze gagal (permission/access).")
        print("Tip: grant roles/bigquery.readSessionUser + roles/bigquery.dataViewer")
        print("     ke service account, atau set env BQ_READ_METHOD=export.")
        finalize_pipeline_run(prid, "FAILED", 0, 0, err)
        print("checkpoint TIDAK maju — data Bronze tetap aman untuk run berikutnya")
        spark.stop()
        sys.exit(0)

    # Jika sebagian bacaan gagal → gunakan yang berhasil, catat warning
    if bronze_stories is None or bronze_comments is None or bronze_trends is None:
        missing = [
            name for name, df in [
                ("stories", bronze_stories), ("comments", bronze_comments), ("trends", bronze_trends)
            ] if df is None
        ]
        print(f"PERINGATAN: sebagian Bronze tidak terbaca: {missing}")

    # filter by watermark window (hanya untuk yang berhasil dibaca)
    if bronze_stories is not None:
        bronze_stories = bronze_stories.filter(
            (F.col("loaded_at") > ts_lit(ws_str))
            & (F.col("loaded_at") <= ts_lit(we_str))
        ).cache()
    if bronze_comments is not None:
        bronze_comments = bronze_comments.filter(
            (F.col("loaded_at") > ts_lit(ws_str))
            & (F.col("loaded_at") <= ts_lit(we_str))
        ).cache()
    if bronze_trends is not None:
        bronze_trends = bronze_trends.filter(
            (F.col("loaded_at") > ts_lit(ws_str))
            & (F.col("loaded_at") <= ts_lit(we_str))
        ).cache()

    bronze_counts = {
        "stories": bronze_stories.count() if bronze_stories is not None else 0,
        "comments": bronze_comments.count() if bronze_comments is not None else 0,
        "trends": bronze_trends.count() if bronze_trends is not None else 0,
    }
    bronze_total = sum(bronze_counts.values())
    print(f"bronze rows in window: stories={bronze_counts['stories']} "
          f"comments={bronze_counts['comments']} trends={bronze_counts['trends']}")

    # hanya advance checkpoint jika semua bacaan berhasil tapi window kosong
    if bronze_total == 0:
        if bronze_stories is None or bronze_comments is None or bronze_trends is None:
            err = "BQ_READ_PARTIAL_DENIED"
            finalize_pipeline_run(prid, "FAILED", 0, 0, err)
            print("SILVER SKIP: window kosong + sebagian bacaan gagal — checkpoint TIDAK maju")
            spark.stop()
            sys.exit(0)
        upsert_checkpoint(we_dt, None)
        finalize_pipeline_run(prid, "SUCCEEDED", 0, 0)
        print("SILVER OK  no bronze data — checkpoint advanced")
        spark.stop()
        sys.exit(0)

    # --- transformasi per tabel ---
    stories_valid, dq_stories = transform_stories(bronze_stories)
    comments_valid, dq_comments = transform_comments(bronze_comments)
    trends_valid, dq_trends = transform_trends(bronze_trends, stories_valid)

    # union semua DQ rows (Task 20)
    dq_all = dq_stories
    for extra in (dq_comments, dq_trends):
        dq_all = dq_all.unionByName(extra)
    dq_count = dq_all.count()

    # --- dedup + write Silver (idempotent) ---
    silver_counts = {}
    src_map = {"stories": stories_valid, "comments": comments_valid, "trends": trends_valid}
    for key in ("stories", "comments", "trends"):
        table, keys = SILVER_TABLES[key]
        new_df = dedup_new(src_map[key], table, keys).cache()
        n_new = new_df.count()
        if n_new > 0:
            write_bq(new_df, table)
        silver_counts[key] = n_new
        print(f"silver.{table}: bronze={bronze_counts[key]} new_rows_written={n_new}")

    # dim_trend
    dim_trend_df = build_dim_trend(trends_valid)
    dim_trend_new = dedup_new(dim_trend_df, "dim_trend", ["trend_key"]).cache()
    n_dim = dim_trend_new.count()
    if n_dim > 0:
        write_bq(dim_trend_new, "dim_trend")
    print(f"silver.dim_trend: new_rows_written={n_dim}")

    # data_quality_issue
    if dq_count > 0:
        dq_new = dedup_new(dq_all, "data_quality_issue", SILVER_TABLES["dq"][1]).cache()
        n_dq = dq_new.count()
        if n_dq > 0:
            write_bq(dq_new, "data_quality_issue")
        print(f"silver.data_quality_issue: new_rows_written={n_dq}")
    else:
        n_dq = 0
        print("silver.data_quality_issue: no issues")

    silver_total = sum(silver_counts.values()) + n_dim + n_dq

    # --- rekonsiliasi Bronze -> Silver ---
    print("=== rekonsiliasi bronze -> silver ===")
    mismatches = []
    for key in ("stories", "comments", "trends"):
        table = SILVER_TABLES[key][0]
        b = bronze_counts[key]
        s = silver_counts[key]
        ok = s <= b
        print(f"{table:28s} bronze={b:5d} silver_new={s:5d} {'OK' if ok else 'MISMATCH'}")
        if not ok:
            mismatches.append(table)

    if mismatches:
        finalize_pipeline_run(prid, "FAILED", bronze_total, silver_total, "RECONCILIATION_MISMATCH")
        print(f"REKONSILIASI GAGAL: {mismatches} — checkpoint TIDAK maju")
        sys.exit(1)

    # --- checkpoint advance setelah seluruh Silver write sukses ---
    last_run_id = None
    parts = []
    for df in (bronze_stories, bronze_comments, bronze_trends):
        if df is not None:
            parts.append(df.select("run_id", "loaded_at"))
    if parts:
        max_loaded = parts[0]
        for p in parts[1:]:
            max_loaded = max_loaded.unionByName(p)
        row = max_loaded.orderBy(F.col("loaded_at").desc()).first()
        last_run_id = row["run_id"] if row else None

    upsert_checkpoint(we_dt, last_run_id)
    finalize_pipeline_run(prid, "SUCCEEDED", bronze_total, silver_total)
    print(f"SILVER OK  bronze={bronze_total} silver={silver_total} dq={dq_count} checkpoint-> {watermark_end}")
    spark.stop()

except Exception as e:
    print(f"SILVER GAGAL: {type(e).__name__}: {e}")
    try:
        finalize_pipeline_run(prid, "FAILED", 0, 0, type(e).__name__[:60])
    except Exception as e2:
        print(f"(gagal update pipeline_run status: {e2})")
    sys.exit(1)

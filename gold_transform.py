# gold_transform.py — Task 21-26: Silver -> Gold aggregations, pipeline_run, checkpoint
import os
import sys
import uuid
import psycopg
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from bq_ensure import ensure_tables, GOLD_SPECS

JOB_NAME = "techtrend_gold"
GCP_PROJECT = os.environ["GCP_PROJECT_ID"]
SILVER_DATASET = "techtrend_silver"
GOLD_DATASET = "techtrend_gold"
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
    return (
        spark.read.format("bigquery")
        .option("parentProject", GCP_PROJECT)
        .option("project", GCP_PROJECT)
        .option("dataset", dataset)
        .option("table", table)
        .load()
    )


def bq_read_safe(dataset, table):
    try:
        df = bq_read(dataset, table)
        df.count()
        return df
    except Exception as e:
        print(f"(skip {dataset}.{table}: {type(e).__name__})")
        return None


def write_bq(df, table, mode="overwrite"):
    writer = (
        df.write.format("bigquery")
        .option("parentProject", GCP_PROJECT)
        .option("project", GCP_PROJECT)
        .option("dataset", GOLD_DATASET)
        .option("table", table)
        .option("writeMethod", "direct")
        .option("createDisposition", "CREATE_IF_NEEDED")
    )
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.mode(mode).save()


def write_gold_idempotent(df, table, date_col, affected_dates):
    if not affected_dates:
        return
    try:
        existing = bq_read(GOLD_DATASET, table).filter(
            ~F.col(date_col).isin(affected_dates)
        )
        # align schema: tambah kolom yang missing di existing sebagai null
        new_cols = set(df.columns)
        for c in new_cols - set(existing.columns):
            existing = existing.withColumn(c, F.lit(None))
        # pastikan urutan kolom sama sebelum unionByName
        combined = existing.select(*df.columns).unionByName(df)
    except Exception:
        combined = df
    write_bq(combined, table, "overwrite")


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


def finalize_pipeline_run(prid, status, silver_n, gold_n, error_category=None):
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ops.pipeline_run "
                "SET silver_row_count = %s, gold_row_count = %s, "
                "completed_at = %s, status = %s, error_category = %s "
                "WHERE pipeline_run_id = %s",
                (silver_n, gold_n, now_utc(), status, error_category, uuid.UUID(prid)),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# --- Task 21: trend_daily (grain: date + trend_key) ---

def build_trend_daily(trends_df, stories_df):
    trend_stories = trends_df.select(
        F.to_date("created_at").alias("trend_date"),
        "run_id", "trend_key", "trend_label",
        "sentiment_score", "confidence", "evidence_count",
        F.explode("supporting_story_ids").alias("sid"),
    )
    story_metrics = stories_df.select(
        F.col("run_id"),
        F.col("story_id").cast("string").alias("sid"),
        F.col("score"),
        F.col("descendants"),
    )
    joined = trend_stories.join(story_metrics, ["run_id", "sid"], "left")

    return joined.groupBy("trend_date", "trend_key").agg(
        F.max("trend_label").alias("trend_label"),
        F.countDistinct("sid").alias("story_count"),
        F.sum("evidence_count").alias("total_evidence_count"),
        F.avg("sentiment_score").alias("avg_sentiment"),
        F.when(
            F.sum("confidence") > 0,
            F.sum(F.col("sentiment_score") * F.col("confidence")) / F.sum("confidence"),
        ).otherwise(F.lit(None).cast("double")).alias("weighted_sentiment"),
        F.avg("confidence").alias("avg_confidence"),
        F.sum("score").alias("total_hn_score"),
        F.avg("score").alias("avg_hn_score"),
        F.sum("descendants").alias("total_comments"),
    )


# --- Task 22: story_performance_daily (grain: date + story_id) ---

def build_story_performance(stories_df, trends_df):
    story_daily = stories_df.groupBy(
        F.to_date("collected_at").alias("story_date"),
        "story_id",
    ).agg(
        F.max_by("score", "collected_at").alias("latest_score"),
        F.max_by("descendants", "collected_at").alias("latest_comment_count"),
        F.max("title").alias("title"),
        F.max("url").alias("url"),
        F.max("author").alias("author"),
    )

    # assigned trends per (date, story_id)
    trend_story = trends_df.select(
        F.to_date("created_at").alias("dt"),
        "trend_key",
        F.explode("supporting_story_ids").alias("sid"),
    )
    story_trends = trend_story.groupBy("dt", "sid").agg(
        F.collect_set("trend_key").alias("assigned_trends"),
    )

    # rank within trend (best rank across assigned trends)
    trend_scores = trend_story.join(
        story_daily.select(
            F.col("story_date").alias("dt"),
            F.col("story_id").cast("string").alias("sid"),
            "latest_score",
        ),
        on=["dt", "sid"],
        how="left",
    )
    w_rank = Window.partitionBy("dt", "trend_key").orderBy(
        F.col("latest_score").desc_nulls_last()
    )
    ranks = (
        trend_scores.withColumn("rn", F.row_number().over(w_rank))
        .groupBy("dt", "sid")
        .agg(F.min("rn").alias("best_rank_in_trend"))
    )

    # merge
    story_daily = story_daily.withColumn(
        "sid", F.col("story_id").cast("string")
    ).withColumnRenamed("story_date", "dt")

    story_daily = story_daily.join(story_trends, ["dt", "sid"], "left")
    story_daily = story_daily.join(ranks, ["dt", "sid"], "left")
    story_daily = story_daily.drop("sid").withColumnRenamed("dt", "story_date")
    return story_daily


def add_story_deltas(story_daily_combined, batch_dates):
    """score_delta & comment_delta via lag over consecutive dates."""
    w = Window.partitionBy("story_id").orderBy("story_date")
    result = (
        story_daily_combined.withColumn("prev_score", F.lag("latest_score").over(w))
        .withColumn("prev_comments", F.lag("latest_comment_count").over(w))
        .withColumn(
            "score_delta",
            F.when(F.col("prev_score").isNotNull(), F.col("latest_score") - F.col("prev_score")),
        )
        .withColumn(
            "comment_delta",
            F.when(F.col("prev_comments").isNotNull(), F.col("latest_comment_count") - F.col("prev_comments")),
        )
        .drop("prev_score", "prev_comments")
    )
    return result.filter(F.col("story_date").isin(batch_dates))


# --- Task 23: trend_momentum_daily (grain: date + trend_key) ---

def build_trend_momentum(trend_daily_combined, batch_dates):
    w_trend = Window.partitionBy("trend_key").orderBy("trend_date")
    w_date = Window.partitionBy("trend_date")

    df = (
        trend_daily_combined
        .withColumn("prev_score", F.lag("total_hn_score").over(w_trend))
        .withColumn("prev_comments", F.lag("total_comments").over(w_trend))
        .withColumn("prev_stories", F.lag("story_count").over(w_trend))
        .withColumn(
            "score_growth",
            F.when(F.col("prev_score").isNotNull() & (F.col("prev_score") > 0),
                   (F.col("total_hn_score") - F.col("prev_score")) / F.col("prev_score"))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "comment_growth",
            F.when(F.col("prev_comments").isNotNull() & (F.col("prev_comments") > 0),
                   (F.col("total_comments") - F.col("prev_comments")) / F.col("prev_comments"))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "story_freq_growth",
            F.when(F.col("prev_stories").isNotNull() & (F.col("prev_stories") > 0),
                   (F.col("story_count") - F.col("prev_stories")) / F.col("prev_stories"))
            .otherwise(F.lit(0.0)),
        )
    )

    # normalize each component to [0,1] per date (min-max)
    for raw, norm in [
        ("score_growth", "score_growth_norm"),
        ("comment_growth", "comment_growth_norm"),
        ("story_freq_growth", "story_freq_growth_norm"),
    ]:
        df = (
            df.withColumn(f"{raw}_min", F.min(raw).over(w_date))
            .withColumn(f"{raw}_max", F.max(raw).over(w_date))
            .withColumn(
                norm,
                F.when(
                    F.col(f"{raw}_max") > F.col(f"{raw}_min"),
                    (F.col(raw) - F.col(f"{raw}_min"))
                    / (F.col(f"{raw}_max") - F.col(f"{raw}_min")),
                ).otherwise(F.lit(0.5)),
            )
            .drop(f"{raw}_min", f"{raw}_max")
        )

    df = df.withColumn(
        "trend_momentum",
        F.round(
            F.lit(0.45) * F.col("score_growth_norm")
            + F.lit(0.35) * F.col("comment_growth_norm")
            + F.lit(0.20) * F.col("story_freq_growth_norm"),
            4,
        ),
    )

    cols = [
        "trend_date", "trend_key", "trend_label",
        "score_growth", "comment_growth", "story_freq_growth",
        "score_growth_norm", "comment_growth_norm", "story_freq_growth_norm",
        "trend_momentum",
    ]
    return df.filter(F.col("trend_date").isin(batch_dates)).select(*cols)


# --- Task 24: pipeline_quality_daily (grain: date) ---

def build_pipeline_quality(runs_df, dq_df, trends_df):
    quality = runs_df.groupBy(
        F.to_date("completed_at").alias("quality_date"),
    ).agg(
        F.sum("source_row_count").alias("total_source_rows"),
        F.sum("bronze_row_count").alias("total_bronze_rows"),
        F.sum("silver_row_count").alias("total_silver_rows"),
        F.sum("gold_row_count").alias("total_gold_rows"),
        F.count(F.lit(1)).alias("total_runs"),
        F.count(F.when(F.col("status") == "SUCCEEDED", True)).alias("successful_runs"),
        F.count(F.when(F.col("status") == "FAILED", True)).alias("failed_runs"),
        F.max("completed_at").alias("freshness_timestamp"),
        F.max("pipeline_run_id").alias("last_successful_run_id"),
    )

    # DQ metrics (join DQ with pipeline_run to get date)
    if dq_df is not None:
        dq_dated = dq_df.join(
            runs_df.select("pipeline_run_id", "completed_at"),
            on="pipeline_run_id",
            how="left",
        )
        dq_agg = dq_dated.groupBy(
            F.to_date("completed_at").alias("quality_date"),
        ).agg(
            F.count(F.lit(1)).alias("quarantine_count"),
            F.sum(F.when(F.col("issue_type") == "DELETED_OR_DEAD", 1).otherwise(0)).alias("deleted_dead_count"),
            F.sum(
                F.when(
                    F.col("issue_type").isin(
                        "SENTIMENT_OUT_OF_RANGE",
                        "CONFIDENCE_OUT_OF_RANGE",
                        "INVALID_EVIDENCE_IDS",
                    ),
                    1,
                ).otherwise(0)
            ).alias("invalid_ai_count"),
        )
        quality = quality.join(dq_agg, "quality_date", "left")
    else:
        quality = (
            quality
            .withColumn("quarantine_count", F.lit(0))
            .withColumn("deleted_dead_count", F.lit(0))
            .withColumn("invalid_ai_count", F.lit(0))
        )

    # LLM success rate
    if trends_df is not None:
        trend_counts = trends_df.groupBy(
            F.to_date("loaded_at").alias("quality_date"),
        ).agg(
            F.count(F.lit(1)).alias("total_trends"),
        )
        quality = quality.join(trend_counts, "quality_date", "left")
        quality = quality.withColumn(
            "llm_success_rate",
            F.when(
                F.col("total_trends") > 0,
                F.round(1.0 - (F.col("invalid_ai_count") / F.col("total_trends")), 4),
            ).otherwise(F.lit(1.0)),
        ).drop("total_trends")
    else:
        quality = quality.withColumn("llm_success_rate", F.lit(1.0))

    return quality.na.fill(0, ["quarantine_count", "deleted_dead_count", "invalid_ai_count"])


# --- Task 25-26: main Gold flow ---

# baca checkpoint Gold (job terpisah dari Bronze/Silver)
ckpt = spark.read.jdbc(
    url=JDBC_URL,
    table=(
        "(SELECT job_name, last_successful_completed_at, "
        "last_successful_run_id::text AS last_successful_run_id, updated_at "
        "FROM ops.pipeline_checkpoint WHERE job_name = 'techtrend_gold') t"
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
print(f"gold pipeline_run_id={prid}  window=[{watermark_start} .. {watermark_end}]")

try:
    insert_pipeline_run(prid, utc_dt(watermark_start), we_dt)

    # pastikan Gold tables ada walau Silver kosong (CREATE TABLE IF NOT EXISTS)
    print("ensure gold tables:")
    ensure_tables(GOLD_DATASET, GOLD_SPECS)

    ws_dt = utc_dt(watermark_start)
    ws_str = watermark_start.replace("+00", "")

    # guard: jika checkpoint di masa depan (clock skew), reset ke epoch
    if ws_dt > we_dt:
        ws_str = "1970-01-01 00:00:00"
        watermark_start = "1970-01-01 00:00:00+00"
        print("(checkpoint di masa depan — reset ke epoch)")

    we_str = watermark_end.replace("+00", "")

    # --- baca Silver dari BigQuery (dataset techtrend_silver) ---
    stories_silver = bq_read_safe(SILVER_DATASET, "fact_story_snapshot")
    trends_silver = bq_read_safe(SILVER_DATASET, "fact_trend_sentiment")
    dq_silver = bq_read_safe(SILVER_DATASET, "data_quality_issue")

    # filter by Gold watermark window
    if stories_silver is not None:
        stories_silver = stories_silver.filter(
            (F.col("loaded_at") > ts_lit(ws_str))
            & (F.col("loaded_at") <= ts_lit(we_str))
        ).cache()
    if trends_silver is not None:
        trends_silver = trends_silver.filter(
            (F.col("loaded_at") > ts_lit(ws_str))
            & (F.col("loaded_at") <= ts_lit(we_str))
        ).cache()

    stories_n = stories_silver.count() if stories_silver is not None else 0
    trends_n = trends_silver.count() if trends_silver is not None else 0
    silver_total = stories_n + trends_n
    print(f"silver rows in window: stories={stories_n} trends={trends_n}")

    if silver_total == 0:
        upsert_checkpoint(we_dt, None)
        finalize_pipeline_run(prid, "SUCCEEDED", 0, 0)
        print("GOLD OK  no silver data — checkpoint advanced")
        spark.stop()
        sys.exit(0)

    # --- baca pipeline_run dari PostgreSQL untuk quality metrics ---
    runs_df = spark.read.jdbc(
        url=JDBC_URL,
        table=(
            "(SELECT pipeline_run_id::text AS pipeline_run_id, job_name, "
            "source_row_count, bronze_row_count, silver_row_count, gold_row_count, "
            "watermark_start, watermark_end, started_at, completed_at, status, error_category "
            "FROM ops.pipeline_run "
            f"WHERE completed_at > '{watermark_start}'::timestamptz "
            f"AND completed_at <= '{watermark_end}'::timestamptz) t"
        ),
        properties=PROPS,
    ).cache()
    print(f"pipeline_runs in window: {runs_df.count()}")

    # --- Task 21+23: trend_daily (with trend_momentum joined from momentum calc) ---
    trend_daily_new = build_trend_daily(trends_silver, stories_silver).cache()
    trend_dates = [r.trend_date for r in trend_daily_new.select("trend_date").distinct().collect()]

    # baca existing trend_daily (drop trend_momentum — akan dihitung ulang)
    existing_td = bq_read_safe(GOLD_DATASET, "trend_daily")
    if existing_td is not None:
        existing_td_base = existing_td.drop("trend_momentum").filter(
            ~F.col("trend_date").isin(trend_dates)
        )
        trend_daily_combined = existing_td_base.unionByName(trend_daily_new)
    else:
        trend_daily_combined = trend_daily_new

    # Task 23: hitung momentum dari combined (butuh historical untuk lag)
    momentum_df = build_trend_momentum(trend_daily_combined, trend_dates).cache()
    mom_n = momentum_df.count()
    print(f"gold.trend_momentum_daily: {mom_n} rows")
    write_gold_idempotent(momentum_df, "trend_momentum_daily", "trend_date", trend_dates)

    # join trend_momentum ke trend_daily_new
    trend_daily_final = trend_daily_new.join(
        momentum_df.select("trend_date", "trend_key", "trend_momentum"),
        on=["trend_date", "trend_key"],
        how="left",
    ).cache()
    td_n = trend_daily_final.count()
    print(f"gold.trend_daily: {td_n} rows (with trend_momentum)")
    write_gold_idempotent(trend_daily_final, "trend_daily", "trend_date", trend_dates)

    # --- Task 22: story_performance_daily ---
    story_perf_new = build_story_performance(stories_silver, trends_silver).cache()
    story_dates = [r.story_date for r in story_perf_new.select("story_date").distinct().collect()]

    # baca existing untuk delta lag
    existing_sp = bq_read_safe(GOLD_DATASET, "story_performance_daily")
    if existing_sp is not None:
        existing_sp = existing_sp.drop("score_delta", "comment_delta")
        sp_combined = existing_sp.filter(
            ~F.col("story_date").isin(story_dates)
        ).unionByName(story_perf_new)
    else:
        sp_combined = story_perf_new

    # hitung delta (butuh hari sebelumnya dari combined)
    story_perf_final = add_story_deltas(sp_combined, story_dates).cache()
    sp_n = story_perf_final.count()
    print(f"gold.story_performance_daily: {sp_n} rows")
    write_gold_idempotent(story_perf_final, "story_performance_daily", "story_date", story_dates)

    # --- Task 24: pipeline_quality_daily ---
    quality_df = build_pipeline_quality(runs_df, dq_silver, trends_silver).cache()
    quality_dates = [r.quality_date for r in quality_df.select("quality_date").distinct().collect()]
    q_n = quality_df.count()
    print(f"gold.pipeline_quality_daily: {q_n} rows")
    write_gold_idempotent(quality_df, "pipeline_quality_daily", "quality_date", quality_dates)

    gold_total = td_n + mom_n + sp_n + q_n
    print(f"=== gold summary: trend_daily={td_n} momentum={mom_n} "
          f"story_perf={sp_n} quality={q_n} total={gold_total} ===")

    # --- Task 25: checkpoint advance setelah seluruh Gold write sukses ---
    last_run_id = None
    max_row = trends_silver.select("run_id", "loaded_at").unionByName(
        stories_silver.select("run_id", "loaded_at")
    ).orderBy(F.col("loaded_at").desc()).first()
    if max_row:
        last_run_id = max_row["run_id"]

    upsert_checkpoint(we_dt, last_run_id)
    finalize_pipeline_run(prid, "SUCCEEDED", silver_total, gold_total)
    print(f"GOLD OK  silver={silver_total} gold={gold_total} checkpoint-> {watermark_end}")
    spark.stop()

except Exception as e:
    print(f"GOLD GAGAL: {type(e).__name__}: {e}")
    try:
        finalize_pipeline_run(prid, "FAILED", 0, 0, type(e).__name__[:60])
    except Exception as e2:
        print(f"(gagal update pipeline_run status: {e2})")
    sys.exit(1)

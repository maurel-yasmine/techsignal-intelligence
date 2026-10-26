# phase3_integration.py — Phase 3: integration verification & failure simulation
# Task 10-13: verify Bronze/Silver/Gold from a run_id
# Task 16-17: verify checkpoint safety after simulated failure + retry
#
# Penggunaan:
#   python phase3_integration.py <run_id>
#   python phase3_integration.py            (auto: latest run_id)
import os
import sys
import psycopg
from datetime import datetime, timezone
from google.cloud import bigquery

PROJECT = os.environ["GCP_PROJECT_ID"]
PG_HOST = os.environ["PG_POOLER_HOST"]
PG_USER = "postgres.usapduupljixqwcystla"
PG_PASSWORD = os.environ["PG_SPARK_PASSWORD"]
_PG_REF = PG_USER.split(".", 1)[1] if "." in PG_USER else ""

BRONZE = "techtrend_bronze"
SILVER = "techtrend_silver"
GOLD = "techtrend_gold"

bq = bigquery.Client(project=PROJECT)


def pg_conn():
    opts = f"-c timezone=UTC -c endpoint={_PG_REF}"
    return psycopg.connect(
        host=PG_HOST, port=5432, dbname="postgres",
        user=PG_USER, password=PG_PASSWORD,
        sslmode="require", connect_timeout=30, options=opts,
    )


def pg_query(sql):
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            return cols, rows
    finally:
        conn.close()


def bq_count(table, where=""):
    sql = f"SELECT COUNT(*) AS n FROM `{PROJECT}.{table}`"
    if where:
        sql += f" WHERE {where}"
    return list(bq.query(sql).result())[0]["n"]


def bq_query(sql):
    return list(bq.query(sql).result())


def print_row(label, value):
    print(f"  {label:45s} = {value}")


def print_table(cols, rows, max_rows=5):
    print(f"  {' | '.join(cols)}")
    for r in rows[:max_rows]:
        print(f"  {' | '.join(str(v) for v in r)}")
    if len(rows) > max_rows:
        print(f"  ... ({len(rows)} rows total)")


def get_run_id():
    if len(sys.argv) > 1:
        return sys.argv[1]
    cols, rows = pg_query(
        "SELECT run_id::text FROM ops.collection_run "
        "WHERE status IN ('SUCCEEDED','PARTIAL') "
        "ORDER BY completed_at DESC LIMIT 1"
    )
    if not rows:
        print("ERROR: tidak ada collection_run SUCCEEDED/PARTIAL di PostgreSQL")
        sys.exit(1)
    return rows[0][0]


# =====================================================================
# Task 10-13: verify pipeline dari run_id
# =====================================================================

def task10_run_spark_batch(run_id):
    print("\n=== Task 10: Spark batch dari checkpoint ===")
    cols, rows = pg_query(
        "SELECT job_name, status, bronze_row_count, silver_row_count, gold_row_count, "
        "watermark_start, watermark_end, completed_at "
        "FROM ops.pipeline_run "
        "ORDER BY started_at DESC LIMIT 6"
    )
    print_table(cols, rows)
    # cek semua job SUCCEEDED
    failed = [r for r in rows if r[1] != "SUCCEEDED"]
    if failed:
        print(f"  WARNING: {len(failed)} pipeline_run tidak SUCCEEDED")
    else:
        print("  OK: semua pipeline_run terbaru SUCCEEDED")


def task11_bronze_reconcile(run_id):
    print(f"\n=== Task 11: Bronze reconciliation (run_id={run_id}) ===")
    # source count dari PostgreSQL
    _, src_rows = pg_query(
        f"SELECT "
        f"(SELECT COUNT(*) FROM raw.hn_story_snapshot WHERE run_id::text = '{run_id}') AS stories, "
        f"(SELECT COUNT(*) FROM raw.hn_comment_snapshot WHERE run_id::text = '{run_id}') AS comments, "
        f"(SELECT COUNT(*) FROM ai.trend_analysis WHERE run_id::text = '{run_id}') AS trends"
    )
    pg_stories, pg_comments, pg_trends = src_rows[0]

    # bronze count dari BigQuery
    bq_stories = bq_count(f"{BRONZE}.hn_story_snapshot_raw", f"run_id = '{run_id}'")
    bq_comments = bq_count(f"{BRONZE}.hn_comment_snapshot_raw", f"run_id = '{run_id}'")
    bq_trends = bq_count(f"{BRONZE}.trend_analysis_raw", f"run_id = '{run_id}'")

    print_row("stories  (PG → Bronze)", f"{pg_stories} → {bq_stories}  {'OK' if pg_stories == bq_stories else 'MISMATCH'}")
    print_row("comments (PG → Bronze)", f"{pg_comments} → {bq_comments}  {'OK' if pg_comments == bq_comments else 'MISMATCH'}")
    print_row("trends   (PG → Bronze)", f"{pg_trends} → {bq_trends}  {'OK' if pg_trends == bq_trends else 'MISMATCH'}")

    return pg_stories == bq_stories and pg_comments == bq_comments and pg_trends == bq_trends


def task12_silver_evidence(run_id):
    print(f"\n=== Task 12: Silver evidence validation (run_id={run_id}) ===")
    # cek trend di Silver
    silver_trends = bq_count(f"{SILVER}.fact_trend_sentiment", f"run_id = '{run_id}'")
    print_row("Silver fact_trend_sentiment rows", silver_trends)

    # cek evidence IDs: supporting_story_ids harus ada di fact_story_snapshot
    rows = bq_query(f"""
        SELECT t.trend_key, t.sentiment_score, t.confidence, t.evidence_count,
               t.supporting_story_ids
        FROM `{PROJECT}.{SILVER}.fact_trend_sentiment` t
        WHERE t.run_id = '{run_id}'
    """)
    if not rows:
        print("  SKIP: tidak ada trend di Silver untuk run_id ini")
        return True

    story_ids = set()
    srows = bq_query(f"""
        SELECT story_id FROM `{PROJECT}.{SILVER}.fact_story_snapshot`
        WHERE run_id = '{run_id}'
    """)
    for r in srows:
        story_ids.add(str(r["story_id"]))

    all_valid = True
    for r in rows:
        sup = r["supporting_story_ids"] or []
        sup_set = set(str(s) for s in sup)
        missing = sup_set - story_ids
        status = "OK" if not missing else f"EVIDENCE MISMATCH: {missing}"
        if missing:
            all_valid = False
        print(f"  {r['trend_key']:25s} sentiment={r['sentiment_score']:.2f} "
              f"conf={r['confidence']:.2f} evidence={r['evidence_count']} {status}")

    # cek DQ issues untuk run ini
    dq = bq_count(f"{SILVER}.data_quality_issue", f"pipeline_run_id IS NOT NULL")
    print_row("data_quality_issue total", dq)

    return all_valid


def task13_gold_momentum(run_id):
    print(f"\n=== Task 13: Gold momentum verification ===")
    rows = bq_query(f"""
        SELECT trend_key, trend_momentum,
               score_growth_norm, comment_growth_norm, story_freq_growth_norm
        FROM `{PROJECT}.{GOLD}.trend_momentum_daily`
        ORDER BY trend_date DESC, trend_momentum DESC
        LIMIT 10
    """)
    if not rows:
        print("  SKIP: tidak ada data di trend_momentum_daily")
        return True

    all_ok = True
    for r in rows:
        expected = round(
            0.45 * (r["score_growth_norm"] or 0)
            + 0.35 * (r["comment_growth_norm"] or 0)
            + 0.20 * (r["story_freq_growth_norm"] or 0), 4
        )
        actual = r["trend_momentum"]
        ok = abs(expected - actual) < 0.001
        if not ok:
            all_ok = False
        print(f"  {r['trend_key']:25s} momentum={actual:.4f} expected={expected:.4f} {'OK' if ok else 'DRIFT'}")

    # cek trend_daily punya trend_momentum column
    try:
        bq_query(f"SELECT trend_momentum FROM `{PROJECT}.{GOLD}.trend_daily` LIMIT 1")
        print("  trend_daily.trend_momentum column: OK")
    except Exception:
        print("  trend_daily.trend_momentum column: MISSING")
        all_ok = False

    return all_ok


def task14_tableau_trace(run_id):
    print(f"\n=== Task 14: Tableau trace (manual) ===")
    # dapatkan trend_key dari run_id
    rows = bq_query(f"""
        SELECT trend_key, trend_label, avg_sentiment, trend_momentum
        FROM `{PROJECT}.{GOLD}.trend_daily`
        WHERE trend_key IN (
            SELECT DISTINCT trend_key
            FROM `{PROJECT}.{SILVER}.fact_trend_sentiment`
            WHERE run_id = '{run_id}'
        )
        ORDER BY trend_date DESC LIMIT 5
    """)
    if not rows:
        print("  Tidak ada trend di Gold untuk run_id ini.")
        print("  Pastikan Spark Silver + Gold sudah dijalankan untuk run_id ini.")
        return

    print("  Trend yang harus muncul di Tableau Dashboard 1:")
    for r in rows:
        print(f"    trend_key={r['trend_key']}  label={r['trend_label']}  "
              f"sentiment={r['avg_sentiment']:.2f}  momentum={r['trend_momentum']}")

    print("\n  Langkah manual:")
    print("  1. Buka Tableau Dashboard 1 (Technology Trend Overview)")
    print("  2. Cari trend_key di atas di bar chart 'Top trends by momentum'")
    print("  3. Buka Dashboard 2 (Trend & Sentiment Drilldown)")
    print("  4. Pilih trend_key di filter dropdown")
    print("  5. Verifikasi supporting stories muncul di bar chart")
    print("  6. Buka Dashboard 3 (Pipeline & AI Quality)")
    print("  7. Verifikasi freshness timestamp <= 2 jam yang lalu")


# =====================================================================
# Task 15-17: failure simulation & retry safety
# =====================================================================

def task15_16_17_checkpoint_safety():
    print("\n=== Task 15-17: checkpoint & retry safety ===")
    cols, rows = pg_query(
        "SELECT job_name, last_successful_completed_at, "
        "last_successful_run_id::text, updated_at "
        "FROM ops.pipeline_checkpoint ORDER BY job_name"
    )
    print_table(cols, rows)

    # verifikasi: tidak ada duplicate keys di Bronze/Silver/Gold
    print("\n  --- duplicate check ---")
    checks = [
        ("Bronze stories", f"SELECT run_id, story_id, COUNT(*) AS n "
         f"FROM `{PROJECT}.{BRONZE}.hn_story_snapshot_raw` "
         f"GROUP BY run_id, story_id HAVING COUNT(*) > 1 LIMIT 5"),
        ("Silver stories", f"SELECT run_id, story_id, COUNT(*) AS n "
         f"FROM `{PROJECT}.{SILVER}.fact_story_snapshot` "
         f"GROUP BY run_id, story_id HAVING COUNT(*) > 1 LIMIT 5"),
        ("Silver trends", f"SELECT run_id, trend_key, COUNT(*) AS n "
         f"FROM `{PROJECT}.{SILVER}.fact_trend_sentiment` "
         f"GROUP BY run_id, trend_key HAVING COUNT(*) > 1 LIMIT 5"),
        ("Gold trend_daily", f"SELECT trend_date, trend_key, COUNT(*) AS n "
         f"FROM `{PROJECT}.{GOLD}.trend_daily` "
         f"GROUP BY trend_date, trend_key HAVING COUNT(*) > 1 LIMIT 5"),
    ]
    all_clean = True
    for label, sql in checks:
        dups = bq_query(sql)
        if dups:
            print(f"  {label}: DUPLICATES FOUND ({len(dups)} groups)")
            all_clean = False
        else:
            print(f"  {label}: no duplicates (idempotent OK)")
    return all_clean


def task18_final_trace(run_id):
    print(f"\n=== Task 18: Final trace end-to-end (run_id={run_id}) ===")
    print("  Trace chain: Telegram → run_id → story_id → analysis_id → pipeline_run_id → trend_key → Tableau")

    # step 1: collection_run
    cols, rows = pg_query(
        f"SELECT run_id::text, trigger_type, status, stories_loaded, "
        f"comments_loaded, trends_loaded, started_at, completed_at "
        f"FROM ops.collection_run WHERE run_id::text = '{run_id}'"
    )
    print(f"\n  [PostgreSQL] ops.collection_run:")
    print_table(cols, rows)

    # step 2: story IDs
    cols, rows = pg_query(
        f"SELECT story_id, title, score FROM raw.hn_story_snapshot "
        f"WHERE run_id::text = '{run_id}' LIMIT 3"
    )
    print(f"\n  [PostgreSQL] raw.hn_story_snapshot:")
    print_table(cols, rows)

    # step 3: trend analysis
    cols, rows = pg_query(
        f"SELECT trend_key, analysis_id, sentiment_label, sentiment_score "
        f"FROM ai.trend_analysis WHERE run_id::text = '{run_id}' LIMIT 5"
    )
    print(f"\n  [PostgreSQL] ai.trend_analysis:")
    print_table(cols, rows)

    if rows:
        trend_key = rows[0][0]
        analysis_id = rows[0][1]

        # step 4: Bronze
        n_bronze = bq_count(f"{BRONZE}.trend_analysis_raw", f"run_id = '{run_id}'")
        print(f"\n  [BigQuery Bronze] trend_analysis_raw: {n_bronze} rows for run_id={run_id}")

        # step 5: Silver
        n_silver = bq_count(f"{SILVER}.fact_trend_sentiment", f"run_id = '{run_id}'")
        print(f"  [BigQuery Silver] fact_trend_sentiment: {n_silver} rows for run_id={run_id}")

        # step 6: Gold
        gold_rows = bq_query(f"""
            SELECT trend_date, trend_key, story_count, avg_sentiment, trend_momentum
            FROM `{PROJECT}.{GOLD}.trend_daily`
            WHERE trend_key = '{trend_key}'
            ORDER BY trend_date DESC LIMIT 3
        """)
        print(f"  [BigQuery Gold] trend_daily for trend_key='{trend_key}':")
        for r in gold_rows:
            print(f"    date={r['trend_date']}  stories={r['story_count']}  "
                  f"sentiment={r['avg_sentiment']:.2f}  momentum={r['trend_momentum']}")

        print(f"\n  CHAIN COMPLETE:")
        print(f"    run_id={run_id}")
        print(f"    → analysis_id={analysis_id}")
        print(f"    → trend_key={trend_key}")
        print(f"    → Gold trend_momentum={'present' if gold_rows else 'missing'}")
        print(f"    → Tableau: buka Dashboard 1, cari '{trend_key}' di momentum bar")
    else:
        print("\n  Tidak ada trend_analysis untuk run_id ini.")


# =====================================================================
# Main
# =====================================================================

def main():
    run_id = get_run_id()
    print(f"Phase 3 Integration Verification")
    print(f"run_id = {run_id}")
    print(f"project = {PROJECT}")
    print("=" * 70)

    task10_run_spark_batch(run_id)
    ok11 = task11_bronze_reconcile(run_id)
    ok12 = task12_silver_evidence(run_id)
    ok13 = task13_gold_momentum(run_id)
    task14_tableau_trace(run_id)
    ok_dup = task15_16_17_checkpoint_safety()
    task18_final_trace(run_id)

    print("\n" + "=" * 70)
    print("=== Phase 3 Summary ===")
    print(f"  Task 11 (Bronze reconcile):   {'PASS' if ok11 else 'FAIL'}")
    print(f"  Task 12 (Silver evidence):    {'PASS' if ok12 else 'FAIL'}")
    print(f"  Task 13 (Gold momentum):      {'PASS' if ok13 else 'FAIL'}")
    print(f"  Task 15-17 (no duplicates):   {'PASS' if ok_dup else 'FAIL'}")
    print(f"\n  Task 14 (Tableau trace):      MANUAL — lihat instruksi di atas")
    print(f"  Task 15 (LLM timeout sim):    MANUAL — lihat RUNBOOK Section 5.1")
    print(f"  Task 16 (Spark fail sim):     MANUAL — stop bronze_write.py mid-run, retry")
    print(f"  Task 18 (final trace):        selesai di atas")

    all_pass = ok11 and ok12 and ok13 and ok_dup
    if all_pass:
        print("\n  >>> Phase 3 automated checks: ALL PASS <<<")
        print("  >>> Lanjut: manual Tableau trace + failure simulation <<<")
    else:
        print("\n  >>> Beberapa check FAIL — perbaiki sebelum finalisasi <<<")
        sys.exit(1)


if __name__ == "__main__":
    main()

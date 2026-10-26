# bq_verify.py — Task 27 prerequisite: verifikasi Gold tables siap untuk Tableau connect
import os
import sys
from google.cloud import bigquery

PROJECT = os.environ["GCP_PROJECT_ID"]
GOLD = "techtrend_gold"
SILVER = "techtrend_silver"
BRONZE = "techtrend_bronze"

client = bigquery.Client(project=PROJECT)

GOLD_TABLES = {
    "trend_daily": ["trend_date", "trend_key", "story_count", "avg_sentiment", "trend_momentum"],
    "story_performance_daily": ["story_date", "story_id", "latest_score", "score_delta"],
    "trend_momentum_daily": ["trend_date", "trend_key", "trend_momentum", "score_growth_norm"],
    "pipeline_quality_daily": ["quality_date", "total_source_rows", "llm_success_rate"],
}

SILVER_TABLES = {
    "fact_story_snapshot": ["run_id", "story_id", "score"],
    "fact_comment_snapshot": ["run_id", "comment_id"],
    "fact_trend_sentiment": ["run_id", "trend_key", "sentiment_score"],
    "data_quality_issue": ["issue_type", "severity"],
}

BRONZE_TABLES = {
    "hn_story_snapshot_raw": ["run_id", "story_id"],
    "hn_comment_snapshot_raw": ["run_id", "comment_id"],
    "trend_analysis_raw": ["run_id", "trend_key"],
    "collection_run_raw": ["run_id"],
}


def check_layer(dataset_name, tables):
    print(f"\n=== {dataset_name} ===")
    ref = bigquery.DatasetReference(PROJECT, dataset_name)
    try:
        ds = client.get_dataset(ref)
        print(f"  dataset location: {ds.location}")
    except Exception as e:
        print(f"  DATASET MISSING: {type(e).__name__}")
        return False

    all_ok = True
    for table, required_cols in tables.items():
        tref = bigquery.TableReference(ref, table)
        try:
            tbl = client.get_table(tref)
            actual_cols = {f.name for f in tbl.schema}
            missing = set(required_cols) - actual_cols
            rows = client.query(
                f"SELECT COUNT(*) AS n FROM `{PROJECT}.{dataset_name}.{table}`"
            ).result()
            n = list(rows)[0]["n"]
            status = "OK" if not missing else f"MISSING COLS: {missing}"
            print(f"  {table:30s} rows={n:>8d}  cols={len(actual_cols):>3d}  {status}")
            if missing:
                all_ok = False
        except Exception as e:
            print(f"  {table:30s} TABLE MISSING: {type(e).__name__}")
            all_ok = False
    return all_ok


def freshness_check():
    print("\n=== freshness ===")
    for ds, tbl, col in [
        (BRONZE, "hn_story_snapshot_raw", "loaded_at"),
        (SILVER, "fact_story_snapshot", "loaded_at"),
        (GOLD, "pipeline_quality_daily", "freshness_timestamp"),
    ]:
        try:
            rows = client.query(
                f"SELECT MAX({col}) AS latest FROM `{PROJECT}.{ds}.{tbl}`"
            ).result()
            latest = list(rows)[0]["latest"]
            print(f"  {ds}.{tbl:30s} latest {col}: {latest}")
        except Exception as e:
            print(f"  {ds}.{tbl:30s} {type(e).__name__}")


def momentum_check():
    print("\n=== momentum formula check ===")
    try:
        rows = client.query(f"""
            SELECT trend_key, trend_momentum,
                   score_growth_norm, comment_growth_norm, story_freq_growth_norm
            FROM `{PROJECT}.{GOLD}.trend_momentum_daily`
            ORDER BY trend_momentum DESC
            LIMIT 5
        """).result()
        for r in rows:
            expected = round(
                0.45 * r["score_growth_norm"]
                + 0.35 * r["comment_growth_norm"]
                + 0.20 * r["story_freq_growth_norm"], 4
            )
            ok = abs(expected - r["trend_momentum"]) < 0.001
            print(f"  {r['trend_key']:25s} momentum={r['trend_momentum']:.4f} "
                  f"expected={expected:.4f} {'OK' if ok else 'DRIFT'}")
    except Exception as e:
        print(f"  {type(e).__name__}: {e}")


def main():
    print(f"project: {PROJECT}")
    ok_bronze = check_layer(BRONZE, BRONZE_TABLES)
    ok_silver = check_layer(SILVER, SILVER_TABLES)
    ok_gold = check_layer(GOLD, GOLD_TABLES)
    freshness_check()
    momentum_check()

    print("\n=== summary ===")
    print(f"  bronze: {'PASS' if ok_bronze else 'FAIL'}")
    print(f"  silver: {'PASS' if ok_silver else 'FAIL'}")
    print(f"  gold:   {'PASS' if ok_gold else 'FAIL'}")
    if ok_gold:
        print("\nGold tables siap untuk Tableau connection.")
        print(f"Connect Tableau ke: {PROJECT}.{GOLD}")
    else:
        print("\nGold tables belum lengkap — jalankan ulang pipeline.")
        sys.exit(1)


if __name__ == "__main__":
    main()

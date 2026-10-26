# RUNBOOK — AI Tech Trend & Sentiment Tracker

> Medallion Architecture: Hacker News → PostgreSQL → Spark → BigQuery (Bronze/Silver/Gold) → Tableau
> Owner: Maurel (Data Engineering)

---

## 1. Arsitektur & Komponen

```
Telegram /run → Gary's collector → PostgreSQL (ops/raw/ai)
                                       ↓
        bronze_write.py (Spark) ──→ BigQuery techtrend_bronze
                                       ↓
        silver_transform.py ──────→ BigQuery techtrend_silver
                                       ↓
        gold_transform.py ────────→ BigQuery techtrend_gold
                                       ↓
                                    Tableau dashboards
```

| Script | Job name | Input | Output |
|---|---|---|---|
| `bronze_write.py` | `techtrend_hourly` | PostgreSQL `ops/raw/ai` | `techtrend_bronze` (4 tables) |
| `silver_transform.py` | `techtrend_silver` | `techtrend_bronze` | `techtrend_silver` (5 tables) |
| `gold_transform.py` | `techtrend_gold` | `techtrend_silver` | `techtrend_gold` (4 tables) |

Setiap job mencatat: `ops.pipeline_run` (RUNNING → SUCCEEDED/FAILED) dan `ops.pipeline_checkpoint` (maju hanya jika semua write sukses).

---

## 2. Environment Variables

| Variable | Keterangan |
|---|---|
| `GCP_PROJECT_ID` | `techtrend-dev` |
| `PG_POOLER_HOST` | Supabase connection pooler host |
| `PG_SPARK_PASSWORD` | Password user `postgres.<ref>` |
| `GOOGLE_APPLICATION_CREDENTIALS` | `D:\PROJECT\secret\spark-techtrend-key.json` |
| `BQ_READ_METHOD` | `direct` (default) atau `export` jika permission Read API bermasalah |

Verifikasi sebelum run:
```
echo $env:GCP_PROJECT_ID, $env:PG_POOLER_HOST, $env:GOOGLE_APPLICATION_CREDENTIALS
```

---

## 3. Urutan Menjalankan Pipeline

```powershell
# 1. Bronze — ekstrak run baru dari PostgreSQL
python bronze_write.py

# 2. Silver — cleaning, dedup, validasi LLM
python silver_transform.py

# 3. Gold — agregasi analitik
python gold_transform.py

# 4. Verifikasi ketiga layer
python bq_verify.py
```

Urutan wajib: Bronze → Silver → Gold. Checkpoint per job terpisah, jadi satu job bisa gagal tanpa merusak job sebelumnya.

---

## 4. Checkpoint & Watermark

### 4.1 Cek status checkpoint

```sql
SELECT job_name, last_successful_completed_at, last_successful_run_id, updated_at
FROM ops.pipeline_checkpoint ORDER BY job_name;
```

### 4.2 Cek riwayat pipeline_run

```sql
SELECT pipeline_run_id, job_name, status, error_category,
       source_row_count, bronze_row_count, silver_row_count, gold_row_count,
       started_at, completed_at
FROM ops.pipeline_run
ORDER BY started_at DESC LIMIT 10;
```

### 4.3 Reset checkpoint (HATI-HATI)

Hanya jika perlu memproses ulang data dari awal:

```sql
-- reset Silver (proses ulang semua Bronze)
DELETE FROM ops.pipeline_checkpoint WHERE job_name = 'techtrend_silver';

-- reset Gold saja
DELETE FROM ops.pipeline_checkpoint WHERE job_name = 'techtrend_gold';
```

Setelah reset, jalankan ulang job terkait. Silver/Gold idempotent — dedup via anti-join mencegah duplikat.

---

## 5. Recovery Skenario

### 5.1 Job FAILED — error_category umum

| error_category | Penyebab | Recovery |
|---|---|---|
| `BQ_READ_PERMISSION_DENIED` | SA tidak punya `bigquery.readsessions.create` | Grant `roles/bigquery.readSessionUser` + `roles/bigquery.dataViewer` (lihat grant_iam.py) |
| `BQ_READ_PARTIAL_DENIED` | Sebagian Bronze table tidak terbaca + window kosong | Sama di atas; checkpoint tidak maju, data aman |
| `RECONCILIATION_MISMATCH` | Bronze/Silver count tidak konsisten | Cek log per-table; biasanya retry job sukses |
| Exception name lain | Lihat log print `PIPELINE GAGAL` | Cek traceback, fix, retry |

### 5.2 Retry safety

- Bronze: anti-join vs existing keys → tidak duplikat.
- Silver: `dropDuplicates` + anti-join → tidak duplikat.
- Gold: overwrite per affected date → tidak duplikat.
- Checkpoint hanya maju setelah SEMUA write + rekonsiliasi sukses.

Jadi **retry aman kapan pun**: cukup jalankan ulang script yang gagal.

### 5.3 PicklingError / RecursionError di Windows

Sudah diperbaiki dengan pola `main()` + `if __name__ == "__main__"` + `spark` sebagai parameter eksplisit (lihat `bronze_write.py`). Jika script baru dibuat, ikuti pola yang sama.

### 5.4 BigQuery permission — grant manual

Jika `grant_iam.py` gagal (SA tidak bisa edit IAM sendiri):
1. Buka https://console.cloud.google.com/iam-admin/iam?project=techtrend-dev
2. GRANT ACCESS → principal: `spark-techtrend@techtrend-dev.iam.gserviceaccount.com`
3. Roles: `BigQuery Read Session User` + `BigQuery Data Viewer`

---

## 6. BigQuery Datasets & Tables

### 6.1 techtrend_bronze
| Table | Grain | Keys |
|---|---|---|
| `hn_story_snapshot_raw` | 1 story per run | `run_id, story_id` |
| `hn_comment_snapshot_raw` | 1 comment per run | `run_id, comment_id` |
| `trend_analysis_raw` | 1 trend per run | `run_id, trend_key` |
| `collection_run_raw` | 1 run | `run_id` |

### 6.2 techtrend_silver
| Table | Isi |
|---|---|
| `fact_story_snapshot` | Story typed + cleaned + `story_age_hours` |
| `fact_comment_snapshot` | Comment HTML-cleaned |
| `fact_trend_sentiment` | LLM output validated + JSON arrays flattened |
| `dim_trend` | Master trend (`trend_key` unik) |
| `data_quality_issue` | Karantina: `NULL_KEY`, `DELETED_OR_DEAD`, `EMPTY_TEXT`, `SENTIMENT_OUT_OF_RANGE`, `CONFIDENCE_OUT_OF_RANGE`, `INVALID_EVIDENCE_IDS` |

### 6.3 techtrend_gold
| Table | Grain | Metrics utama |
|---|---|---|
| `trend_daily` | date + trend_key | story_count, avg_sentiment, weighted_sentiment, avg_confidence, total/avg_hn_score, total_comments, total_evidence_count |
| `story_performance_daily` | date + story_id | latest_score, score_delta, latest_comment_count, comment_delta, assigned_trends, best_rank_in_trend |
| `trend_momentum_daily` | date + trend_key | trend_momentum = 0.45×score_growth_norm + 0.35×comment_growth_norm + 0.20×story_freq_growth_norm (komponen disimpan terpisah) |
| `pipeline_quality_daily` | date | source/bronze/silver/gold counts, quarantine_count, invalid_ai_count, llm_success_rate, freshness_timestamp |

---

## 7. Tableau Connection (Task 27)

### 7.1 Prasyarat
1. Jalankan `python bq_verify.py` — semua layer PASS.
2. Service account Tableau (bisa sama dengan Spark SA, prinsip least-privilege: cukup `roles/bigquery.dataViewer` + `roles/bigquery.jobUser`).

### 7.2 Steps — Tableau Desktop
1. **Connect → Google BigQuery**
2. Sign in dengan akun Google yang punya akses `techtrend-dev` (atau gunakan OAuth service account di Tableau Server).
3. Pilih project `techtrend-dev` → dataset `techtrend_gold`.
4. **Tambahkan 4 tables**: `trend_daily`, `story_performance_daily`, `trend_momentum_daily`, `pipeline_quality_daily`.
5. **Jangan** connect langsung ke `techtrend_bronze` atau raw PostgreSQL.

### 7.3 Join & Relationship
- `trend_daily` ↔ `trend_momentum_daily`: join on `trend_date + trend_key`
- `story_performance_daily` ↔ `trend_daily`: via `assigned_trends` (array) — gunakan extract atau explode di custom SQL
- `pipeline_quality_daily`: table terpisah untuk Dashboard 3

### 7.4 Extract (direkomendasikan)
Gunakan Tableau Extract, bukan Live, karena:
- Gold tables di-refresh per run (hourly) — extract cukup di-refresh bersamaan.
- Menghindari biaya BigQuery query berulang dari Tableau interaksi.
- Schedule refresh extract: setiap 1 jam (atau manual setelah run pipeline).

---

## 8. Tableau Dashboards (Task 28)

### Dashboard 1 — Technology Trend Overview
| Visual | Source | Fields |
|---|---|---|
| Bar: Top trends by momentum | `trend_momentum_daily` | trend_key, trend_momentum (filter latest date) |
| Line: Trend volume over time | `trend_daily` | trend_date, story_count per trend_key |
| Area: Story & comment growth | `trend_daily` | trend_date, total_hn_score, total_comments |
| Donut: Sentiment distribution | `trend_daily` | sentiment label (dari avg_sentiment bucket) |
| KPI cards | gabungan | active trends (count distinct trend_key), top trend, total stories, total comments |
| KPI: freshness | `pipeline_quality_daily` | freshness_timestamp |

### Dashboard 2 — Trend & Sentiment Drilldown
| Visual | Source | Fields |
|---|---|---|
| Filter: trend selector | `trend_daily` | trend_key (dropdown) |
| Line: Sentiment time series | `trend_daily` | trend_date, avg_sentiment untuk trend terpilih |
| Bar: Supporting stories ranked | `story_performance_daily` | story_id, latest_score, score_delta (sorted) |
| Scatter: Sentiment vs engagement | `trend_daily` + `story_performance_daily` | avg_sentiment vs total_comments |
| Filter: model/prompt version | `dim_trend` | model_version, prompt_version |
| Warning: low evidence | `trend_daily` | color red jika total_evidence_count < threshold (misal 3) |

### Dashboard 3 — Pipeline & AI Quality
| Visual | Source | Fields |
|---|---|---|
| Table: Run status | `pipeline_quality_daily` | quality_date, total_runs, successful_runs, failed_runs |
| Bar: Row counts per layer | `pipeline_quality_daily` | total_source_rows, total_bronze_rows, total_silver_rows, total_gold_rows (reconciliation) |
| Line: LLM success rate | `pipeline_quality_daily` | quality_date, llm_success_rate |
| Bar: Quarantine | `pipeline_quality_daily` | quarantine_count, invalid_ai_count |
| KPI: freshness | `pipeline_quality_daily` | freshness_timestamp (age in hours, warna merah jika > 2 jam) |

### Low-evidence warning (Task 29)
Di Dashboard 2, tambahkan calculated field:
```
IF [Total Evidence Count] < 3 THEN "LOW EVIDENCE" ELSE "OK" END
```
Tampilkan sebagai color legend atau badge di trend cards.

### Freshness indicator (Task 29)
Calculated field di Dashboard 1 & 3:
```
DATEDIFF('hour', [Freshness Timestamp], NOW())
```
Warna: hijau ≤ 2 jam, kuning ≤ 6 jam, merah > 6 jam.

---

## 9. Demo Script (Final Trace — Phase 3 gate)

Untuk portfolio/demo, trace satu trend end-to-end:

1. Telegram `/run` → Gary membuat `collection_run` (run_id di PostgreSQL).
2. `SELECT * FROM ops.collection_run ORDER BY started_at DESC LIMIT 1;` → catat `run_id`.
3. `SELECT * FROM raw.hn_story_snapshot WHERE run_id = '<run_id>' LIMIT 3;` → story IDs.
4. `SELECT * FROM ai.trend_analysis WHERE run_id = '<run_id>';` → trend_key + analysis_id.
5. `SELECT * FROM techtrend_bronze.hn_story_snapshot_raw WHERE run_id = '<run_id>';` → Bronze.
6. `SELECT * FROM techtrend_silver.fact_trend_sentiment WHERE run_id = '<run_id>';` → Silver.
7. `SELECT * FROM techtrend_gold.trend_daily WHERE trend_key = '<trend_key>' ORDER BY trend_date;` → Gold.
8. Buka Tableau Dashboard 1 → cari trend_key yang sama → tunjukkan bar momentum.
9. Drill down Dashboard 2 → supporting stories → cocokkan dengan story IDs di langkah 3.

Rantai lengkap:
`Telegram update_id → run_id → story_id → analysis_id → pipeline_run_id → trend_key → Tableau mark`

---

## 10. Troubleshooting Cepat

| Gejala | Fix |
|---|---|
| `PermissionDenied: bigquery.readsessions.create` | Grant role (section 5.4) atau `set BQ_READ_METHOD=export` |
| `PicklingError / RecursionError` | Pastikan pola `main()` (section 5.3) |
| Checkpoint maju tapi data kosong | Cek `ops.pipeline_run` — apakah status SUCCEEDED dengan count 0 (window kosong) vs data hilang |
| Silver rows < Bronze rows | Normal — deleted/dead/invalid rows dikarantina ke `data_quality_issue` |
| Gold momentum = 0.5 semua | Hanya 1 trend di tanggal tersebut → min-max normalization neutral; normal jika dataset kecil |
| Tableau tidak melihat dataset | Pastikan akun OAuth punya akses project `techtrend-dev`; cek region dataset konsisten |

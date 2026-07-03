# 🌤️ Air Quality Data Pipeline

A production-style end-to-end data engineering pipeline that ingests live air quality data from Indian cities, validates it, transforms it through a medallion architecture, and surfaces insights via a live Streamlit dashboard — with automated Slack alerts when pollution levels are unhealthy.

**[🔗 Live Dashboard](#)** ← *(add Streamlit Community Cloud link here after deployment)*

---

## What It Does

Every day (twice daily at 06:00 and 18:00 UTC), the pipeline automatically:

1. **Fetches** live air quality readings (PM2.5, PM10, NO2, SO2, CO, O3) for 8 Indian cities from the OpenAQ public API
2. **Stores** raw JSON in MinIO (S3-compatible object storage), partitioned by date and city
3. **Loads** structured data into PostgreSQL
4. **Validates** data quality using Great Expectations — halts the pipeline if data looks wrong
5. **Transforms** raw data through a dbt medallion architecture (staging → intermediate → gold)
6. **Alerts** via Slack if any city's pollution crosses unhealthy thresholds
7. **Displays** everything on a live Streamlit dashboard with city-level AQI status, pollutant breakdowns, and trend charts

---

## Architecture

```
OpenAQ API (live, public)
      │
      ▼
┌─────────────────────────────────────────────────────┐
│                  Apache Airflow                      │
│                                                      │
│  Task 1: extract_and_load_air_quality_data          │
│     └─ Fetch /latest per city → save JSON to MinIO  │
│                                                      │
│  Task 2: load_minio_to_postgres                     │
│     └─ Flatten JSON → upsert into raw PG table      │
│                                                      │
│  Task 3: validate_data_quality                      │
│     └─ Great Expectations checks → halt if fail     │
│                                                      │
│  Task 4: run_dbt_models                             │
│     └─ staging → intermediate → gold mart           │
│                                                      │
│  Task 5: send_slack_alerts                          │
│     └─ Check is_unhealthy flag → post to Slack      │
└─────────────────────────────────────────────────────┘
      │                          │
      ▼                          ▼
  MinIO                     PostgreSQL
  (raw JSON)                (transformed tables)
                                 │
                                 ▼
                          Streamlit Dashboard
                          (live, public URL)
```

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Orchestration | Apache Airflow 2.9 | Scheduling, retries, task dependencies |
| Raw Storage | MinIO | S3-compatible object store for raw JSON |
| Warehouse | PostgreSQL 15 | Structured storage for raw + transformed data |
| Transformation | dbt-core 1.8 | Medallion architecture (staging/intermediate/marts) |
| Data Quality | Great Expectations 0.18 | Automated validation gate before transformation |
| Dashboard | Streamlit | Live public dashboard |
| Alerting | Slack Webhooks | Push alerts when AQI exceeds safe thresholds |
| Containerization | Docker + Docker Compose | Single-command local deployment |
| Data Source | OpenAQ v3 API | Free public air quality sensor data |

---

## dbt Medallion Architecture

```
raw_air_quality_measurements          (Postgres, loaded by Airflow)
        │
        ▼
stg_air_quality_measurements          (staging: clean types, drop nulls)
        │
        ▼
int_air_quality_daily_avg             (intermediate: daily avg per city/parameter)
        │
        ▼
mart_air_quality_summary              (gold: dashboard-ready, with is_unhealthy flag)
```

---

## Cities Tracked

Bangalore · Mumbai · Delhi · Mangalore · Hyderabad · Chennai · Pune · Kolkata

---

## Data Quality Checks (Great Expectations)

- `city` and `parameter` columns must never be null
- `value` must never be null
- City must be one of the tracked set (no stray data)
- PM2.5 values must be in physically plausible range (0–1000 µg/m³)
- Row count must be > 0 (catches silent API failures)

Pipeline halts and dbt does not run if any check fails.

---

## Project Structure

```
air-quality-pipeline/
├── Dockerfile                    # Custom Airflow image with all deps pre-installed
├── docker-compose.yml            # Full stack: Airflow + Postgres + MinIO
├── .env                          # API keys (not committed)
├── dags/
│   ├── air_quality_ingestion.py  # Main Airflow DAG (5 tasks)
│   └── dbt_profiles/
│       └── profiles.yml          # dbt connection config for Docker network
├── dbt/
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/
│       ├── intermediate/
│       └── marts/
├── dashboard/
│   └── app.py                    # Streamlit dashboard
└── scripts/
    └── test_openaq.py            # Local API connectivity test
```

---

## Running Locally

### Prerequisites
- Docker Desktop
- Python 3.11+
- Git

### Setup

```bash
# 1. Clone the repo
git clone https://github.com/ShraddhaPai2001/air-quality-pipeline.git
cd air-quality-pipeline

# 2. Create .env file
echo "OPENAQ_API_KEY=your_key_here" > .env
echo "SLACK_WEBHOOK_URL=your_webhook_here" >> .env

# 3. Build the custom Docker image (one time only)
docker compose build

# 4. Start the stack
docker compose up -d

# 5. Open Airflow UI
# http://localhost:8080 — login: admin / admin
# Trigger the DAG: air_quality_ingestion

# 6. Open MinIO console
# http://localhost:9001 — login: minioadmin / minioadmin

# 7. Run the dashboard
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

### Get an OpenAQ API key
Sign up free at https://explore.openaq.org/register

---

## Key Engineering Decisions

**Why `ingestion_date` instead of `measured_at` for trend grouping?**
OpenAQ sensors report data with highly variable freshness — some sensors lag by months. Grouping by `ingestion_date` (when the pipeline ran) rather than `measured_at` (sensor timestamp) gives a reliable daily trend that accurately reflects when data was collected.

**Why upsert instead of insert?**
The pipeline runs twice daily. Using `ON CONFLICT DO UPDATE` means re-runs update existing readings rather than creating duplicates, keeping the raw table clean.

**Why custom Docker image instead of `_PIP_ADDITIONAL_REQUIREMENTS`?**
Airflow's `_PIP_ADDITIONAL_REQUIREMENTS` reinstalls packages on every container restart, causing 5-10 minute startup delays. A custom Dockerfile bakes packages in once — restarts take seconds.

---

## Resume Bullet

> *Built and deployed a production-style air quality data pipeline ingesting live data for 8 Indian cities via Airflow (scheduled twice daily), transforming through dbt medallion architecture, validating with Great Expectations, and surfacing insights via a live Streamlit dashboard with automated Slack alerts for unhealthy AQI levels.*

---

*Data from OpenAQ public API. For educational/portfolio purposes.*
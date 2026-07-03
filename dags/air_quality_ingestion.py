"""
Air Quality Ingestion DAG
Pulls latest air quality measurements from OpenAQ for selected Indian cities
and writes raw JSON to MinIO (S3-compatible storage), partitioned by date and city.
"""

import os
import json
import io
import time
from datetime import datetime, timedelta

import requests
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

try:
    from minio import Minio
except ImportError:
    Minio = None  # will fail loudly inside the task if minio package isn't installed


# ---- Config ----
OPENAQ_API_KEY = os.environ.get("OPENAQ_API_KEY")
OPENAQ_BASE_URL = "https://api.openaq.org/v3/locations"
OPENAQ_LATEST_URL = "https://api.openaq.org/v3/locations/{location_id}/latest"
OPENAQ_DAYS_URL = "https://api.openaq.org/v3/sensors/{sensor_id}/days"

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "air-quality-raw"

CITIES = {
    "bangalore": (12.9716, 77.5946),
    "mumbai": (19.0760, 72.8777),
    "delhi": (28.7041, 77.1025),
    "mangalore": (12.9141, 74.8560),
    "hyderabad": (17.3850, 78.4867),
    "chennai": (13.0827, 80.2707),
    "pune": (18.5204, 73.8567),
    "kolkata": (22.5726, 88.3639),
}
RADIUS_METERS = 25000
REQUEST_DELAY_SECONDS = 1.5  # pause between OpenAQ calls to avoid hitting rate limits

# Postgres connection (the same postgres container Airflow itself uses, different database)
PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "airflow")  # reusing existing db for simplicity
PG_USER = os.environ.get("PG_USER", "airflow")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "airflow")
RAW_TABLE = "raw_air_quality_measurements"
HISTORY_TABLE = "raw_air_quality_history"
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")


def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,  # local MinIO over http, not https
    )


def ensure_bucket(client):
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)


def openaq_get(url, params=None, max_retries=5, base_delay=2):
    """
    GET wrapper for OpenAQ with rate-limit handling.
    On 429, waits (using Retry-After header if present, else exponential backoff) and retries.
    """
    headers = {"X-API-Key": OPENAQ_API_KEY}
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait_seconds = int(retry_after) if retry_after and retry_after.isdigit() else base_delay * (2 ** (attempt - 1))
            print(f"Rate limited (429) on {url}. Waiting {wait_seconds}s before retry {attempt}/{max_retries}...")
            time.sleep(wait_seconds)
            continue
        resp.raise_for_status()
        return resp
    # If we exhausted retries, raise the last error
    resp.raise_for_status()
    return resp


def fetch_locations(lat: float, lon: float):
    params = {"coordinates": f"{lat},{lon}", "radius": RADIUS_METERS, "limit": 10}
    resp = openaq_get(OPENAQ_BASE_URL, params=params)
    time.sleep(REQUEST_DELAY_SECONDS)
    return resp.json().get("results", [])


def fetch_latest_for_location(location_id: int):
    """Fetch the latest reading per sensor for a given location, using OpenAQ's dedicated /latest endpoint."""
    params = {"limit": 100}
    url = OPENAQ_LATEST_URL.format(location_id=location_id)
    resp = openaq_get(url, params=params)
    time.sleep(REQUEST_DELAY_SECONDS)
    return resp.json().get("results", [])


def extract_and_load(**context):
    """Main task: pull data for every city, fetch measurements per sensor, upload raw JSON to MinIO."""
    ds = datetime.utcnow().strftime("%Y-%m-%d")  # actual current date, not Airflow's data-interval ds
    client = get_minio_client()
    ensure_bucket(client)

    summary = {}

    for city, (lat, lon) in CITIES.items():
        locations = fetch_locations(lat, lon)
        city_payload = {
            "city": city,
            "fetched_at": datetime.utcnow().isoformat(),
            "locations": [],
        }

        for loc in locations:
            sensors = loc.get("sensors", [])
            loc_record = {
                "location_id": loc.get("id"),
                "location_name": loc.get("name"),
                "coordinates": loc.get("coordinates"),
                "measurements": [],
            }
            try:
                latest_readings = fetch_latest_for_location(loc.get("id"))
                # tag each reading with its parameter name/unit using the sensor metadata from `loc`
                sensor_lookup = {s.get("id"): s.get("parameter", {}) for s in loc.get("sensors", [])}
                for reading in latest_readings:
                    sensor_id = reading.get("sensorsId")
                    param_info = sensor_lookup.get(sensor_id, {})
                    loc_record["measurements"].append({
                        "parameter": param_info.get("name"),
                        "unit": param_info.get("units"),
                        "value": reading.get("value"),
                        "datetime_utc": (reading.get("datetime") or {}).get("utc"),
                    })
            except requests.exceptions.HTTPError:
                pass  # skip locations that fail, don't kill the whole task

            city_payload["locations"].append(loc_record)

        summary[city] = {
            "num_locations": len(locations),
            "num_measurements": sum(len(l["measurements"]) for l in city_payload["locations"]),
        }

        # Upload one JSON file per city per day
        object_name = f"raw/{ds}/{city}.json"
        data_bytes = json.dumps(city_payload, default=str).encode("utf-8")
        client.put_object(
            MINIO_BUCKET,
            object_name,
            data=io.BytesIO(data_bytes),
            length=len(data_bytes),
            content_type="application/json",
        )

    print("Ingestion summary:", json.dumps(summary, indent=2))


def fetch_history_for_sensor(sensor_id: int, date_from: str, date_to: str):
    """Fetch daily-average history for a sensor over a date range using OpenAQ's /days endpoint."""
    params = {"limit": 100, "date_from": date_from, "date_to": date_to}
    url = OPENAQ_DAYS_URL.format(sensor_id=sensor_id)
    resp = openaq_get(url, params=params)
    time.sleep(REQUEST_DELAY_SECONDS)
    return resp.json().get("results", [])


def ensure_history_table(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
                id SERIAL PRIMARY KEY,
                city TEXT,
                location_id BIGINT,
                location_name TEXT,
                sensor_id BIGINT,
                parameter TEXT,
                unit TEXT,
                avg_value DOUBLE PRECISION,
                measurement_date DATE,
                loaded_at TIMESTAMP DEFAULT now(),
                UNIQUE (sensor_id, measurement_date)
            );
        """)
    conn.commit()


def fetch_and_load_history(**context):
    """Pull last 7 days of daily-average data per sensor and upsert into Postgres history table."""
    today = datetime.utcnow().date()
    date_from = (today - timedelta(days=7)).isoformat()
    date_to = today.isoformat()

    conn = get_pg_connection()
    ensure_history_table(conn)

    rows_upserted = 0

    for city, (lat, lon) in CITIES.items():
        locations = fetch_locations(lat, lon)
        for loc in locations:
            location_id = loc.get("id")
            location_name = loc.get("name")
            for sensor in loc.get("sensors", []):
                sensor_id = sensor.get("id")
                param_info = sensor.get("parameter", {}) or {}
                if sensor_id is None:
                    continue
                try:
                    days = fetch_history_for_sensor(sensor_id, date_from, date_to)
                except requests.exceptions.HTTPError:
                    continue

                with conn.cursor() as cur:
                    for d in days:
                        period = d.get("period", {}) or {}
                        date_from_field = (period.get("datetimeFrom") or {}).get("local")
                        measurement_date = date_from_field[:10] if date_from_field else None
                        if measurement_date is None:
                            continue
                        cur.execute(
                            f"""
                            INSERT INTO {HISTORY_TABLE}
                            (city, location_id, location_name, sensor_id, parameter, unit, avg_value, measurement_date)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (sensor_id, measurement_date)
                            DO UPDATE SET avg_value = EXCLUDED.avg_value, loaded_at = now()
                            """,
                            (
                                city,
                                location_id,
                                location_name,
                                sensor_id,
                                param_info.get("name"),
                                param_info.get("units"),
                                d.get("value"),
                                measurement_date,
                            ),
                        )
                        rows_upserted += 1
                conn.commit()

    conn.close()
    print(f"Upserted {rows_upserted} rows into {HISTORY_TABLE}")


def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )


def ensure_raw_table(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
                id SERIAL PRIMARY KEY,
                city TEXT,
                location_id BIGINT,
                location_name TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                parameter TEXT,
                value DOUBLE PRECISION,
                unit TEXT,
                measured_at TIMESTAMP,
                ingestion_date DATE,
                loaded_at TIMESTAMP DEFAULT now(),
                UNIQUE (city, location_id, parameter, measured_at)
            );
        """)
    conn.commit()


def load_minio_to_postgres(**context):
    """Read today's raw JSON files from MinIO, flatten, and upsert rows into Postgres raw table.
    Uses ON CONFLICT DO UPDATE so re-running the DAG twice on the same day
    updates existing rows rather than inserting duplicates.
    """
    ds = datetime.utcnow().strftime("%Y-%m-%d")
    client = get_minio_client()
    conn = get_pg_connection()
    ensure_raw_table(conn)

    rows_upserted = 0

    for city in CITIES.keys():
        object_name = f"raw/{ds}/{city}.json"
        try:
            response = client.get_object(MINIO_BUCKET, object_name)
            payload = json.loads(response.read())
        except Exception as e:
            print(f"Skipping {city}: could not read object ({e})")
            continue

        with conn.cursor() as cur:
            for loc in payload.get("locations", []):
                coords = loc.get("coordinates") or {}
                lat = coords.get("latitude")
                lon = coords.get("longitude")
                for m in loc.get("measurements", []):
                    cur.execute(
                        f"""
                        INSERT INTO {RAW_TABLE}
                        (city, location_id, location_name, latitude, longitude,
                         parameter, value, unit, measured_at, ingestion_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (city, location_id, parameter, measured_at)
                        DO UPDATE SET
                            value = EXCLUDED.value,
                            loaded_at = now()
                        """,
                        (
                            city,
                            loc.get("location_id"),
                            loc.get("location_name"),
                            lat,
                            lon,
                            m.get("parameter"),
                            m.get("value"),
                            m.get("unit"),
                            m.get("datetime_utc"),
                            ds,
                        ),
                    )
                    rows_upserted += 1
        conn.commit()

    conn.close()
    print(f"Upserted {rows_upserted} rows into {RAW_TABLE}")


def run_dbt(**context):
    """
    Run dbt models after data quality passes.
    Automates what was previously a manual step.
    """
    import subprocess
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "/opt/airflow/dags/dbt_profiles"],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt run failed:\n{result.stderr}")
    print("dbt run completed successfully.")


def send_slack_alerts(**context):
    """
    Check mart table for unhealthy readings from today's run.
    Sends a Slack message per city that has unhealthy pollutant levels.
    Only fires if SLACK_WEBHOOK_URL is configured.
    """
    import pandas as pd

    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL not set — skipping alerts.")
        return

    ds = datetime.utcnow().strftime("%Y-%m-%d")
    conn = get_pg_connection()

    df = pd.read_sql(
        f"""
        SELECT city, parameter, avg_value, is_unhealthy
        FROM public_marts.mart_air_quality_summary
        WHERE measured_at::date = '{ds}'
          AND is_unhealthy = true
        ORDER BY city, parameter
        """,
        conn,
    )
    conn.close()

    if df.empty:
        print(f"No unhealthy readings found for {ds} — no Slack alert needed.")
        return

    # Group alerts by city
    for city, group in df.groupby("city"):
        lines = [f"*⚠️ Air Quality Alert — {city.title()}* ({ds})"]
        for _, row in group.iterrows():
            param_label = {
                "pm25": "Fine Particles (PM2.5)",
                "pm10": "Coarse Dust (PM10)",
                "no2":  "Nitrogen Dioxide",
                "so2":  "Sulfur Dioxide",
                "co":   "Carbon Monoxide",
                "o3":   "Ground-Level Ozone",
            }.get(row["parameter"], row["parameter"].upper())
            lines.append(f"• {param_label}: *{row['avg_value']:.1f}* — exceeds safe threshold")
        lines.append("_Check the dashboard for details._")

        message = "\n".join(lines)
        payload = {"text": message}

        resp = requests.post(
            SLACK_WEBHOOK_URL,
            json=payload,
            timeout=10,
        )

        if resp.status_code == 200:
            print(f"Slack alert sent for {city}.")
        else:
            print(f"Slack alert failed for {city}: {resp.status_code} {resp.text}")


default_args = {
    "owner": "shraddha",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def validate_data_quality(**context):
    """
    Great Expectations checkpoint — runs after raw data is loaded into Postgres.
    Validates the raw_air_quality_measurements table before dbt transforms it.
    Fails loudly if expectations are violated so bad data never reaches the dashboard.
    """
    import great_expectations as gx
    import pandas as pd

    conn = get_pg_connection()
    ds = datetime.utcnow().strftime("%Y-%m-%d")

    # Load today's raw data into a pandas DataFrame for validation
    df = pd.read_sql(
        f"SELECT * FROM {RAW_TABLE} WHERE ingestion_date = '{ds}'",
        conn
    )
    conn.close()

    if df.empty:
        raise ValueError(f"No data found in {RAW_TABLE} for ingestion_date={ds}. "
                         "Pipeline halted — check the ingestion task.")

    # Build an in-memory GE context (no filesystem setup needed)
    context_ge = gx.get_context(mode="ephemeral")

    datasource = context_ge.sources.add_pandas(name="raw_measurements")
    asset = datasource.add_dataframe_asset(name="today_raw")
    batch_request = asset.build_batch_request(dataframe=df)

    # Define expectations
    suite_name = "raw_air_quality_suite"
    suite = context_ge.add_expectation_suite(expectation_suite_name=suite_name)

    validator = context_ge.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    # 1. City must never be null
    validator.expect_column_values_to_not_be_null("city")

    # 2. Parameter must never be null
    validator.expect_column_values_to_not_be_null("parameter")

    # 3. Value must never be null
    validator.expect_column_values_to_not_be_null("value")

    # 4. City must be one of our tracked cities
    validator.expect_column_values_to_be_in_set(
        "city", list(CITIES.keys())
    )

    # 5. PM2.5 values when present must be in a physically plausible range
    pm25_df = df[df["parameter"] == "pm25"]
    if not pm25_df.empty:
        validator.expect_column_values_to_be_between(
            "value",
            min_value=0,
            max_value=1000,
            mostly=0.95,  # allow 5% outliers/sensor glitches
        )

    # 6. ingestion_date must not be null
    validator.expect_column_values_to_not_be_null("ingestion_date")

    # 7. Row count sanity check — we expect at least some data each run
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=100000)

    # Save and run the suite
    validator.save_expectation_suite(discard_failed_expectations=False)

    checkpoint = context_ge.add_or_update_checkpoint(
        name="raw_quality_checkpoint",
        validator=validator,
    )

    results = checkpoint.run()

    # Print summary
    print(f"Data quality validation results: success={results.success}")
    for result in results.run_results.values():
        stats = result.get("validation_result", {}).get("statistics", {})
        print(f"  Evaluated: {stats.get('evaluated_expectations', 0)} expectations, "
              f"Failed: {stats.get('unsuccessful_expectations', 0)}")

    # Fail the task if any expectation failed — halts the pipeline
    if not results.success:
        raise ValueError(
            "Data quality validation FAILED. "
            "Check Great Expectations results above. "
            "dbt run will not proceed until data quality is fixed."
        )

    print("All data quality checks passed — safe to proceed to dbt.")


with DAG(
    dag_id="air_quality_ingestion",
    description="Daily ingestion of air quality data from OpenAQ into MinIO raw zone",
    default_args=default_args,
    start_date=datetime(2026, 6, 1),
    schedule_interval="0 6,18 * * *",
    catchup=False,
    tags=["air-quality", "ingestion"],
) as dag:

    extract_and_load_task = PythonOperator(
        task_id="extract_and_load_air_quality_data",
        python_callable=extract_and_load,
    )

    load_to_postgres_task = PythonOperator(
        task_id="load_minio_to_postgres",
        python_callable=load_minio_to_postgres,
    )

    validate_quality_task = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_data_quality,
    )

    run_dbt_task = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            "dbt run "
            "--project-dir /opt/airflow/dags/dbt "
            "--profiles-dir /opt/airflow/dags/dbt_profiles"
        ),
    )

    slack_alert_task = PythonOperator(
        task_id="send_slack_alerts",
        python_callable=send_slack_alerts,
    )

    # Full automated pipeline — no manual steps needed:
    # extract → load → validate quality → run dbt → send slack alerts
    extract_and_load_task >> load_to_postgres_task >> validate_quality_task >> run_dbt_task >> slack_alert_task
FROM apache/airflow:2.9.3-python3.11

USER airflow

RUN pip install --no-cache-dir --upgrade pip setuptools wheel

RUN pip install --no-cache-dir "minio==7.2.7" "requests==2.32.3"

RUN pip install --no-cache-dir "great-expectations==0.18.19"

RUN pip install --no-cache-dir "Logbook==1.6.0"

RUN pip install --no-cache-dir "psycopg2-binary==2.9.9"

# Install dbt-core first (no psycopg2 dependency)
RUN pip install --no-cache-dir "dbt-core==1.8.0"

# Install dbt-postgres without its deps (avoids pulling psycopg2 source)
# then manually install its other deps
RUN pip install --no-cache-dir --no-deps "dbt-postgres==1.8.0"
RUN pip install --no-cache-dir \
    "dbt-adapters>=1.0.0,<2.0.0" \
    "dbt-common>=1.0.0,<2.0.0"
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# ── Paths (set in .env, loaded into environment before starting Airflow) ──────
BASE_DIR        = os.environ["PIPELINE_BASE_DIR"]        # e.g. /home/ubuntu/final/taxi/msba405-sample-pipeline
SPARK_SCRIPT    = os.path.join(BASE_DIR, "spark/spark-job.py")
DATA_DIR        = os.path.join(BASE_DIR, "data")
ZONE_DATA       = os.path.join(DATA_DIR, "taxi_zone_lookup.csv")
WEATHER_DATA    = os.path.join(DATA_DIR, "72505394728.csv")
OUTPUT_DIR      = os.path.join(BASE_DIR, "output")
DUCKDB_BIN      = os.environ.get("DUCKDB_EXECUTABLE", "duckdb")
DUCKDB_DATABASE = os.path.join(BASE_DIR, os.environ.get("DUCKDB_DATABASE", "duckdb/final.db"))
DUCKDB_QUERIES  = os.path.join(BASE_DIR, os.environ.get("DUCKDB_QUERIES",  "duckdb/queries.sql"))

MONTHS = [f"2024-{m:02d}" for m in range(1, 13)]  # 2024-01 through 2024-12

# ── DAG definition ────────────────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'fhvhv_spark_to_duckdb',
    default_args=default_args,
    schedule=None,  # Trigger manually; all 12 months are a one-shot batch
    catchup=False,
)

# ── Build one Spark task per month ────────────────────────────────────────────
spark_tasks = []

for month in MONTHS:
    parquet_file = os.path.join(DATA_DIR, f"fhvhv_tripdata_{month}.parquet")
    month_output = os.path.join(OUTPUT_DIR, month)

    task = BashOperator(
        task_id=f"spark_{month.replace('-', '_')}",
        bash_command=f'''
            mkdir -p {month_output} &&
            spark-submit --master local[*] \
                --driver-memory 2g \
                --executor-memory 2g \
                --conf spark.sql.shuffle.partitions=4 \
                {SPARK_SCRIPT} \
                {parquet_file} \
                {ZONE_DATA} \
                {WEATHER_DATA} \
                {month_output}
        ''',
        cwd=BASE_DIR,
        dag=dag,
    )
    spark_tasks.append(task)

# ── Final task: load all monthly output into DuckDB ───────────────────────────
load_duckdb_task = BashOperator(
    task_id='load_parquet_into_duckdb',
    bash_command=f'''
        LOADPATH="{OUTPUT_DIR}/*/*.parquet" &&
        sed "s|\\$LOADPATH|${{LOADPATH//\//\\/}}|g" "{DUCKDB_QUERIES}" | {DUCKDB_BIN} "{DUCKDB_DATABASE}"
    ''',
    cwd=BASE_DIR,
    dag=dag,
)

# ── Chain: Jan >> Feb >> ... >> Dec >> DuckDB ─────────────────────────────────
for upstream, downstream in zip(spark_tasks, spark_tasks[1:]):
    upstream >> downstream

spark_tasks[-1] >> load_duckdb_task

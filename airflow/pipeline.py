from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# File paths
SPARK_SCRIPT_PATH = "../spark/spark-job.py"
TAXI_PATH = "../data/yellow_tripdata_2024-10.parquet"
ZONE_PATH = "../data/taxi_zone_lookup.csv"
WEATHER_PATH = "../data/72505394728.csv"
OUTPUT_PATH = "../output"
DUCKDB_EXECUTABLE = "duckdb"  # Path to DuckDB executable
DUCKDB_DATABASE = "../duckdb/final.db"
DUCKDB_QUERIES = "../duckdb/queries.sql"

# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 23),
    'retries': 1,
}

# Define DAG
dag = DAG(
    'spark_to_duckdb',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
    # If N runs fail, catchup=True will rerun each of them.
    # If False, it will skip them.
)

# Task 1: Run Spark job using BashOperator
spark_submit_task = BashOperator(
    task_id='run_spark_job',
    # Add pwd to see working directory
    bash_command='''
        pwd &&
        echo "Current directory contents:" &&
        ls -la &&
        echo "Trying to run spark-submit..." &&
        spark-submit --master local[*] \
            --driver-memory 2g \
            --executor-memory 2g \
            --conf spark.sql.shuffle.partitions=4 \
            spark/spark-job.py \
            data/yellow_tripdata_2024-10.parquet \
            data/taxi_zone_lookup.csv \
            data/72505394728.csv \
            output
    ''',
    # This will show stderr in logs
    cwd="/home/ubuntu/final/taxi/msba405-sample-pipeline",  # Set working directory
    dag=dag,
)

# Task 2: Load Parquet into DuckDB using BashOperator
load_duckdb_task = BashOperator(
    task_id='load_parquet_into_duckdb',
    bash_command=f'''
        LOADPATH="{OUTPUT_PATH}/*.parquet" &&
        sed "s|\\$LOADPATH|${{LOADPATH//\//\\/}}|g" "{DUCKDB_QUERIES}" | {DUCKDB_EXECUTABLE} "{DUCKDB_DATABASE}"
    ''',
    dag=dag
)

# Set task dependencies
spark_submit_task >> load_duckdb_task

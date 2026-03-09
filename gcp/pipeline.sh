#!/usr/bin/env bash
set -euo pipefail

REGION=us-central1
BUCKET=msba405demo
JOB_NAME="nyctaxi-$(date +%s)"

RAW_TAXI="raw/fhvhv"
ZONES="raw/reference/taxi_zone_lookup.csv"
WEATHER="raw/weather/72505394728.csv"
RUN_OUTPUT="curated/rides/${JOB_NAME}"
SPARK="spark/spark-gcp.py"

OUTPUT_URI="gs://${BUCKET}/${RUN_OUTPUT}/"
SPARK_URI="gs://${BUCKET}/${SPARK}"
RAW_TAXI_URI="gs://${BUCKET}/${RAW_TAXI}/"
ZONES_URI="gs://${BUCKET}/${ZONES}"
WEATHER_URI="gs://${BUCKET}/${WEATHER}"

export $(grep -v '^#' pipeline.env | xargs)

cleanup_output() {
    echo "!!!! Cleaning failed run output: ${OUTPUT_URI}"
    gcloud storage rm -r "${OUTPUT_URI}**" || true
    echo "!!!! Cleanup complete."
}

echo "Submitting Dataproc job: ${JOB_NAME}"
echo "Output path: ${OUTPUT_URI}"


#if gcloud dataproc batches submit pyspark "${SPARK_URI}" \
#    --region="${REGION}" \
#    --batch="${JOB_NAME}" \
#    --properties="spark.dynamicAllocation.enabled=true,spark.dynamicAllocation.minExecutors=2,spark.dynamicAllocation.maxExecutors=8,spark.executor.memory=4g,spark.executor.cores=4,spark.driver.memory=4g" \
#    -- \
#    "${RAW_TAXI_URI}" \
#    "${ZONES_URI}" \
#    "${WEATHER_URI}" \
#    "${OUTPUT_URI}"
#then
#    echo "#### Spark job finished."
#else
#    STATUS=$?
#    echo "!!!! Spark job failed with exit code ${STATUS}."
#    cleanup_output
#    exit "${STATUS}"
#fi

echo "----------------------------"

echo "Loading curated GCS data into Snowflake and rebuilding Tableau views..."

python3 assets/snowflake-load.py

echo "######## Process complete."

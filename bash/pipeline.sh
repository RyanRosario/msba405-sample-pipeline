#!/bin/bash

# Change if these commands are not on the path.
DUCKDB=duckdb
SPARK=spark-submit

# Allows you to change the name of the script.
TRANSFORMER=../spark/spark-job.py
DATABASE=../duckdb/final.db
OUTPUT=../output
QUERIES=../duckdb/queries.sql

# Data
TAXI=../data/fhvhv_tripdata_2024-*.parquet
ZONE=../data/taxi_zone_lookup.csv
WEATHER=../data/72505394728.csv

# Variables for DuckDB load.
LOADPATH="$OUTPUT/*.parquet"

# A pipeline should be fire and forget.
# It needs to be *atomic*. Either everything executes, or nothing does.
# If part of it fails, we need to *rollback* to the state we were in
# before the pipeline began execution. Otherwise, we end up with partial
# results and an inconsistent state.
rollback() {
	rm -fr $OUTPUT
	rm -f $DATABASE
}

message() {
	printf "%50s\n" | tr " " "-"
	printf "$1\n"
	printf "%50s\n" | tr " " "-"
}

check() {
	if [ $? -eq 0 ]; then
		message "$1"
	else 
		message "$2"
		rollback
		exit 1
	fi
}

run_spark() {
	rm -fr $OUTPUT
	$SPARK \
		--master local[*] \
		--conf "spark.sql.shuffle.partitions=2" \
		--name "NYC Taxi/Weather Analysis" \
		$TRANSFORMER \
    			"$TAXI" \
		       	$ZONE \
		       	$WEATHER \
			$OUTPUT
	check "Spark job successfully completed (E and T)." "Spark job FAILED."
}

run_duckdb() {
	sed "s|\$LOADPATH|${LOADPATH//\//\\/}|g" "$QUERIES" | $DUCKDB "$DATABASE"
	check "Data loaded into DuckDB successfully (L)." "Data load FAILED."
}

message "\n\n\n\nSTARTING SAMPLE PIPELINE...\n\n\n\n"

run_spark
run_duckdb

check "PROCESS COMPLETE" "PIPELINE FAILED"


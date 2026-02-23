#!/bin/sh
#
# Note that this file is not necessary
# Just make sure you delete the indications of success before executing again.

rm -f ../duckdb/spark_complete.flag ../duckdb/pipeline_complete.flag ../duckdb/load_complete.flag
python3 pipeline.py

USE DATABASE NYC_TAXI;
USE SCHEMA PUBLIC;
USE WAREHOUSE FINAL;

/* ============================================================
   Staging Table: Infer schema from curated Parquet in GCS
   ============================================================ */

CREATE OR REPLACE TABLE RIDES_RAW_STAGING
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION => '@rides_curated_stage',
      FILE_FORMAT => 'rides_parquet_ff',
      IGNORE_CASE => TRUE
    )
  )
);

/* ============================================================
   Load curated data from GCS into staging
   ============================================================ */

COPY INTO RIDES_RAW_STAGING
FROM @rides_curated_stage
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]parquet';

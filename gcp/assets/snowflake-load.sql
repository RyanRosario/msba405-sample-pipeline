-- Adjust these names if you want different object names.

CREATE OR REPLACE STORAGE INTEGRATION gcs_msba405demo_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://msba405demo/curated/rides/');

-- Run this once, copy the STORAGE_GCP_SERVICE_ACCOUNT value,
-- and grant that service account access to the bucket in GCP.
DESC INTEGRATION gcs_msba405demo_int;

CREATE OR REPLACE FILE FORMAT rides_parquet_ff
  TYPE = PARQUET
  USE_LOGICAL_TYPE = TRUE;

CREATE OR REPLACE STAGE rides_curated_stage
  URL = 'gcs://msba405demo/curated/rides/'
  STORAGE_INTEGRATION = gcs_msba405demo_int
  FILE_FORMAT = rides_parquet_ff;

CREATE OR REPLACE TABLE RIDES_RAW
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION => '@rides_curated_stage',
      FILE_FORMAT => 'rides_parquet_ff'
    )
  )
);

COPY INTO RIDES_RAW
FROM @rides_curated_stage
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

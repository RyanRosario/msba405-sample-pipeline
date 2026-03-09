USE DATABASE NYC_TAXI;
USE SCHEMA PUBLIC;
USE WAREHOUSE FINAL;

/* ============================================================
   Promote staging table to production
   ============================================================ */

CREATE OR REPLACE TABLE RIDES_RAW AS
SELECT * FROM RIDES_RAW_STAGING;

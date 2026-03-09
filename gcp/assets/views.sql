USE DATABASE NYC_TAXI;
USE SCHEMA PUBLIC;
USE WAREHOUSE FINAL;

/* ============================================================
   Tableau Base View
   ============================================================ */
CREATE OR REPLACE VIEW TABLEAU_RIDES_BASE AS
SELECT
    pickup_datetime,
    pickup_hour,
    DATE_TRUNC('day', pickup_datetime) AS pickup_date,
    DAYNAME(pickup_datetime) AS dow,
    EXTRACT(hour FROM pickup_datetime) AS pickup_hour_of_day,
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone,
    passenger_count,
    trip_distance,
    fare_amount,
    total_amount,
    precipitation,
    humidity,
    temperature,
    condition,
    severity
FROM RIDES_RAW;

/* ============================================================
   Time Series: Number of Rides Over Time (Hourly)
   ============================================================ */
CREATE OR REPLACE VIEW TABLEAU_RIDES_OVER_TIME AS
SELECT
    DATE_TRUNC('hour', pickup_datetime) AS ts_hour,
    COUNT(*) AS total_rides,
    AVG(passenger_count) AS avg_passengers,
    AVG(trip_distance) AS avg_distance,
    AVG(fare_amount) AS avg_fare,
    AVG(total_amount) AS avg_total_amount,
    SUM(total_amount) AS total_revenue
FROM RIDES_RAW
GROUP BY DATE_TRUNC('hour', pickup_datetime)
ORDER BY ts_hour;

/* ============================================================
   Time Series: Number of Rides Per Day
   ============================================================ */
CREATE OR REPLACE VIEW TABLEAU_RIDES_OVER_DAY AS
SELECT
    DATE_TRUNC('day', pickup_datetime) AS ts_day,
    COUNT(*) AS total_rides,
    AVG(passenger_count) AS avg_passengers,
    AVG(trip_distance) AS avg_distance,
    AVG(fare_amount) AS avg_fare,
    AVG(total_amount) AS avg_total_amount,
    SUM(total_amount) AS total_revenue
FROM RIDES_RAW
GROUP BY DATE_TRUNC('day', pickup_datetime)
ORDER BY ts_day;

/* ============================================================
   Bar Chart: Average Rides Per Day of Week
   ============================================================ */
CREATE OR REPLACE VIEW TABLEAU_AVG_RIDES_PER_DAY_OF_WEEK AS
WITH daily_counts AS (
    SELECT
        DATE_TRUNC('day', pickup_datetime) AS pickup_date,
        DAYNAME(pickup_datetime) AS dow,
        COUNT(*) AS daily_rides
    FROM RIDES_RAW
    GROUP BY
        DATE_TRUNC('day', pickup_datetime),
        DAYNAME(pickup_datetime)
)
SELECT
    dow,
    AVG(daily_rides) AS avg_rides_per_day,
    MIN(daily_rides) AS min_rides_per_day,
    MAX(daily_rides) AS max_rides_per_day
FROM daily_counts
GROUP BY dow;

/* ============================================================
   Bar Chart: Rides by Weather Condition
   ============================================================ */
CREATE OR REPLACE VIEW TABLEAU_RIDES_BY_WEATHER AS
SELECT
    COALESCE(condition, 'Unknown') AS condition,
    COALESCE(severity, 'Unknown') AS severity,
    COUNT(*) AS total_rides,
    AVG(trip_distance) AS avg_distance,
    AVG(fare_amount) AS avg_fare,
    AVG(total_amount) AS avg_total_amount
FROM RIDES_RAW
GROUP BY
    COALESCE(condition, 'Unknown'),
    COALESCE(severity, 'Unknown')
ORDER BY total_rides DESC;

/* ============================================================
   Scatter Plot: Precipitation vs Taxi Rides
   ============================================================ */
CREATE OR REPLACE VIEW TABLEAU_PRECIPITATION_VS_RIDES AS
SELECT
    pickup_hour,
    condition,
    severity,
    precipitation,
    COUNT(*) AS total_rides,
    AVG(trip_distance) AS avg_distance,
    AVG(total_amount) AS avg_total_amount
FROM RIDES_RAW
WHERE precipitation IS NOT NULL
GROUP BY
    pickup_hour,
    condition,
    severity,
    precipitation
ORDER BY pickup_hour;

/* ============================================================
   Scatter Plot: Temperature vs Taxi Rides
   ============================================================ */
CREATE OR REPLACE VIEW TABLEAU_TEMPERATURE_VS_RIDES AS
SELECT
    pickup_hour,
    condition,
    severity,
    temperature,
    COUNT(*) AS total_rides,
    AVG(trip_distance) AS avg_distance,
    AVG(total_amount) AS avg_total_amount
FROM RIDES_RAW
WHERE temperature IS NOT NULL
GROUP BY
    pickup_hour,
    condition,
    severity,
    temperature
ORDER BY pickup_hour;

/* ============================================================
   Scatter Plot: Humidity vs Taxi Rides
   ============================================================ */
CREATE OR REPLACE VIEW TABLEAU_HUMIDITY_VS_RIDES AS
SELECT
    pickup_hour,
    condition,
    severity,
    humidity,
    COUNT(*) AS total_rides,
    AVG(trip_distance) AS avg_distance,
    AVG(total_amount) AS avg_total_amount
FROM RIDES_RAW
WHERE humidity IS NOT NULL
GROUP BY
    pickup_hour,
    condition,
    severity,
    humidity
ORDER BY pickup_hour;

/* ============================================================
   Map / Heatmap: Pickup -> Dropoff
   ============================================================ */
CREATE OR REPLACE VIEW TABLEAU_PICKUP_DROPOFF_HEATMAP AS
SELECT
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone,
    COALESCE(condition, 'Unknown') AS condition,
    COALESCE(severity, 'Unknown') AS severity,
    DATE_TRUNC('day', pickup_datetime) AS pickup_date,
    COUNT(*) AS total_rides,
    AVG(trip_distance) AS avg_distance,
    AVG(total_amount) AS avg_total_amount
FROM RIDES_RAW
GROUP BY
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone,
    COALESCE(condition, 'Unknown'),
    COALESCE(severity, 'Unknown'),
    DATE_TRUNC('day', pickup_datetime);

/* ============================================================
   Dashboard Filters
   ============================================================ */
CREATE OR REPLACE VIEW TABLEAU_DASHBOARD_FILTERS AS
SELECT DISTINCT
    DATE_TRUNC('day', pickup_datetime) AS pickup_date,
    DAYNAME(pickup_datetime) AS dow,
    EXTRACT(hour FROM pickup_datetime) AS hour_of_day,
    COALESCE(condition, 'Unknown') AS condition,
    COALESCE(severity, 'Unknown') AS severity,
    pickup_borough,
    dropoff_borough
FROM RIDES_RAW;

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, date_trunc, row_number, split,
    regexp_extract, regexp_replace, when, 
    monotonically_increasing_id, broadcast
)
from pyspark.sql import functions as F
from pyspark.sql import Window

def main():
    print(sys.argv)
    if len(sys.argv) != 5:
        print("Usage: spark-submit spark-job.py [taxi_path] [zone_path] [weather_path] [output_path]")
        sys.exit(1)

    taxi_path = sys.argv[1]
    zone_path = sys.argv[2]
    weather_path = sys.argv[3]
    output_path = sys.argv[4]
    spark = SparkSession.builder.getOrCreate()
    
    # Load and preprocess HVFHV (Uber/Lyft) rides data
    rides = (
        spark.read
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .option("spark.sql.files.maxPartitionBytes", "128MB")
        .parquet(taxi_path)
        .select(
            # UPDATED FOR HVFHV SCHEMA:
            col('hvfhs_license_num').alias('VendorID'), # Maps Uber/Lyft license to VendorID
            col('pickup_datetime'),                     # Standardized name in HVFHV
            col('dropoff_datetime'),                    # Standardized name in HVFHV
            # HVFHV does not always have passenger_count; filling with 1 if missing
            F.lit(1).alias('passenger_count'), 
            col('trip_miles').alias('trip_distance'),   # HVFHV uses miles, not distance
            col('PULocationID').alias('pickup_location'),
            col('DOLocationID').alias('dropoff_location'),
            col('base_passenger_fare').alias('fare_amount'), # Primary fare field
            col('driver_pay').alias('total_amount')     # Using driver_pay for 'total' context
        )
        .filter((F.year(col('pickup_datetime')) == 2024) & (F.year(col('dropoff_datetime')) == 2024))
        .withColumn("seq", monotonically_increasing_id())
        .withColumn('pickup_hour', date_trunc('hour', 'pickup_datetime'))
    )

    # Zone Loading (Stays the same)
    zones = spark.read.csv(zone_path, header=True)

    rides = (
        rides
        .join(broadcast(zones).alias('pickup'), rides.pickup_location == col('pickup.LocationID'))
        .join(broadcast(zones).alias('dropoff'), rides.dropoff_location == col('dropoff.LocationID'))
        .select(
            'seq', 'VendorID', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 
            'trip_distance', 'pickup_location', 'dropoff_location', 'fare_amount', 
            'total_amount', 'pickup_hour',
            col('pickup.Borough').alias('pickup_borough'),
            col('pickup.Zone').alias('pickup_zone'),
            col('dropoff.Borough').alias('dropoff_borough'),
            col('dropoff.Zone').alias('dropoff_zone')
        )
    )

    # Weather Processing (Stays the same)
    weather = (
       spark.read.csv(weather_path, header=True)
       .withColumn('HOUR', date_trunc('hour', to_timestamp('DATE')))
       .filter(
            (F.year('HOUR') == 2024) & 
            (F.month('HOUR') == 1) # Updated filter to January to match your data
        )
       .select(
           'HOUR', 'HourlyPrecipitation', 'HourlyRelativeHumidity',
           'HourlyDryBulbTemperature', 'HourlyPresentWeatherType'
       )
       .withColumn('weather_code_first', split('HourlyPresentWeatherType', r'\|')[0])
       .withColumn(
           'severity',
           when(col('weather_code_first').isNull(), None)
           .when(col('weather_code_first').contains('-'), 'light')
           .when(col('weather_code_first').contains('+'), 'heavy')
           .otherwise('moderate')
       )
       .withColumn(
           'condition',
           regexp_extract('weather_code_first', r'[+-]?(\w+)(?=:|$)', 1)
       )
       .withColumn(
           'precipitation',
           when(col('HourlyPrecipitation') == 'T', '0')
           .otherwise(regexp_replace('HourlyPrecipitation', 's$', ''))
           .cast('float')
       )
       .withColumn(
           'row_num', 
           row_number().over(Window.partitionBy('HOUR').orderBy('HOUR'))
       )
       .filter('row_num = 1')
       .select(
           'HOUR', 'precipitation',
           col('HourlyRelativeHumidity').cast('float').alias('humidity'),
           col('HourlyDryBulbTemperature').cast('float').alias('temperature'),
           'severity', 'condition'
       )
    )

    # Join rides with weather
    final = (
        rides.join(weather, rides.pickup_hour == weather.HOUR)
        .select(
            "seq", "VendorID", "pickup_datetime", "dropoff_datetime", "passenger_count", 
            "trip_distance", "fare_amount", "total_amount", "pickup_borough",
            "pickup_zone", "dropoff_borough", "dropoff_zone", "pickup_hour", 
            "precipitation", "humidity", "temperature", 
            "severity", "condition"
        )
    )

    final.write \
       .option("maxRecordsPerFile", 100000) \
       .mode("overwrite") \
       .parquet(output_path)

if __name__ == "__main__":
    main()

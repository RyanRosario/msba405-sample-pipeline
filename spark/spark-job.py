import sys
import glob  # Added to resolve local filesystem globs
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, date_trunc, row_number, split,
    regexp_extract, regexp_replace, when, 
    monotonically_increasing_id, broadcast
)
from pyspark.sql import functions as F
from pyspark.sql import Window

def main():
    if len(sys.argv) != 5:
        print("Usage: spark-submit spark-job.py [taxi_path] [zone_path] [weather_path] [output_path]")
        sys.exit(1)

    taxi_path = sys.argv[1]
    zone_path = sys.argv[2]
    weather_path = sys.argv[3]
    output_path = sys.argv[4]
    
    spark = SparkSession.builder.getOrCreate()
    
    # Resolve the glob pattern locally before passing to Spark
    all_taxi_files = glob.glob(taxi_path)
    if not all_taxi_files:
        print(f"Error: No files found matching pattern: {taxi_path}")
        sys.exit(1)

    # Load and preprocess HVFHV (Uber/Lyft) data
    rides = (
        spark.read
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .option("spark.sql.files.maxPartitionBytes", "128MB")
        .parquet(*all_taxi_files)  # Pass the list of expanded file paths
        .select(
            col('h_license_num').alias('VendorID') if 'h_license_num' in spark.read.parquet(all_taxi_files[0]).columns else col('hvfhs_license_num').alias('VendorID'),
            col('pickup_datetime'),
            col('dropoff_datetime'),
            F.lit(1).alias('passenger_count'), 
            col('trip_miles').alias('trip_distance'),
            col('PULocationID').alias('pickup_location'),
            col('DOLocationID').alias('dropoff_location'),
            col('base_passenger_fare').alias('fare_amount'),
            col('driver_pay').alias('total_amount')
        )
        .filter((F.year(col('pickup_datetime')) == 2024) & (F.year(col('dropoff_datetime')) == 2024))
        .withColumn("seq", monotonically_increasing_id())
        .withColumn('pickup_hour', date_trunc('hour', 'pickup_datetime'))
    )

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

    weather = (
       spark.read.csv(weather_path, header=True)
       .withColumn('HOUR', date_trunc('hour', to_timestamp('DATE')))
       .filter((F.year('HOUR') == 2024) & (F.month('HOUR') == 1)) # Adjusted for Jan 2024 data
       .select('HOUR', 'HourlyPrecipitation', 'HourlyRelativeHumidity', 'HourlyDryBulbTemperature', 'HourlyPresentWeatherType')
       .withColumn('weather_code_first', split('HourlyPresentWeatherType', r'\|')[0])
       .withColumn('severity', when(col('weather_code_first').isNull(), None).when(col('weather_code_first').contains('-'), 'light').when(col('weather_code_first').contains('+'), 'heavy').otherwise('moderate'))
       .withColumn('condition', regexp_extract('weather_code_first', r'[+-]?(\w+)(?=:|$)', 1))
       .withColumn('precipitation', when(col('HourlyPrecipitation') == 'T', '0').otherwise(regexp_replace('HourlyPrecipitation', 's$', '')).cast('float'))
       .withColumn('row_num', row_number().over(Window.partitionBy('HOUR').orderBy('HOUR')))
       .filter('row_num = 1')
       .select('HOUR', 'precipitation', col('HourlyRelativeHumidity').cast('float').alias('humidity'), col('HourlyDryBulbTemperature').cast('float').alias('temperature'), 'severity', 'condition')
    )

    final = (
        rides.join(weather, rides.pickup_hour == weather.HOUR)
        .select("seq", "VendorID", "pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance", "fare_amount", "total_amount", "pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone", "pickup_hour", "precipitation", "humidity", "temperature", "severity", "condition")
    )

    final.write.option("maxRecordsPerFile", 100000).mode("overwrite").parquet(output_path)

if __name__ == "__main__":
    main()

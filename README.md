# Sample MSBA 405 Pipeline (Final Project)
# Pipeline to Power NYC Taxi vs Weather Analysis

**Note:** Do not use this pipeline exactly as is if your project uses these datasets. Focus on other analyses.

In the `README.md`you must provide enough information for us to download the dataset
and run the pipeline on our own machines.

This should be "fire and forget." We should be able to execute one command to run the pipeline.

## The Data

For this analysis we used High Frequency for Hire trips (e.g. Uber, Lyft) data for all of 2024. 

We went to this page: `https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`
and could download the data in Parquet format. Or, we can download all of the data for an entire year using:

```
for m in {01..12}; do
  curl -O "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-$m.parquet"
done
```

By hovering over the link, we see that the URL has the following format.

`https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2024-01.parquet`

which is

`https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2024-[month].parquet`

So we can download one file if we do not wish to download all of them.

`curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2024-01.parquet`

We also downloaded the zone mapping for pickup and dropoff locations from:

`curl -O https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`

It is available on the same website on Taxi Zone Lookup Table.

We then downloaded hourly weather conditions for New York City for NOAA.

`https://www.ncei.noaa.gov/data/local-climatological-data/access/2024/`

The proper weather station ID is `72505394728`. The direct link is:

`curl -O https://www.ncei.noaa.gov/data/local-climatological-data/access/2024/72505394728.csv`

All of the data for 2024 is in this one file so we do not need a loop.

Clone this repo containing our code and move the data files into the data directory of the repo
so the pipeline can read them.

Then run

bash pipeline.sh

TIP FOR STUDENTS: Put your data into the root of your git repo so it's easy to run your
code BUT add the file extensions to .gitignore so that the data is not committed to Github.
The data for this sample pipeline has extension .parquet and .csv. See the .gitignore file
for an example!

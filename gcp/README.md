# Setting up a Bash Pipeline on GCP

In this demo, we will setup a Bash pipeline in GCP using GCS to store our data and code
and Dataproc to process the data.

## Install `gcloud` on GCE

```bash
sudo apt update
sudo apt install -y apt-transport-https ca-certificates gnupg curl

curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
  | sudo gpg --dearmor -o /usr/share/keyrings/google-cloud.gpg

echo "deb [signed-by=/usr/share/keyrings/google-cloud.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
  | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list

sudo apt update
sudo apt install -y google-cloud-cli
```

Authenticate:

```bash
gcloud auth login
```

## Setup your project

First, find your Google Cloud project Id.

```bash
export PROJECT=<YOUR_PROJECT_ID>
gcloud config set project $PROJECT
```

## Configure permissions

Your Google account needs permission to:

* create buckets
* run Dataproc jobs
* upload data

Assign these roles in your project:

```bash
roles/storage.admin
roles/dataproc.admin
roles/iam.serviceAccountUser
```

You can grant them with:

```bash
export YOUR_EMAIL=<YOUR EMAIL ADDRESS>
gcloud projects add-iam-policy-binding $PROJECT \
  --member="user:$YOUR_EMAIL" \
  --role="roles/storage.admin"

gcloud projects add-iam-policy-binding $PROJECT \
  --member="user:$YOUR_EMAIL" \
  --role="roles/dataproc.admin"
```

where `YOUR_EMAIL` is the email associated with the Google account you are using with Cloud.


## Create a GCS bucket

Create a GCS bucket. This bucket **must** be in the same region as your Google Cloud project. You **must**
use `us-central1` as that is the cheapest.

```bash
export BUCKET=<YOUR_BUCKET_NAME>
gcloud storage buckets create gs://$BUCKET --location=us-central1 --uniform-bucket-level-access
```

## Copy your data to the bucket.

For this demo, I am going to store the raw taxi trip data into `gs://msba405demo/raw/fhvhv`. You can use
whichever structure your want.

```bash
gcloud storage cp ../data/fhvhv_tripdata_2024-*.parquet gs://msba405demo/raw/fhvhv/
```

Copy the taxi zone lookup data into raw/reference.

```bash
gcloud storage cp ../data/taxi_zone_lookup.csv gs://msba405demo/raw/reference/
```

Copy the weather data to raw/weather

```bash
gcloud storage cp ../data/72505394728.csv gs://msba405demo/raw/weather/
```

Note that you will need to substitude `../data` with the directory containing the data.

## Copy the Spark job code

Note that the Spark code from the `spark` directory needs some modification to work with GCS paths rather
than local filesystem globs. See `spark-gcp.py` for an example. Run this command from the `gcp` directory.

```bash
gcloud storage cp assets/spark-gcp.py gs://msba405demo/spark/
```

## Run the Spark job on Dataproc Serverless

Dataproc allows us to setup a cluster and run Spark jobs on it. We must specify a variety of configuration
parameters, and if we are not careful, we can incur charges. Instead, we can use Dataproc Serverless which
abstracts away the cluster. We tell GCP what we want and it handles the configuration for us while minimizing
the possibility of an overcharge.

```bash
gcloud dataproc batches submit pyspark gs://msba405demo/spark/spark-gcp.py \
  --region=us-central1 \
  --properties=spark.dynamicAllocation.enabled=true,\
spark.dynamicAllocation.minExecutors=2,\
spark.dynamicAllocation.maxExecutors=8,\
spark.executor.memory=4g,\
spark.executor.cores=4,\
spark.driver.memory=4g \
  -- \
  gs://msba405demo/raw/fhvhv/ \
  gs://msba405demo/raw/reference/taxi_zone_lookup.csv \
  gs://msba405demo/raw/weather/72505394728.csv \
  gs://msba405demo/curated/rides/
```

This particular Python script for the Spark job requires 4 arguments to be passed to it: 

1. Path to taxi ride data
2. Taxi zone lookup CSV
3. Weather data file
4. Output directory

These arguments are passed after the `--` in the above command.

You must change the `msba405demo` bucket to your own bucket's name.

`--properties=spark.dynamicAllocation.enabled=true` enables auto scaling. If Dataproc senses that we need more
compute, it will assign up to at most `maxExecutors` but will always use at least `minExecutors`. If you
do not want to use autoscaling, just delete that line and use `--properties=spark.executor.instances=...` to
specify the number of instances. **Do not** change the memory or cores without first checking with course staff.

## Snowflake

Enter your credentials, data warehouse and database information into `pipeline.env`. **DO NOT** commit this file to Github!

One time setup in Snowflake:

```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINAL;

CREATE DATABASE IF NOT EXISTS NYC_TAXI;
USE DATABASE NYC_TAXI;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

CREATE OR REPLACE STORAGE INTEGRATION gcs_msba405demo_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://msba405demo/curated/rides/');

DESC STORAGE INTEGRATION gcs_msba405demo_int;
-- See Snowflake recording. Grant service account access to GCS bucket.

CREATE OR REPLACE FILE FORMAT rides_parquet_ff
  TYPE = PARQUET
  USE_LOGICAL_TYPE = TRUE;

CREATE OR REPLACE STAGE rides_curated_stage
  URL = 'gcs://msba405demo/curated/rides/'
  STORAGE_INTEGRATION = gcs_msba405demo_int
  FILE_FORMAT = rides_parquet_ff;
```

## The Full Pipeline

Once the data and code are loaded into GCS, you can study the following files:

1. `pipeline.sh` kicks off the Spark job. If successful, it kicks of the Snowflake processing.
2. Snowflake processing loads the data from GCS into Snowflake in staging tables.
3. Snowflake processing generates Tableau views into staging tables.
4. If the process completes successfully, the staging tables are "promoted" to production tables and the staging tables are dropped.

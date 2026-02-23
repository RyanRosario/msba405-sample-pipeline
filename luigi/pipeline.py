import luigi
import logging
import os
import shutil
import time
import duckdb
import subprocess  # Added for better command execution

from luigi.contrib.spark import SparkSubmitTask

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('luigi-interface')

class InputFileTask(luigi.ExternalTask):
    path = luigi.Parameter()
    
    def output(self):
        return luigi.LocalTarget(self.path)

class SparkJobTask(SparkSubmitTask):
    taxi_data = '../data/fhvhv_tripdata_2024-*.parquet'
    zone_data = '../data/taxi_zone_lookup.csv'
    weather_data = '../data/72505394728.csv'
    success_flag = '../output/_OKLUIGI'
    output_dir = '../output'
    job = '../spark/spark-job.py'

    def requires(self):
        return [
                InputFileTask(path=self.zone_data),
                InputFileTask(path=self.weather_data)
        ]

    def output(self):
        return luigi.LocalTarget(self.success_flag)

    def complete(self):
        # We only consider it complete if the flag exists AND the output dir exists
        return os.path.exists(self.success_flag) and os.path.exists(self.output_dir)

    def run(self):
        # 1. Clean up old failed attempts
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        if os.path.exists(self.success_flag):
            os.remove(self.success_flag)

        # 2. Build the command
        cmd = [
            "spark-submit",
            "--master", "local[*]",
            "--conf", "spark.sql.shuffle.partitions=2",
            "--name", "NYC Taxi/Weather Analysis",
            self.job,
            self.taxi_data,
            self.zone_data,
            self.weather_data,
            self.output_dir
        ]

        logger.info(f"🚀 Launching Spark: {' '.join(cmd)}")

        try:
            # 3. Use subprocess.run with check=True to catch non-zero exit codes
            subprocess.run(cmd, check=True)
            
            # 4. Only write success flag if we get here
            with open(self.success_flag, "w") as f:
                f.write(f"Completed at {time.ctime()}")
            logger.info("✅ Spark transformation finished successfully.")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Spark job failed with exit code {e.returncode}")
            # Re-raise so Luigi knows this task actually failed
            raise e

class LoadDuckDBTask(luigi.Task):
    loadpath = luigi.Parameter(default="../output/*.parquet")
    queries = '../duckdb/queries.sql'
    duckdb_file = '../duckdb/final.db'
    duckdb_success = '../output/duckdb_loaded.flag'

    def requires(self):
        return SparkJobTask()

    def output(self):
        return luigi.LocalTarget(self.duckdb_success)

    def run(self):
        sql_file = os.path.abspath(self.queries)
        db_path = os.path.abspath(self.duckdb_file)

        with open(sql_file, "r") as f:
            sql_script = f.read().replace("$LOADPATH", self.loadpath)

        logger.info(f"📥 Loading results into DuckDB: {db_path}")
        con = duckdb.connect(database=db_path, read_only=False)
        con.execute(sql_script)
        con.close()

        with open(self.output().path, "w") as f:
            f.write("Success")

if __name__ == "__main__":
    luigi.build([LoadDuckDBTask()], local_scheduler=True, workers=1)

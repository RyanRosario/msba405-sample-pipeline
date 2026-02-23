import luigi
import logging
import os
import shutil
import time
import duckdb
import subprocess
import glob

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('luigi-interface')

class InputFileTask(luigi.ExternalTask):
    """Checks for the lookup files that MUST exist before we start."""
    path = luigi.Parameter()
    def output(self):
        return luigi.LocalTarget(self.path)

class SparkJobTask(luigi.Task):
    # Paths configured for your 2024 HVFHV data
    taxi_data = '../data/fhvhv_tripdata_2024-*.parquet'
    zone_data = '../data/taxi_zone_lookup.csv'
    weather_data = '../data/72505394728.csv'
    
    # We store this flag in the duckdb folder so it survives the output cleanup
    success_flag = '../duckdb/spark_complete.flag'
    output_dir = '../output'
    job = '../spark/spark-job.py'

    def requires(self):
        # Only require static files; we handle the taxi glob in run()
        return [
            InputFileTask(path=self.zone_data),
            InputFileTask(path=self.weather_data)
        ]

    def output(self):
        return luigi.LocalTarget(self.success_flag)

    def run(self):
        # 1. Manual check for taxi data since Luigi's 'requires' handles globs poorly
        if not glob.glob(self.taxi_data):
            raise RuntimeError(f"No files found matching {self.taxi_data}")

        # 2. Clean up old failed output directory
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
            
        cmd = [
            "spark-submit",
            "--master", "local[*]",
            "--conf", "spark.sql.shuffle.partitions=2",
            "--name", "NYC Taxi/Weather Analysis",
            self.job, self.taxi_data, self.zone_data, self.weather_data, self.output_dir
        ]

        logger.info(f"🚀 Running Spark: {' '.join(cmd)}")
        
        try:
            subprocess.run(cmd, check=True)
            with open(self.success_flag, "w") as f:
                f.write(f"Done: {time.ctime()}")
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Spark failed with exit code {e.returncode}")
            raise e

class LoadDuckDBTask(luigi.Task):
    loadpath = luigi.Parameter(default="../output/*.parquet")
    queries = '../duckdb/queries.sql'

    # UPDATED: Database now goes into the output folder
    duckdb_file = '../output/final.db'
    output_dir = '../output'

    # Master flag remains in the duckdb folder for persistence
    final_success = '../duckdb/pipeline_complete.flag'

    def requires(self):
        return SparkJobTask()

    def output(self):
        return luigi.LocalTarget(self.final_success)

    def run(self):
        sql_file = os.path.abspath(self.queries)
        db_path = os.path.abspath(self.duckdb_file)

        with open(sql_file, "r") as f:
            sql_script = f.read().replace("$LOADPATH", self.loadpath)

        # 1. Load data into DuckDB inside the output folder
        logger.info(f"📥 Loading results into DuckDB at: {db_path}")
        con = duckdb.connect(database=db_path, read_only=False)
        con.execute(sql_script)
        con.close()

        # 2. TARGETED CLEANUP: Only delete the .parquet files
        # We NO LONGER use shutil.rmtree(self.output_dir) because that would delete final.db
        logger.info(f"🗑️ Cleaning up intermediate Parquet files in {self.output_dir}")
        for f in glob.glob(os.path.join(self.output_dir, "*.parquet")):
            os.remove(f)

        # 3. Write the final flag
        with open(self.final_success, "w") as f:
            f.write(f"Pipeline finished. DB at {self.duckdb_file} - {time.ctime()}")

    def requires(self):
        return SparkJobTask()

    def output(self):
        return luigi.LocalTarget(self.final_success)

    def run(self):
        sql_file = os.path.abspath(self.queries)
        db_path = os.path.abspath(self.duckdb_file)

        with open(sql_file, "r") as f:
            sql_script = f.read().replace("$LOADPATH", self.loadpath)

        logger.info(f"📥 Loading results into DuckDB: {db_path}")
        con = duckdb.connect(database=db_path, read_only=False)
        con.execute(sql_script)
        con.close()

        # CLEANUP: Delete the giant output folder now that DuckDB has the data
        if os.path.exists(self.output_dir):
            logger.info(f"🗑️ Cleaning up {self.output_dir}")
            shutil.rmtree(self.output_dir)

        # Write the final flag
        with open(self.final_success, "w") as f:
            f.write(f"Pipeline finished and cleaned at {time.ctime()}")

if __name__ == "__main__":
    luigi.build([LoadDuckDBTask()], local_scheduler=False, workers=1)

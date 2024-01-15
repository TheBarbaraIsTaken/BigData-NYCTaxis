"""
=== Step 2 ===
This code reads each file and filters out every row,
where the date (pick up or drop off) is not in a +/- 1 month interval of the date in the file name.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, add_months, lit

spark = SparkSession.builder.getOrCreate()


start_year = 2011
end_year = 2023

for y in range(start_year, start_year+1):
    end_month = 12
    if y == 2023:
        end_month = 9
    for i in range(1, end_month+1):
        # Formatted month for the file name
        m = str(i).zfill(2)

        # Read each parquet file
        path = f"/user/s3263371/project/taxi/yellow_tripdata_{y}-{m}.parquet"
        df = spark.read.parquet(path)

        # Filter rows based on the condition (more than one month before or after the date in file name)
        # Pick up date
        df_filtered = df.filter(
            (col("tpep_pickup_datetime") >= add_months(lit(f"{y}-{m}-01 00:00:00"), -1)) &
            (col("tpep_pickup_datetime") < add_months(lit(f"{y}-{m}-01 00:00:00"), 2))
        )

        # Drop off date
        df_filtered = df_filtered.filter(
            (col("tpep_dropoff_datetime") >= add_months(lit(f"{y}-{m}-01 00:00:00"), -1)) &
            (col("tpep_dropoff_datetime") < add_months(lit(f"{y}-{m}-01 00:00:00"), 2))
        )

        # Overwrite results on hdfs
        # out = f"/user/s3266443/dataset/taxi/clean_tripdata_{y}-{m}.parquet"
        out = f"/user/s3263371/project/taxi/clean_tripdata_{y}-{m}.parquet"
        df_filtered.write.parquet(out, mode="overwrite")

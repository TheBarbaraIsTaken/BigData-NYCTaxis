"""
=== Step 1 ===
This program examines pick up dates in the raw data.
Counts each row grouped by year and month for each file individually
to gain information about faulty data
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, count

spark = SparkSession.builder.getOrCreate()

# Uncomment and change output path
# f = open(f"/home/s3263371/project_script/out/clear/pickup_dates.txt", 'a')
# f = open(f"/home/s3266443/BigData-NYCTaxis/out/clear/pickup_dates.txt", 'a')

start_year = 2011
end_year = 2023
for y in range(start_year, end_year+1):
    end_month = 12

    if y == 2023:
        end_month = 9

    for i in range(1, end_month+1):
        # Formatted month for the file name
        m = str(i).zfill(2)

        # Read each parquet files
        path = f"/user/s3263371/project/taxi/yellow_tripdata_{y}-{m}.parquet"
        df = spark.read.parquet(path)

        df = df.select("tpep_pickup_datetime", "tpep_dropoff_datetime")

        # Group by year and month
        grouped_df = df.groupBy(year("tpep_pickup_datetime").alias("pickup_year"), month("tpep_pickup_datetime").alias("pickup_month")).agg(count("*").alias("count"))
        sorted_df = grouped_df.orderBy("pickup_year", "pickup_month")

        # Write results to file
        print(f"============================================={y}.{m}.==================================================", file=f)
        print(*sorted_df.take(35), sep='\n', file=f)

f.close()

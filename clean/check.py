"""
=== Step 4 ===
This program is for checking cleaned data.
It reads all files and groups by year
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count

spark = SparkSession.builder.getOrCreate()

# Read files
path = f"/user/s3263371/project/taxi/clean_tripdata_*.parquet"
df = spark.read.parquet(path)

grouped_df = df.groupBy(year("tpep_pickup_datetime").alias("pickup_year")).agg(count("*").alias("count"))
sorted_df = grouped_df.orderBy("pickup_year")

# Write output
# Uncomment and change output path
# n = 20
# with open(f"/home/s3263371/project_script/out/clear/check.txt", 'w') as f:
#     print(f"First {n} rows of {sorted_df.count()}\n", file=f)
#     print(*sorted_df.take(n), sep='\n', file=f)

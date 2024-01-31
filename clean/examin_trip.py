"""
=== Step 2.5 ===
This program examines trip information:
distance, locations, travel time and tip
and writes the distribution in csv files
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, unix_timestamp, round, lit
import csv

spark = SparkSession.builder.getOrCreate()

# Read file
path = "/user/s3263371/project/taxi/temp/clean_tripdata_*.parquet"
df = spark.read.parquet(path)

## Trip distance
df_trip_distance = df.select((round("trip_distance",0)).alias("trip_distance"))
df_trip_distance = df_trip_distance.groupBy("trip_distance").agg(count("*").alias("count"))

## Location IDs
df_location = df.select("PULocationID", "DOLocationID")
df_PULocationID = df_location.groupBy("PULocationID").agg(count("*").alias("count"))
df_DOLocationID = df_location.groupBy("DOLocationID").agg(count("*").alias("count"))

## Travel time
df_delta_min = df.select((round((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / lit(60), 0)).alias("delta_min"))
df_delta_min = df_delta_min.select((round("delta_min",0)).alias("delta_min"))
df_delta_min = df_delta_min.groupBy("delta_min").agg(count("*").alias("count"))

## Tip amount
df_tip_amount = df.select((round("tip_amount",0)).alias("tip_amount"))
df_tip_amount = df_tip_amount.groupBy("tip_amount").agg(count("*").alias("count"))

# Uncomment and change output path
# Write results to output
# output_path = "/home/s3263371/project_script/out/clear/"

with open(output_path + "trip_distance_counts.csv", "w") as f:
    csvwriter = csv.writer(f)
    csvwriter.writerow(df_trip_distance.columns)
    csvwriter.writerows(df_trip_distance.collect())

with open(output_path + "PULocationID_counts.csv", "w") as f:
    csvwriter = csv.writer(f)
    csvwriter.writerow(df_PULocationID.columns)
    csvwriter.writerows(df_PULocationID.collect())

with open(output_path + "DOLocationID_counts.csv", "w") as f:
    csvwriter = csv.writer(f)
    csvwriter.writerow(df_DOLocationID.columns)
    csvwriter.writerows(df_DOLocationID.collect())

with open(output_path + "delta_min_counts.csv", "w") as f:
    csvwriter = csv.writer(f)
    csvwriter.writerow(df_delta_min.columns)
    csvwriter.writerows(df_delta_min.collect())

with open(output_path + "tip_amount_counts.csv", "w") as f:
    csvwriter = csv.writer(f)
    csvwriter.writerow(df_tip_amount.columns)
    csvwriter.writerows(df_tip_amount.collect())


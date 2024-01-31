from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, count, avg
from pyspark.sql.types import LongType

spark = SparkSession.builder.getOrCreate()

# Read files
path = f"/user/s3263371/project/taxi/clean_tripdata_*.parquet"
df = spark.read.parquet(path)

grouped_df = df.groupBy([hour("tpep_pickup_datetime").alias("pickup_hour"), "PULocationID", "DOLocationID"]).agg(count("*").alias("count"), avg("tip_amount").alias("avg_tip"))

grouped_df.coalesce(1).write.csv("/user/s2176610/heatmapdata.csv", mode="overwrite")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, unix_timestamp, explode, sequence, date_format

spark = SparkSession.builder.getOrCreate()

path = f"/user/s3266443/dataset/taxi/clean_tripdata_*.parquet"
df = spark.read.parquet(path)


def drop_cols(dataframe):
    # Not interesting
    dataframe = dataframe.drop("VendorID")
    dataframe = dataframe.drop("store_and_fwd_flag")
    dataframe = dataframe.drop("RatecodeID")
    dataframe = dataframe.drop("airport_fee")
    return dataframe


df = drop_cols(df)

df = df.withColumn("ride_duration",
                   (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60)

df = df.withColumn("traffic", col("trip_distance") / col("ride_duration"))

per_hour_stats = (df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
                  .groupBy("pickup_hour")
                  .agg({"tip_amount": "avg", "traffic": "avg"})
                  .withColumnRenamed("avg(tip_amount)", "average_tip")
                  .withColumnRenamed("avg(traffic)", "average_traffic")
                  .orderBy("pickup_hour"))

traffic_stats = (df.groupBy("PULocationID")
                 .agg({"traffic": "avg"})
                 .withColumnRenamed("avg(traffic)", "average_traffic")
                 .orderBy("PULocationID"))

hourly_traffic = (df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
                  .withColumn("dropoff_hour", hour("tpep_dropoff_datetime"))
                  .withColumn("hours", explode(sequence(col("pickup_hour"), col("dropoff_hour") - 1)))
                  .groupBy("hours")
                  .count()
                  .withColumnRenamed("count", "active rides"))

unique_days_count = df.withColumn("pickup_date", date_format("tpep_pickup_datetime", "yyyy-MM-dd")).select("pickup_date").distinct().count()
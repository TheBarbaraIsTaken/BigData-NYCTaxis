from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, count

from collections import Counter
from pyspark import SparkContext
from math import sqrt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import matplotlib.pyplot as plt

"""
Command to execute:
> cd /home/s3266443/project-group23/
> time spark-submit --conf spark.dynamicAllocation.maxExecutors=10 main.py 2> /dev/null

Files to read in these folders:
> hdfs dfs -ls /user/s3263371/project/buildings
> hdfs dfs -ls /user/s3263371/project/taxi
"""

spark = SparkSession.builder.getOrCreate()

# Read files
path = f"/user/s3266443/dataset/taxi/clean_tripdata_*.parquet"
df = spark.read.parquet(path)


def drop_cols(df):
    # Not interesting
    df = df.drop("VendorID")
    df = df.drop("store_and_fwd_flag")
    df = df.drop("RatecodeID")
    """
    === airport_fee ===
    Running the following throws error:
    > df.select("airport_fee").distinct().show()
    Solution:
    - Cast "null" values to "Float": not working
        # df = df.withColumn("airport_fee", col("airport_fee").cast("double"))
    - Dropping column
    """
    df = df.drop("airport_fee")
    return df

df = drop_cols(df)

"""
>>> df.show()
+--------------------+---------------------+---------------+-------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+
|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|
+--------------------+---------------------+---------------+-------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+
| 2011-01-01 00:10:00|  2011-01-01 00:12:00|              4|          0.0|         145|         145|           1|        2.9|  0.5|    0.5|      0.28|         0.0|                  0.0|        4.18|                null|
| 2011-01-01 00:04:00|  2011-01-01 00:13:00|              4|          0.0|         264|         264|           1|        5.7|  0.5|    0.5|      0.24|         0.0|                  0.0|        6.94|                null|
| 2011-01-01 00:14:00|  2011-01-01 00:16:00|              4|          0.0|         264|         264|           1|        2.9|  0.5|    0.5|      1.11|         0.0|                  0.0|        5.01|                null|
| 2011-01-01 00:04:00|  2011-01-01 00:06:00|              5|          0.0|         146|         146|           1|        2.9|  0.5|    0.5|       0.0|         0.0|                  0.0|         3.9|                null|
| 2011-01-01 00:08:00|  2011-01-01 00:08:00|              5|          0.0|         146|         146|           1|        2.5|  0.5|    0.5|      0.11|         0.0|                  0.0|        3.61|                null|
| 2011-01-01 00:23:00|  2011-01-01 00:23:00|              1|          0.0|         146|         146|           2|        2.5|  0.5|    0.5|       0.0|         0.0|                  0.0|         3.5|                null|
| 2011-01-01 00:25:00|  2011-01-01 00:25:00|              1|          0.0|         146|         146|           2|        2.5|  0.5|    0.5|       0.0|         0.0|                  0.0|         3.5|                null|
+--------------------+---------------------+---------------+-------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+

>>> df.count()
176.887.249
"""


def read_zones():
    # Read zones
    path = "/user/s3266443/dataset/zone-mapping/zoning.parquet"
    df = spark.read.parquet(path, header=True)
    return df

df_zones = read_zones()

# Join zones
df =  df.join(df_zones, df.PULocationID == df_zones.LocationID, 'left').drop(df_zones.LocationID)


def tip_amount_by_zone(df):
    # Chart tipping amount (y-axis) by zone (x-axis)
    df_tip = df.filter(df.Zone != "Unknown")
    df_tip = df_tip.groupBy("Zone").agg(F.mean("tip_amount").alias("tip_amount"))
    df_tip = df_tip.orderBy("tip_amount", ascending=False)
    df_tip = df_tip.dropna()
    df_tip.show()

    df_tip_pd = df_tip.toPandas()
    plt.bar(df_tip_pd["Zone"], df_tip_pd["tip_amount"])
    plt.xlabel('Zone')
    plt.ylabel('Tip Amount')
    plt.title('Tip Amount by Zone')
    plt.xticks(rotation=15)
    plt.savefig("tip_amount.png")

tip_amount_by_zone(df)

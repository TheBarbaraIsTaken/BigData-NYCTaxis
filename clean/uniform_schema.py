"""
=== Step 1.3 ===
This program makes the schema of each file the same
By casting columns to the same type and adding missing columns
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.getOrCreate()

# Use the first file for uniform schema
input_path = f"/user/s3263371/project/taxi/yellow_tripdata_2011-01.parquet"
first_df = spark.read.parquet(input_path)
correct_schema = first_df.schema


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

        # Cast columns
        if y != 2019:
            df = df.select(
                col("VendorID").cast("long"),
                col("tpep_pickup_datetime").cast("timestamp"),
                col("tpep_dropoff_datetime").cast("timestamp"),
                col("passenger_count").cast("long"),
                col("trip_distance").cast("double"),
                col("RatecodeID").cast("long"),
                col("store_and_fwd_flag").cast("string"),
                col("PULocationID").cast("long"),
                col("DOLocationID").cast("long"),
                col("payment_type").cast("long"),
                col("fare_amount").cast("double"),
                col("extra").cast("double"),
                col("mta_tax").cast("double"),
                col("tip_amount").cast("double"),
                col("tolls_amount").cast("double"),
                col("improvement_surcharge").cast("double"),
                col("total_amount").cast("double"),
                col("congestion_surcharge").cast("double"),
                col("airport_fee").cast("double")
            )
        else:
            df = df.select(
                col("VendorID").cast("long"),
                col("tpep_pickup_datetime").cast("timestamp"),
                col("tpep_dropoff_datetime").cast("timestamp"),
                col("passenger_count").cast("long"),
                col("trip_distance").cast("double"),
                col("RatecodeID").cast("long"),
                col("store_and_fwd_flag").cast("string"),
                col("PULocationID").cast("long"),
                col("DOLocationID").cast("long"),
                col("payment_type").cast("long"),
                col("fare_amount").cast("double"),
                col("extra").cast("double"),
                col("mta_tax").cast("double"),
                col("tip_amount").cast("double"),
                col("tolls_amount").cast("double"),
                col("improvement_surcharge").cast("double"),
                col("total_amount").cast("double"),
                col("congestion_surcharge").cast("double"),
            )

            # Add missing column with null values
            df = df.withColumn("airport_fee", lit(None).cast("double"))

        df = spark.createDataFrame(df.rdd, correct_schema)

        # Overwrite results on hdfs
        out = f"/user/s3263371/project/taxi/temp/clean_tripdata_{y}-{m}.parquet"
        df.write.parquet(out, mode="overwrite")

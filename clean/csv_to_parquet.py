"""
=== Step 1.1 ===
This program converts csv files to parquet
"""
from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("csv2parquet").getOrCreate()

folder_path = '/user/s3263371/project/taxi/'

# Construct the CSV file paths
input_files = [f'{folder_path}yellow_tripdata_2019-{str(i).zfill(2)}.csv' for i in range(1,13)]

# Construct the Parquet file paths
output_files = [f'{folder_path}yellow_tripdata_2019-{str(i).zfill(2)}.parquet' for i in range(1,13)]

assert len(input_files) == len(output_files), "Number of input and output files must be equal"

files = zip(input_files, output_files)

# Process
for csv_file_path, parquet_file_path in files:
    # Read CSV into a DataFrame
    df = spark.read.csv(csv_file_path, header=True)

    # Write DataFrame to Parquet
    df.write.parquet(parquet_file_path, mode='overwrite')


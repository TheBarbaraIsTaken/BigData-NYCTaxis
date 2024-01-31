"""
=== Step 1.2 ===
This program examines the columns in each year
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Uncomment and change output path
# f = open("/home/s3263371/project_script/out/default_columns.txt", 'w')

start_year = 9
end_year = 23

for y in range(start_year, end_year+1):
    # Read file
    parquet_path = f"/user/s3263371/project/taxi/yellow_tripdata_20{str(y).zfill(2)}-*.parquet"
    df = spark.read.parquet(parquet_path)

    print(*df.columns, file=f)

f.close()

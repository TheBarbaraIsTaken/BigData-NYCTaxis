"""
=== Step 2.1 ===
This program examines corrputed records in the files
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

CORRUPT_COL = "_corrupt_record"

# Uncomment and change output path
# f = open("/home/s3263371/project_script/out/corrupted.txt", "w")
print("Original files:", file=f)

# Read file
path_original = "/user/s3263371/project/taxi/temp/clean_tripdata_*.parquet"
df = spark.read.format("parquet").options(mode="PERMISSIVE", columnNameOfCorruptRecord=CORRUPT_COL).load(path_original)

if CORRUPT_COL in df.columns:
    df_corrupted = df.filter(col("_corrupt_record").isNotNull())
    print(f"Number of corrputed records: {df_corrupted.count()} out of {df.count()}.", file=f)
else:
    print("No corrupted columns found", file=f)

f.close()

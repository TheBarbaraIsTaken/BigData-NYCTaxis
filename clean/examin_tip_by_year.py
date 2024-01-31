"""
=== Step 2.3 ===
This program examines basic information about tips by year
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

spark = SparkSession.builder.getOrCreate()

# Uncomment and change output path
# f = open("/home/s3263371/project_script/out/clear/tip_by_year.txt", "w")
print("year total null_count non_zero_count non_zero_tip_avg tip_avg", file=f)

tips = ["Tip_Amt"] + ["tip_amount" for i in range(14)]

for ind, y in enumerate(range(9, 24)):
    tip = tips[ind]

    # Read file
    parquet_path = f"/user/s3263371/project/taxi/yellow_tripdata_20{str(y).zfill(2)}-*.parquet"
    df = spark.read.parquet(parquet_path)

    # Counts
    total = df.count()

    null_count = df.filter(col(tip).isNull()).count()

    filtered_df = df.filter(col(tip) != 0)
    non_zero_count = filtered_df.count()

    not_zero_avg = filtered_df.agg(avg(tip).alias("AverageTip")).collect()[0]["AverageTip"]

    avg_tip = df.agg(avg(tip).alias("AverageTip")).collect()[0]["AverageTip"]
    
    # Write output
    print(f"20{str(y).zfill(2)}", total, null_count, non_zero_count, not_zero_avg, avg_tip, file=f)

f.close()

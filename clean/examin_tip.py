"""
=== Step 2.4 ===
This program examines basic information about tips
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, udf, percentile_approx
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate()

# Read data
path = f"/user/s3263371/project/taxi/temp/clean_tripdata_*.parquet"
df = spark.read.parquet(path)
df = df.select("tip_amount", "payment_type")

total_count = df.count()

# False data
non_negative_df = df.filter(col("tip_amount") >= 0)
non_negative_count = non_negative_df.count()
null_count = df.filter(col("tip_amount").isNull()).count()

# Payment type
payment_num = 6
payment_dict = {
    1: 'Credit card',
    2: 'Cash',
    3: 'No charge',
    4: 'Dispute',
    5: 'Unknown',
    6: 'Voided trip'
}
grouped_payment_df = df.groupBy(col("payment_type").alias("payment_type")).agg(count("*").alias("count"))
map_payment = udf(lambda x: payment_dict[x], StringType())
# Don't uncomment
# Payment types doesn't follow the convetion of the dataset description
# grouped_payment_df = grouped_payment_df.select(map_payment("payment_type").alias("payment_type"), "count")

# Not credit card but has a tip
credit_card = 1
filtered_df = df.filter((col("tip_amount") != 0))
grouped_tip_df = filtered_df.groupBy("payment_type").agg(count("*").alias("count"))
# grouped_tip_df = grouped_tip_df.select(map_payment("payment_type").alias("payment_type"), "count")

temp_df = df.filter(col("tip_amount") > 10)
quantile_df = temp_df.select(percentile_approx("tip_amount", [0.25, 0.5, 0.75], 1000000).alias("quantiles"))

# Uncomment and change output path
# Write output
# with open("/home/s3263371/project_script/out/clear/examin_tip.txt", "w") as f:
#     print("Negative counts:", total_count-non_negative_count, file=f)
#     print("Quantiles:", quantile_df.take(1), file=f)
#     print(f"Null counts: {null_count}\n", file=f)
#     # print("Payment type counts:", *grouped_payment_df.take(payment_num), sep='\n', file=f)
#     print("\nPayment type counts (where tip is not zero):", *grouped_tip_df.take(payment_num), sep='\n', file=f)

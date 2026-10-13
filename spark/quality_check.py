from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("NYCTaxiQualityCheck").getOrCreate()
file_path = "data/raw/yellow_tripdata_2026-01.parquet"

df= spark.read.parquet(file_path)

# Data quality check through statistics to find some unusual data

# print(df.columns)

df.select(
"passenger_count",
"trip_distance",
"fare_amount",
"tip_amount",
"tolls_amount",
"total_amount",
"Airport_fee"
).describe().show()

print("\nTotal Rows:", df.count())


# ## -------------------------FINDING INVALID CANDIDATES
df.filter(
    (col("passenger_count") <= 0) |
    (col("trip_distance") <= 0) |
    (col("fare_amount") < 0) |
    (col("total_amount") < 0)
).select(
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "tolls_amount",
    "total_amount",
    "Airport_fee"
).show(10, truncate= False)

print("====== time check =====")
df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
).show(10, truncate= False)

# for i in df.columns:
#     print(i)

spark.stop()
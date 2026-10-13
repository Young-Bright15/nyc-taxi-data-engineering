from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    round,
    avg,
    sum
)

spark = SparkSession.builder.appName("NYCTaxiGold").getOrCreate()

input_path= "data/processed/yellow_tripdata_2026-01-transformed.parquet"

df= spark.read.parquet(input_path)

daily_summary= df.groupBy(
    "pickup_date"
).agg(
    count("*").alias("total_daily_trips"),
    round(avg("trip_distance"), 2).alias("avg_distance"),
    round(avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
    round(avg("fare_amount"), 2).alias("avg_fare"),
    round(sum("total_amount"),2).alias("total_daily_amount")
)

print("Daily Trip Summary")
daily_summary.orderBy(
    "pickup_date"
).show(20)

hourly_summary= df.groupBy(
    "pickup_hour"
).agg(
    count("*").alias("total_hourly_trips"),
    avg("trip_distance").alias("avg_distance"),
    avg("fare_amount").alias("avg_fare")
)

print("hourly summary data")
hourly_summary.orderBy(
    "pickup_hour"
).show(30, truncate=False)

daily_summary.write.mode("overwrite").parquet("data/gold/daily_trip_summary.parquet")
hourly_summary.write.mode("overwrite").parquet("data/gold/hourly_trip_summary.parquet")

spark.stop()
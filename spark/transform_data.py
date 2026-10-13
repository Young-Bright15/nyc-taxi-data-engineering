from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, to_date, hour, dayofweek, when, unix_timestamp, round)

spark = SparkSession.builder.appName("NYCTaxi_Data Transform").getOrCreate()

input_path = "data/processed/yellow_tripdata_2026-01.parquet"
output_path = "data/processed/yellow_tripdata_2026-01-transformed.parquet"

df= spark.read.parquet(input_path)

### create columns(pickup_date, pickup_hour, pickup_day_of_week, trip_duration_minute, fare_per_mile,)
df= df.withColumn(
    "pickup_date",
    to_date(col("tpep_pickup_datetime"))
)
df= df.withColumn(
    "pickup_hour",
    hour(col("tpep_pickup_datetime"))
)
df=df.withColumn(
    "pickup_day_of_week",
    dayofweek(col("tpep_pickup_datetime"))
)
df=df.withColumn(
    "fare_per_mile",
    when(
        col("fare_amount") > 0,
        round((col("fare_amount")/col("trip_distance")),2)
    )
)
df= df.withColumn(
    "trip_duration_minutes",
    round((
        unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))
    )/60, 2)
)
## unix_timestamp() calculates the time in seconds so /60 converts it to minutes.

df.write.mode("overwrite").parquet(output_path)
print("\nTransformed data saved to :\n", output_path)

df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pickup_date",
    "pickup_hour",
    "pickup_day_of_week",
    "trip_distance",
    "trip_duration_minutes",
    "fare_amount",
    "fare_per_mile"
).filter(
    col("trip_distance") > 0
).show(30, truncate= False)

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("NYCTaxiClean").getOrCreate()

input_file_path = "data/raw/yellow_tripdata_2026-01.parquet"
output_file_path = "data/processed/yellow_tripdata_2026-01.parquet"

df= spark.read.parquet(input_file_path)

print("===rows before cleaning:===", df.count())

clean_df= df.filter(
    col("tpep_pickup_datetime").isNotNull()
).filter(
    col("tpep_dropoff_datetime").isNotNull()
).filter(
    col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")
).filter(
    col("passenger_count") >0
).filter(
    col("trip_distance") >0
)

print("===rows after cleaning:===", clean_df.count())
print("===rejected row count:===", abs(clean_df.count()-df.count()))

clean_df.write.mode("overwrite").parquet(output_file_path)

print("Cleaned data successfully saved to:", output_file_path)

clean_df.show(10, truncate=False)

spark.stop()
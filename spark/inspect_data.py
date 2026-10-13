from pyspark.sql import SparkSession
from pathlib import Path

spark = SparkSession.builder.appName("NYCTaxiInspection").getOrCreate()
file_path = "data/raw/yellow_tripdata_2026-01.parquet"

df= spark.read.parquet(file_path)
df.printSchema()
df.show(5, truncate= False)

print("ROW COUNT\n")
print("Number Of Rows:", df.count())

print(df.columns)
spark.stop()
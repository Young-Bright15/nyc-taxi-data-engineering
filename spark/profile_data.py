from pyspark.sql import SparkSession
from pyspark.sql.functions import when, count, col

spark = SparkSession.builder.appName("NYCTaxiProfiling").getOrCreate()
file_path = "data/raw/yellow_tripdata_2026-01.parquet"

df = spark.read.parquet(file_path)
### CHECKING THE PROFILE OF OUR DATA

print("Number of rows:", df.count())
print("\nNumber of columns:", len(df.columns))

print("NYC_TAXI_DATA_COLUMNS:\n")
print(df.columns)

#for every column in our dataset count how many rows have a null value
print("\nCOLUMNS WITH MISING VALUES")
missing_values = df.select(
    [
        count(
            when(col(c).isNull(), 0)
        ).alias(c)
        for c in df.columns
    ]
)
missing_values.show(truncate=False)

spark.stop()

import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

server = "localhost"
database = "NYC_Taxi_Analytics"

connection_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database};"
    "Trusted_Connection=yes;"
)

connection_url=(
    "mssql+pyodbc:///?odbc_connect="
    + quote_plus(connection_string)
)

engine= create_engine(connection_url)
with engine.connect() as connection:
    print("\nSuccessfully connected to SQL Server\n")

##########READ THE GOLD DATA WITH PANDAS
daily_df= pd.read_parquet("data/gold/daily_trip_summary.parquet")
hourly_df= pd.read_parquet("data/gold/hourly_trip_summary.parquet")

############ LOAD THE DATA INTO SQL SERVER

daily_df.to_sql(
    "DailyTripSummary",
    con=engine,
    if_exists="replace",
    index=False
)

print("DailyTripSummary successfully loaded to sql server")

hourly_df.to_sql(
    "HourlyTripSummary",
    con=engine,
    if_exists="replace",
    index=False
)

print("HourlyTripSummary successfully loaded to sql server.")

engine.dispose()
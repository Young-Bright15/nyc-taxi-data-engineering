import requests
from pathlib import Path

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"
output_path= Path("data/raw/yellow_tripdata_2026-01.parquet")

response = requests.get(url)
response.raise_for_status()                 # this is to check whether the request succeeds

output_path.parent.mkdir(
    parents=True,
    exist_ok=True
)                                         # this is to create the output directory. It's ok if it already exists.

with open(output_path, "wb") as file:
    file.write(response.content)

print("Download Complete")
print("Saved to", output_path)



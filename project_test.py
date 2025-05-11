import os
from datetime import datetime
from functools import reduce

import pandas as pd
import pandas_datareader.data as web

# Define the directory paths
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

# Define the FRED series codes
FRED_SERIES = {
    "UNRATE": "Unemployment Rate",
    "CPIAUCSL": "Consumer Price Index",
    "INDPRO": "Industrial Production",
    "FEDFUNDS": "Federal Funds Rate",
    "GDP": "Gross Domestic Product",
    "PCE": "Personal Consumption Expenditures",
    "SP500": "S&P 500 Index",
}

# Date range
START_DATE = "2015-01-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")

print("\n📊 Downloading economic indicators from FRED...")


def download_fred_data(series_code, name):
    df = web.DataReader(series_code, "fred", START_DATE, END_DATE)
    df = df.reset_index()
    df.rename(columns={series_code: name}, inplace=True)
    df.to_csv(f"{RAW_DIR}/{series_code}.csv", index=False)
    print(f"✅ Downloaded {name} ({series_code}), rows: {len(df)})")
    return df


# Download data
all_data = {}
for code, name in FRED_SERIES.items():
    all_data[code] = download_fred_data(code, name)

print("\n🔗 Merging economic indicators...")

# Load and process data
indicators = []
for code, name in FRED_SERIES.items():
    df = pd.read_csv(f"{RAW_DIR}/{code}.csv", parse_dates=["DATE"])

    if code == "SP500":
        # Convert daily S&P 500 data to monthly and align to first day of the month
        df = df.set_index("DATE").resample("M").last().reset_index()
        df["DATE"] = df["DATE"].dt.to_period("M").dt.to_timestamp()
        print(f"📈 Converted S&P 500 to monthly (start of month), rows: {len(df)})")

    indicators.append(df)

# Merge all data on "DATE"
merged_df = reduce(
    lambda left, right: pd.merge(left, right, on="DATE", how="inner"),
    indicators,
)

print(f"✅ Merged indicators shape: {merged_df.shape}")
print(f"   Date range: {merged_df['DATE'].min()} to {merged_df['DATE'].max()}")

# Save merged data
merged_df.to_csv(f"{PROCESSED_DIR}/merged_data.csv", index=False)
print(f"\n✅ Merged data saved to {PROCESSED_DIR}/merged_data.csv")

# Show first few rows
print("\nFirst 5 rows:")
print(merged_df.head())

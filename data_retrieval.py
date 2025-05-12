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
    "UNRATE": "Unemployment Rate",  # Monthly
    "CPIAUCSL": "Consumer Price Index",  # Monthly
    "INDPRO": "Industrial Production",  # Monthly
    "FEDFUNDS": "Federal Funds Rate",  # Monthly
    "GDP": "Gross Domestic Product",  # Quarterly
    "PCE": "Personal Consumption Expenditures",  # Quarterly
    "SP500": "S&P 500 Index",  # Daily -> Monthly
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
        # Convert daily S&P 500 to monthly (last value of the month)
        df = df.set_index("DATE").resample("M").last().reset_index()
        df["DATE"] = df["DATE"].dt.to_period("M").dt.to_timestamp()
        print(f"📈 Converted S&P 500 to monthly, rows: {len(df)})")
    else:
        # Round all dates to month start (force to monthly grid)
        df["DATE"] = df["DATE"].dt.to_period("M").dt.to_timestamp()

    df = df.sort_values("DATE")
    indicators.append(df)

# Create a full monthly date range from START_DATE to END_DATE
date_range = pd.date_range(start=START_DATE, end=END_DATE, freq="MS")
merged_df = pd.DataFrame({"DATE": date_range})

# Merge each indicator one by one (using outer join, then forward-fill missing)
for df in indicators:
    merged_df = pd.merge(merged_df, df, on="DATE", how="left")

# Fill missing quarterly values forward
merged_df = merged_df.ffill()

print(f"✅ Merged indicators shape: {merged_df.shape}")
print(f"   Date range: {merged_df['DATE'].min()} to {merged_df['DATE'].max()}")

# Save merged data
merged_df.to_csv(f"{PROCESSED_DIR}/merged_data.csv", index=False)
print(f"\n✅ Merged data saved to {PROCESSED_DIR}/merged_data.csv")

# Show first few rows
print("\nFirst 5 rows:")
print(merged_df.head())

# Fix missing data before scaling
filled_df = merged_df.copy()
filled_df = filled_df.fillna(method="ffill").fillna(
    method="bfill"
)  # forward fill then backward fill

# Normalization (Indexing to 100 at START_DATE)
normalized_df = filled_df.copy()
for col in normalized_df.columns:
    if col != "DATE":
        first_value = normalized_df[col].iloc[0]  # Use first row (after fill)
        normalized_df[col] = (normalized_df[col] / first_value) * 100

# Save normalized data
normalized_df.to_csv(f"{PROCESSED_DIR}/merged_data_normalized.csv", index=False)
print(
    f"✅ Normalized (indexed) data saved to {PROCESSED_DIR}/merged_data_normalized.csv"
)

# Min-Max Scaling (0-1 scale)
minmax_df = filled_df.copy()
for col in minmax_df.columns:
    if col != "DATE":
        min_val = minmax_df[col].min()
        max_val = minmax_df[col].max()
        minmax_df[col] = (minmax_df[col] - min_val) / (max_val - min_val)

# Save min-max scaled data
minmax_df.to_csv(f"{PROCESSED_DIR}/merged_data_minmax.csv", index=False)
print(f"✅ Min-Max scaled data saved to {PROCESSED_DIR}/merged_data_minmax.csv")

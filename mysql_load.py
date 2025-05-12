import os

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text

# CONF
MYSQL_USER = "root"
MYSQL_PASSWORD = "password"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "economic_dashboard"

# CONNECTION
engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
)

# create the db if it not exists
with engine.connect() as conn:
    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}"))
    conn.execute(text(f"USE {MYSQL_DB}"))

# upload raw CSV files
raw_folder = "data/raw"
csv_files = [f for f in os.listdir(raw_folder) if f.endswith(".csv")]

for csv_file in csv_files:
    # indicator name from filename
    indicator_name = os.path.splitext(csv_file)[0].lower()

    # load CSV
    df = pd.read_csv(os.path.join(raw_folder, csv_file), parse_dates=["DATE"])

    # upload to MySQL
    df.to_sql(
        indicator_name,
        con=engine,
        if_exists="replace",
        index=False,
        schema=MYSQL_DB,
        dtype={
            "DATE": sqlalchemy.types.DateTime(),
            "value": sqlalchemy.types.Float(),
        },
    )
    print(f"✅ Loaded {len(df)} rows into `{indicator_name}` table")

print("🎉 All raw data loaded into MySQL!")


# Upload the merged data file
print("\n🚀 Uploading merged data...")

# Load merged CSV
merged_df = pd.read_csv("data/processed/merged_data.csv", parse_dates=["DATE"])

# Upload process
merged_df.to_sql(
    "merged_data",
    con=engine,
    if_exists="replace",
    index=False,
    schema=MYSQL_DB,
    dtype={col: sqlalchemy.types.Float() for col in merged_df.columns if col != "DATE"}
    | {"DATE": sqlalchemy.types.DateTime()},
)

print(f"✅ Loaded {len(merged_df)} rows into `merged_data` table")
print("🎉 All data (raw + merged) loaded into MySQL!")

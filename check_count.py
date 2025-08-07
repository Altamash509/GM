import pandas as pd
import os
from datetime import datetime

# Set today’s date
today_str = datetime.today().strftime("%Y-%m-%d")

# Construct the path to the extracted CSV
csv_path = os.path.join(
    "green_marble", "green_marble", "ingestion", "data", "extracted_data",
    f"indiamart_products_{today_str}.csv"
)

# Read and show info
if os.path.exists(csv_path):
    df = pd.read_csv(csv_path)
    print(f"✅ Total products scraped: {len(df)}\n")
    print("📌 Sample products:")
    print(df.head())
else:
    print(f"❌ File not found at path: {csv_path}")

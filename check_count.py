import pandas as pd

file_path = "green_marble/green_marble/ingestion/data/extracted_data/indiamart_products_2025-08-07.csv"

df = pd.read_csv(file_path)
print("✅ Total products scraped:", len(df))
print("\n📌 Sample products:")
print(df.head())

from green_marble.ingestion.scripts.india_mart_parser import parse_html_file
import os

# ✅ Corrected path based on your folder structure
html_path = os.path.abspath(
    os.path.join("green_marble", "ingestion", "data", "raw", "india_mart", "2025-08-07", "raw_page_1.html")
)

print("Looking for file at:", html_path)

# Run the parser
parse_html_file(html_path)

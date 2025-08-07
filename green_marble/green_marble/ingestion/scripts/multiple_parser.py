###Downloads 150+ HTML pages for many keywords

import os
import time
import logging
import requests
from urllib.parse import quote
from datetime import datetime

# Setup logging
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'multi_scraper.log')
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# === CONFIG ===
KEYWORDS = [
    "floor tiles", "marble tiles", "wall tiles", "kitchen tiles",
    "bathroom tiles", "porcelain tiles", "ceramic tiles", "granite tiles",
    "wooden tiles", "white marble", "black granite", "tile adhesive",
    "mosaic tiles", "outdoor tiles", "parking tiles"
]
PAGES_PER_KEYWORD = 10
BASE_URL = "https://dir.indiamart.com/search.mp?ss={keyword}&page={page}"
TODAY = datetime.today().strftime('%Y-%m-%d')
RAW_HTML_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw_html', TODAY)
os.makedirs(RAW_HTML_DIR, exist_ok=True)

# === SCRAPE LOOP ===
def scrape_indiamart():
    total_pages = 0
    for keyword in KEYWORDS:
        logging.info(f"🔍 Scraping keyword: {keyword}")
        encoded_keyword = quote(keyword)
        for page in range(1, PAGES_PER_KEYWORD + 1):
            url = BASE_URL.format(keyword=encoded_keyword, page=page)
            logging.info(f"🌐 Fetching URL: {url}")
            try:
                response = requests.get(url, timeout=15)
                if response.status_code == 200:
                    filename = f"{keyword.replace(' ', '_')}_page_{page}.html"
                    filepath = os.path.join(RAW_HTML_DIR, filename)
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(response.text)
                    logging.info(f"✅ Saved {filename}")
                    total_pages += 1
                else:
                    logging.warning(f"❌ Failed {url} with status code {response.status_code}")
            except Exception as e:
                logging.error(f"❌ Exception while scraping {url}: {e}")
            time.sleep(1)  # Be kind to the server
    logging.info(f"🎉 Scraping finished. Total pages saved: {total_pages}")

if __name__ == "__main__":
    scrape_indiamart()

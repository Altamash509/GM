import os
import requests
import logging
import uuid
import time
import random
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

# ------------------ Logging Setup ------------------
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'scraper.log')

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ------------------ Configs ------------------
BASE_URL = "https://dir.indiamart.com/search.mp?ss={keyword}&cq=1&pg={page_num}"
KEYWORDS = ["floor tiles", "marble", "ceramic tiles", "granite", "bathroom tiles"]
PAGES_PER_KEYWORD = 2  # Increase gradually after testing

today_str = datetime.today().strftime("%Y-%m-%d")
raw_html_dir = f"green_marble/green_marble/ingestion/data/raw_html/{today_str}"
os.makedirs(raw_html_dir, exist_ok=True)

extracted_dir = "green_marble/green_marble/ingestion/data/extracted_data"
os.makedirs(extracted_dir, exist_ok=True)

# ------------------ Functions ------------------

def fetch_and_save_html(keyword, page_num):
    try:
        url = BASE_URL.format(keyword=keyword.replace(" ", "+"), page_num=page_num)
        logging.info(f"Fetching: {url}")
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()

        filename = f"{keyword.replace(' ', '_')}_page_{page_num}.html"
        filepath = os.path.join(raw_html_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(response.text)

        logging.info(f"Saved HTML: {filename}")
        return filepath
    except Exception as e:
        logging.error(f"Failed to fetch page {page_num} for {keyword}: {e}")
        return None

def parse_html_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'html.parser')

        product_blocks = soup.find_all("div", class_="prc_bncta db mt10 pl10 pr10")
        products = []

        for block in product_blocks:
            try:
                product_id = uuid.uuid4().hex
                name_tag = block.find("p", class_="prd_nam")
                price_tag = block.find("span", class_="prc_conv")
                unit_tag = block.find("span", class_="fs12 fw4")
                company_tag = block.find("a", class_="new_mobile_card_cname")
                location_tag = block.find("div", class_="countryNameList")
                rating_tag = block.find("div", class_="new_mobile_card_rating_container")
                trust_tags = block.find_all("span", class_="svgM")

                specs = block.find("ul", class_="fs12 isq mb7")
                specs_text = ", ".join(li.text.strip() for li in specs.find_all("li")) if specs else None

                product = {
                    "product_id": product_id,
                    "product_name": name_tag.text.strip() if name_tag else None,
                    "price": price_tag.text.strip() if price_tag else None,
                    "unit": unit_tag.text.strip() if unit_tag else None,
                    "specs": specs_text,
                    "company": company_tag.text.strip() if company_tag else None,
                    "location": location_tag.text.strip() if location_tag else None,
                    "rating_info": rating_tag.text.strip() if rating_tag else None,
                    "verified_tags": ", ".join(tag.text.strip() for tag in trust_tags) if trust_tags else None,
                    "scrape_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                products.append(product)
            except Exception as e:
                logging.warning(f"Error parsing product block: {e}")
        
        logging.info(f"✅ Parsed {len(products)} products from {os.path.basename(filepath)}")
        return products
    except Exception as e:
        logging.error(f"Error reading HTML file: {filepath} — {e}")
        return []

# ------------------ Main Script ------------------

if __name__ == "__main__":
    all_products = []

    for keyword in KEYWORDS:
        for page in range(1, PAGES_PER_KEYWORD + 1):
            html_file = fetch_and_save_html(keyword, page)
            if html_file:
                products = parse_html_file(html_file)
                all_products.extend(products)
            
            time.sleep(random.uniform(1, 3))  # Sleep to avoid rate limiting

    if all_products:
        df = pd.DataFrame(all_products)
        output_csv_path = os.path.join(extracted_dir, f"indiamart_products_{today_str}.csv")
        df.to_csv(output_csv_path, index=False)
        logging.info(f"🎉 Final CSV saved: {output_csv_path}")
        print(f"✅ Scraping complete. Total products: {len(df)}")
        print(f"📄 Output file: {output_csv_path}")
    else:
        print("⚠️ No products scraped.")

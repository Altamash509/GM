import os
import logging
import hashlib
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd

# ──────────────────────────────
# ✅ 1. Set up logging
# ──────────────────────────────
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, 'scraper.log')

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ──────────────────────────────
# ✅ 2. Parse HTML file
# ──────────────────────────────
def parse_html_file(file_path):
    logging.info(f"Started parsing file: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')

    product_blocks = soup.find_all("div", class_="prc_bncta db mt10 pl10 pr10")
    logging.info(f"Found {len(product_blocks)} product blocks.")

    extracted_data = []

    for block in product_blocks:
        try:
            # Product Name
            product_name_tag = block.find("p", class_="prd_nam")
            product_name = product_name_tag.get_text(strip=True) if product_name_tag else None

            # Price
            price_tag = block.find("p", class_="prc")
            price = price_tag.get_text(strip=True).replace('\n', '') if price_tag else None

            # Unit
            unit_tag = price_tag.find_next("span", class_="fs12 fw4") if price_tag else None
            unit = unit_tag.get_text(strip=True) if unit_tag else None

            # Seller Name
            seller_tag = block.find("a", class_="new_mobile_card_cname")
            seller_name = seller_tag.get_text(strip=True) if seller_tag else None

            # Location (best-effort: we extract from Export info line)
            location_tag = block.find("div", class_="countryNameList")
            location = location_tag.get_text(strip=True) if location_tag else None

            # Seller Type (guess based on badge or default)
            seller_type = "Verified Seller" if "Verified" in block.get_text() else "Unknown"

            # Contact URL
            contact_url = "https://www.indiamart.com" + seller_tag['href'] if seller_tag and seller_tag.has_attr('href') else None

            # Product URL
            product_link_tag = block.find("a")
            product_url = "https://www.indiamart.com" + product_link_tag['href'] if product_link_tag and product_link_tag.has_attr('href') else None

            # Image URL
            img_tag = block.find("img")
            image_url = img_tag['src'] if img_tag and img_tag.has_attr('src') else None

            # Rating (Not easily available — placeholder)
            rating = None

            # Number of Reviews (Not shown — placeholder)
            num_reviews = None

            # Category (Try to guess from keywords — fallback)
            category = "Tiles" if "tile" in product_name.lower() else "Other"

            # Availability (Not visible — placeholder)
            availability = None

            # Scrape Timestamp
            scrape_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Product ID (Hash from name + seller)
            id_source = f"{product_name}_{seller_name}"
            product_id = hashlib.md5(id_source.encode('utf-8')).hexdigest()

            # Collect Record
            extracted_data.append({
                "product_id": product_id,
                "product_name": product_name,
                "price": price,
                "category": category,
                "seller_name": seller_name,
                "location": location,
                "seller_type": seller_type,
                "contact_url": contact_url,
                "image_url": image_url,
                "product_url": product_url,
                "rating": rating,
                "num_reviews": num_reviews,
                "unit": unit,
                "availability": availability,
                "scrape_date": scrape_date
            })

        except Exception as e:
            logging.error(f"Error extracting block: {e}")

    logging.info(f"Successfully parsed {len(extracted_data)} products from {file_path}")

    # Save as CSV
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'extracted_data')
    os.makedirs(output_dir, exist_ok=True)

    date_str = datetime.now().strftime('%Y-%m-%d')
    output_path = os.path.join(output_dir, f'indiamart_products_{date_str}.csv')

    df = pd.DataFrame(extracted_data)
    df.to_csv(output_path, index=False)

    logging.info(f"Saved extracted data to: {output_path}")
    print(f"✅ Parsed and saved {len(df)} rows to {output_path}")

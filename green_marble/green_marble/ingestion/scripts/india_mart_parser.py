import os
import hashlib
import logging
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

# ✅ Setup Logging
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'parser.log')
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def generate_product_id(url):
    return hashlib.md5(url.encode()).hexdigest()

def parse_html_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f, 'html.parser')
    except Exception as e:
        logging.error(f"❌ Failed to read file {file_path}: {e}")
        return []

    product_blocks = soup.select('div.prc_bncta.db.mt10.pl10.pr10')
    logging.info(f"📄 File: {file_path} | Found {len(product_blocks)} product blocks.")

    records = []
    for block in product_blocks:
        try:
            name = block.find_previous('p', class_='prd_nam').get_text(strip=True)
            image_tag = block.find_previous('img')
            product_url = image_tag.get('onclick', '')
            image_url = image_tag.get('src', '')
            product_id = generate_product_id(product_url)

            price = block.find('span', class_='p_neg').get_text(strip=True) if block.find('span', class_='p_neg') else None
            seller = block.find('span', class_='bo').get_text(strip=True) if block.find('span', class_='bo') else None
            location = block.find('span', class_='smTxt').get_text(strip=True) if block.find('span', class_='smTxt') else None
            verified_tags = ', '.join([span.get_text(strip=True) for span in block.find_all('span', class_='veri_ic')])
            
            record = {
                'product_id': product_id,
                'product_name': name,
                'price': price,
                'seller_name': seller,
                'location': location,
                'image_url': image_url,
                'product_url': product_url,
                'verified_tags': verified_tags,
                'scrape_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            records.append(record)

        except Exception as e:
            logging.warning(f"⚠️ Error parsing a block: {e}")
            continue

    return records

# ✅ Bulk Runner
if __name__ == "__main__":
    raw_html_base = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw_html')
    extracted_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'extracted_data')
    os.makedirs(extracted_data_path, exist_ok=True)

    all_records = []

    for root, _, files in os.walk(raw_html_base):
        for file in files:
            if file.endswith('.html'):
                html_path = os.path.join(root, file)
                logging.info(f"🔍 Parsing: {html_path}")
                records = parse_html_file(html_path)
                all_records.extend(records)

    if all_records:
        df = pd.DataFrame(all_records)
        out_csv = os.path.join(extracted_data_path, f'indiamart_products_{datetime.now().strftime("%Y-%m-%d")}.csv')
        df.to_csv(out_csv, index=False)
        print(f"✅ Parsed and saved {len(df)} products to: {out_csv}")
    else:
        print("⚠️ No records parsed. Check HTML structure or raw files.")

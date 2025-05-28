import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timezone
import logging
import re
import pytz

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_data(max_pages=50):
    base_url = "https://fashion-studio.dicoding.dev"
    products = []
    wib_timezone = pytz.timezone("Asia/Jakarta")
    
    try:
        for page in range(1, max_pages + 1):
            try:
                url = base_url if page == 1 else f"{base_url}/page{page}"

                logging.info(f"Scraping halaman {page}: {url}")
                
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, "html.parser")
                product_cards = soup.find_all("div", class_="collection-card")
                
                if not product_cards:
                    logging.info(f"Tidak ada produk di halaman {page}")
                    continue
                
                page_timestamp = datetime.now(wib_timezone).isoformat()
                
                for card in product_cards:
                    try:
                        product_data = {
                            "Title": None,
                            "Price": None,
                            "Rating": None,
                            "Colors": None,
                            "Size": None,
                            "Gender": None,
                            "scrape_timestamp": page_timestamp
                        }
                        
                        # 1. Ekstrak Title
                        title_tag = card.find("h3", class_="product-title")
                        if title_tag:
                            product_data["Title"] = title_tag.get_text(strip=True)
                        else:
                            logging.warning("Title tidak ditemukan")
                            continue 
                            
                        # 2. Ekstrak Price
                        price_tag = card.find(["span", "p"], class_="price")
                        if price_tag:
                            product_data['Price'] = price_tag.get_text(strip=True).replace("$", "")
                        else:
                            logging.warning("Harga tidak ditemukan")
                            continue 
                        
                        # 3. Ekstrak Rating
                        rating_tag = card.find("p", string=lambda text: "Rating" in str(text))
                        if rating_tag:
                            product_data["Rating"] = rating_tag.get_text(strip=True)
                        else:
                            logging.warning("Rating tidak ditemukan")
                        
                        # 4. Ekstrak Colors
                        colors_tag = card.find("p", string=lambda text: "Colors" in str(text))
                        if colors_tag:
                            colors_text = colors_tag.get_text(strip=True)
                            colors_value = re.search(r"\d+", colors_text)
                            if colors_value:
                                product_data["Colors"] = int(colors_value.group())
                            else:
                                logging.warning("Colors tidak valid ditemukan")
                        else:
                            logging.warning("Colors tidak ditemukan")
                        
                        # 5. Ekstrak Size
                        size_tag = card.find("p", string=lambda text: "Size" in str(text))
                        if size_tag:
                            product_data["Size"] = size_tag.get_text(strip=True).split(":")[-1].strip()
                        else:
                            logging.warning("Size tidak ditemukan")
                        
                        # 6. Ekstrak Gender
                        gender_tag = card.find("p", string=lambda text: "Gender" in str(text))
                        if gender_tag:
                            product_data["Gender"] = gender_tag.get_text(strip=True).split(":")[-1].strip()
                        else:
                            logging.warning("Gender tidak ditemukan")
                        
                        # Simpan data
                        products.append(product_data)
                        
                    except Exception as e:
                        logging.error(f"Gagal parsing produk: {str(e)}", exc_info=True)
                        continue
                        
            except requests.RequestException as e:
                logging.error(f"Gagal mengambil halaman {page}: {str(e)}")
                continue

            # Logging jumlah input dari halaman
            logging.info(f"Jumlah produk di halaman {page}: {len(products)}")
                
    except Exception as e:
        logging.critical(f"Error kritis: {str(e)}")
        return pd.DataFrame()
    
    logging.info(f"Total data yang berhasil dikumpulkan: {len(products)}")

    return pd.DataFrame(products)
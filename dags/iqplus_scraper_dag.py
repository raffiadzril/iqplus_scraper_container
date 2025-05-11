from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
import re
import unicodedata
import logging
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException
from pymongo import MongoClient

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def retry_get(driver, url, retries=3, delay=2):
    for attempt in range(retries):
        try:
            driver.get(url)
            return
        except WebDriverException as e:
            logger.warning(f"⚠️ Gagal membuka {url}, percobaan ke-{attempt+1}: {e}")
            time.sleep(delay + random.random())
    raise Exception(f"Gagal membuka {url} setelah {retries} percobaan.")

def scrape_iqplus():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    
    # Di Docker, ChromeDriver sudah diinstall melalui apt-get
    driver = webdriver.Chrome(options=options)

    def simpan_ke_mongo(berita):
        # Menggunakan hostname host.docker.internal untuk mengakses MongoDB di Windows host
        client = MongoClient('mongodb://host.docker.internal:27017/')
        db = client['iqplus_db']
        collection = db['stock_news']

        if collection.find_one({'link': berita['link']}) is None:
            collection.insert_one(berita)
            logger.info(f"✅ Berita disimpan: {berita['title']}")
        else:
            logger.info(f"ℹ️ Duplikat, lewati: {berita['title']}")

    try:
        base_url = "http://www.iqplus.info/news/stock_news/go-to-page"
        retry_get(driver, f"{base_url},1.html")
        time.sleep(2)

        try:
            nav_element = driver.find_element(By.CLASS_NAME, "nav")
            page_links = nav_element.find_elements(By.TAG_NAME, "a")
            max_pages = max([
                int(re.search(r'go-to-page,(\d+).html', a.get_attribute("href")).group(1))
                for a in page_links if re.search(r'go-to-page,(\d+).html', a.get_attribute("href"))
            ])
        except:
            max_pages = 5

        page = 1
        while page <= max_pages:
            retry_get(driver, f"{base_url},{page}.html")
            time.sleep(2)

            try:
                berita_section = driver.find_element(By.ID, "load_news")
                berita_list = berita_section.find_element(By.CLASS_NAME, "news")
                berita_items = berita_list.find_elements(By.TAG_NAME, "li")
            except:
                logger.warning(f"⚠️ Gagal mengambil berita di halaman {page}")
                break

            if len(berita_items) == 0:
                break

            for i in range(len(berita_items)):
                try:
                    berita_section = driver.find_element(By.ID, "load_news")
                    berita_list = berita_section.find_element(By.CLASS_NAME, "news")
                    berita_items = berita_list.find_elements(By.TAG_NAME, "li")

                    if i >= len(berita_items):
                        break

                    berita = berita_items[i]
                    link_element = berita.find_element(By.TAG_NAME, "a")
                    public_date = berita.find_element(By.TAG_NAME, "b").text.strip()
                    judul = link_element.text.strip()
                    link = link_element.get_attribute("href")

                    retry_get(driver, link)
                    time.sleep(1)

                    try:
                        content_element = driver.find_element(By.ID, "zoomthis")
                        konten = content_element.text.strip()
                        # Bersihkan karakter non-UTF8
                        konten = re.sub(r'[^\x00-\x7F]+', ' ', konten)
                    except:
                        konten = "Konten tidak ditemukan"

                    berita_baru = {
                        "title": judul,
                        "link": link,
                        "published_at": public_date,
                        "content": konten
                    }

                    simpan_ke_mongo(berita_baru)

                    retry_get(driver, f"{base_url},{page}.html")
                    time.sleep(1)
                except Exception as e:
                    logger.warning(f"⚠️ Error di berita {i+1} halaman {page}: {e}")

            page += 1
    finally:
        driver.quit()

# DAG config
default_args = {
    'start_date': datetime(2025, 5, 10),
}

with DAG('iqplus_scraper_dag',
         schedule_interval='@daily',
         catchup=False,
         default_args=default_args,
         description='Scraping berita saham IQPlus ke MongoDB',
         tags=['scraping', 'iqplus', 'mongodb']
         ) as dag:

    scraping_task = PythonOperator(
        task_id='scrape_iqplus_to_mongo',
        python_callable=scrape_iqplus
    )

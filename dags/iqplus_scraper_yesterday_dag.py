from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
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
    """Fungsi untuk mencoba membuka URL dengan retry jika gagal"""
    for attempt in range(retries):
        try:
            driver.get(url)
            return
        except WebDriverException as e:
            logger.warning(f"⚠️ Gagal membuka {url}, percobaan ke-{attempt+1}: {e}")
            time.sleep(delay + random.random())
    raise Exception(f"Gagal membuka {url} setelah {retries} percobaan.")

def is_dari_kemarin(tanggal_str):
    """Memeriksa apakah tanggal publikasi berita adalah dari kemarin"""
    try:
        # Format tanggal dari IQPlus biasanya: "DD/MM/YYYY HH:MM:SS"
        tanggal_berita = datetime.strptime(tanggal_str.strip(), "%d/%m/%Y %H:%M:%S")
        kemarin = datetime.now() - timedelta(days=1)
        return tanggal_berita.date() == kemarin.date()
    except Exception as e:
        logger.warning(f"⚠️ Error saat parsing tanggal {tanggal_str}: {e}")
        return False

def inisialisasi_webdriver():
    """Inisialisasi WebDriver untuk scraping"""
    logger.info("🚀 Memulai inisialisasi WebDriver...")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    
    # Di Docker, ChromeDriver sudah diinstall melalui apt-get
    driver = webdriver.Chrome(options=options)
    logger.info("✅ WebDriver berhasil diinisialisasi")
    return driver

def scrape_yesterday_news(**kwargs):
    """Scrape berita dari kemarin dan berhenti segera ketika menemukan berita yang bukan dari kemarin"""
    logger.info("🔍 Memulai scraping berita dari kemarin...")
    driver = inisialisasi_webdriver()
    berita_kemarin = []
    base_url = "http://www.iqplus.info/news/stock_news/go-to-page"
    
    try:
        page = 1
        berita_kemarin_ditemukan = False  # Flag untuk menandai apakah sudah menemukan berita kemarin
        masih_ada_berita_kemarin = True   # Flag untuk melanjutkan pencarian berita kemarin
        
        while masih_ada_berita_kemarin:
            logger.info(f"📃 Memeriksa halaman {page}...")
            retry_get(driver, f"{base_url},{page}.html")
            time.sleep(2)
            
            try:
                # Cek apakah ada berita di halaman ini
                berita_section = driver.find_element(By.ID, "load_news")
                berita_list_element = berita_section.find_element(By.CLASS_NAME, "news")
                berita_items = berita_list_element.find_elements(By.TAG_NAME, "li")
                
                if len(berita_items) == 0:
                    logger.info(f"🛑 Tidak ada berita di halaman {page}, menghentikan pencarian")
                    break
                
                berita_kemarin_di_halaman_ini = False
                
                for i in range(len(berita_items)):
                    try:
                        # Re-find elements untuk menghindari StaleElementReferenceException
                        berita_section = driver.find_element(By.ID, "load_news")
                        berita_list_element = berita_section.find_element(By.CLASS_NAME, "news")
                        berita_items = berita_list_element.find_elements(By.TAG_NAME, "li")
                        
                        if i >= len(berita_items):
                            break
                        
                        berita = berita_items[i]
                        link_element = berita.find_element(By.TAG_NAME, "a")
                        public_date = berita.find_element(By.TAG_NAME, "b").text.strip()
                          # Cek apakah berita ini dari kemarin
                        if is_dari_kemarin(public_date):
                            berita_kemarin_ditemukan = True
                            berita_kemarin_di_halaman_ini = True
                            
                            judul = link_element.text.strip()
                            link = link_element.get_attribute("href")
                            
                            # Buka halaman berita untuk mengambil kontennya
                            retry_get(driver, link)
                            time.sleep(1)
                            
                            try:
                                content_element = driver.find_element(By.ID, "zoomthis")
                                konten = content_element.text.strip()
                                # Bersihkan karakter non-UTF8
                                konten = re.sub(r'[^\x00-\x7F]+', ' ', konten)
                            except Exception as e:
                                konten = "Konten tidak ditemukan"
                                logger.warning(f"⚠️ Gagal mengambil konten: {e}")
                            
                            # Tambahkan berita ke daftar
                            berita_baru = {
                                "title": judul,
                                "link": link,
                                "published_at": public_date,
                                "content": konten,
                                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            
                            berita_kemarin.append(berita_baru)
                            logger.info(f"📰 Berhasil scrape berita: {judul}")
                            
                            # Kembali ke halaman daftar
                            retry_get(driver, f"{base_url},{page}.html")
                            time.sleep(1)
                        else:
                            logger.info(f"🛑 Menemukan berita bukan dari kemarin: {public_date}")
                            # Segera hentikan pencarian ketika menemukan berita yang bukan dari kemarin
                            masih_ada_berita_kemarin = False
                            logger.info(f"🏁 Pencarian dihentikan karena menemukan berita yang bukan dari kemarin")
                            break
                            
                    except Exception as e:
                        logger.warning(f"⚠️ Error di berita {i+1} halaman {page}: {e}")
                  # Pemeriksaan ini tidak perlu lagi karena kita segera berhenti saat menemukan berita non-kemarin
                # Tetap mempertahankan pengecekan ini sebagai fallback
                if berita_kemarin_ditemukan and not berita_kemarin_di_halaman_ini:
                    masih_ada_berita_kemarin = False
                    logger.info("🏁 Semua berita kemarin sepertinya sudah terkumpul")
                    break
                
                page += 1
                
            except Exception as e:
                logger.warning(f"⚠️ Gagal memproses halaman {page}: {e}")
                break
    
    finally:
        driver.quit()
    
    # Simpan hasil scraping ke XCom
    kwargs['ti'].xcom_push(key='berita_kemarin', value=berita_kemarin)
    logger.info(f"✅ Selesai scrape, mendapatkan total {len(berita_kemarin)} berita dari kemarin")
    
    return berita_kemarin

def simpan_ke_mongodb(**kwargs):
    """Menyimpan hasil scraping ke MongoDB"""
    logger.info("💾 Memulai penyimpanan data ke MongoDB...")
    ti = kwargs['ti']
    
    # Ambil hasil scraping dari XCom
    berita_list = ti.xcom_pull(key='berita_kemarin')
    
    if not berita_list:
        logger.warning("⚠️ Tidak ada berita yang bisa disimpan ke MongoDB")
        return {"berita_baru": 0, "berita_duplikat": 0}
    
    logger.info(f"📊 Total berita yang akan disimpan: {len(berita_list)}")
    
    # Simpan ke MongoDB
    client = MongoClient('mongodb://host.docker.internal:27017/')
    db = client['iqplus_db-scraping']
    collection = db['stock_news']
    
    berita_baru = 0
    berita_duplikat = 0
    
    for berita in berita_list:
        if collection.find_one({'link': berita['link']}) is None:
            collection.insert_one(berita)
            berita_baru += 1
            logger.info(f"✅ Berita disimpan: {berita['title']}")
        else:
            berita_duplikat += 1
            logger.info(f"ℹ️ Duplikat, lewati: {berita['title']}")
    
    logger.info(f"🏁 Penyimpanan selesai! Berita baru: {berita_baru}, Duplikat: {berita_duplikat}")
    return {"berita_baru": berita_baru, "berita_duplikat": berita_duplikat}

def generate_report(**kwargs):
    """Membuat laporan hasil scraping"""
    ti = kwargs['ti']
    hasil_simpan = ti.xcom_pull(task_ids='save_to_mongodb')
    
    if not hasil_simpan:
        logger.warning("⚠️ Tidak bisa mendapatkan hasil penyimpanan")
        return
    
    berita_baru = hasil_simpan.get("berita_baru", 0)
    berita_duplikat = hasil_simpan.get("berita_duplikat", 0)
    
    logger.info("📊 Laporan Hasil Scraping IQPlus:")
    logger.info(f"📅 Tanggal Scraping: {datetime.now().strftime('%Y-%m-%d')}")
    logger.info(f"🕖 Waktu Scraping: {datetime.now().strftime('%H:%M:%S')}")
    logger.info(f"📰 Total Berita Kemarin yang Ditemukan: {berita_baru + berita_duplikat}")
    logger.info(f"✨ Berita Baru yang Disimpan: {berita_baru}")
    logger.info(f"🔄 Berita Duplikat yang Dilewati: {berita_duplikat}")
    
    return {
        "tanggal_scraping": datetime.now().strftime('%Y-%m-%d'),
        "waktu_scraping": datetime.now().strftime('%H:%M:%S'),
        "total_berita_kemarin": berita_baru + berita_duplikat,
        "berita_baru": berita_baru,
        "berita_duplikat": berita_duplikat
    }

# DAG config
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 10),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG('iqplus_scraper_yesterday_dag',
         schedule_interval='0 7 * * *',  # Berjalan setiap hari pada pukul 7 pagi
         catchup=False,
         default_args=default_args,
         description='Scraping berita saham IQPlus dari kemarin ke MongoDB',
         tags=['scraping', 'iqplus', 'mongodb']
         ) as dag:

    # Task 1: Scrape berita kemarin dari semua halaman yang diperlukan
    scraping_task = PythonOperator(
        task_id='scrape_yesterday_news',
        python_callable=scrape_yesterday_news,
        provide_context=True,
    )
    
    # Task 2: Simpan hasil scraping ke MongoDB
    save_task = PythonOperator(
        task_id='save_to_mongodb',
        python_callable=simpan_ke_mongodb,
        provide_context=True,
    )
    
    # Task 3: Generate report hasil scraping
    report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True,
    )
    
    # Alur eksekusi task
    scraping_task >> save_task >> report_task

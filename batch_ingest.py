# batch_ingest.py (Final, Robust Version)

import os
import requests
import pandas as pd
from tqdm import tqdm
from sqlalchemy.orm import sessionmaker
from ingest_data import process_nc_file, DATABASE_URL, create_engine
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
INDEX_FILE_URL = "https://data-argo.ifremer.fr/ar_index_global_prof.txt"
DOWNLOAD_DIR = "data"
FILE_LIMIT = 1000
MAX_WORKERS = 5

DB_USER = 'postgres'
DB_PASSWORD = '123654' # Your password
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'argo_db'
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

MIN_LAT, MAX_LAT = -30, 30
MIN_LON, MAX_LON = 30, 110

def download_file(url, local_path):
    """Downloads a single file via HTTP with a timeout."""
    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to download {url}: {e}")
        return False

def download_and_process(row, db_url):
    """A self-contained function for a single thread."""
    file_path = row['file']
    file_url = f"https://data-argo.ifremer.fr/{file_path}"
    local_nc_path = os.path.join(DOWNLOAD_DIR, f"{os.path.basename(file_path)}_{os.getpid()}")

    if download_file(file_url, local_nc_path):
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        with Session() as session:
            process_nc_file(local_nc_path, session)
        
        try:
            os.remove(local_nc_path)
        except OSError:
            pass
        return file_path
    return None

def main():
    print("🚀 Starting Multithreaded Batch Ingestion...")
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    index_local_path = os.path.join(DOWNLOAD_DIR, "ar_index_global_prof.txt")

    if not os.path.exists(index_local_path):
        print("   - Downloading master index file...")
        if not download_file(INDEX_FILE_URL, index_local_path):
            print("   - CRITICAL: Could not download index file. Exiting.")
            return
    else:
        print("   - Master index file already exists.")

    print("   - Parsing index file...")
    column_names = ['file', 'date', 'latitude', 'longitude', 'ocean', 'profiler_type', 'institution', 'date_update']
    
    # ✅ PARSE 'date_update' for correct chronological sorting
    df = pd.read_csv(index_local_path, comment='#', header=None,
                     names=column_names,
                     parse_dates=['date_update'],
                     low_memory=False)

    print("   - Cleaning and validating data types...")
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    
    # ✅ ADDED critical step to remove rows with invalid locations
    original_rows = len(df)
    df.dropna(subset=['latitude', 'longitude'], inplace=True)
    cleaned_rows = len(df)
    print(f"   - Removed {original_rows - cleaned_rows} rows with invalid location data.")

    print(f"   - Filtering for region...")
    region_df = df[(df['latitude'] >= MIN_LAT) & (df['latitude'] <= MAX_LAT) & (df['longitude'] >= MIN_LON) & (df['longitude'] <= MAX_LON)].copy()
    
    # This sort now works correctly because 'date_update' is a proper date object
    region_df.sort_values(by='date_update', ascending=False, inplace=True)
    limited_df = region_df.head(FILE_LIMIT)
    
    print(f"   - Found {len(limited_df)} files to process with {MAX_WORKERS} parallel workers.")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = { executor.submit(download_and_process, row, DATABASE_URL): row['file'] for _, row in limited_df.iterrows() }
        success_count = 0
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing files"):
            if future.result() is not None:
                success_count += 1

    print(f"\n✅ Batch Ingestion Complete! Successfully processed {success_count}/{len(limited_df)} files.")

if __name__ == "__main__":
    main()
import os
import requests
import pandas as pd
from tqdm.notebook import tqdm  # Use notebook-friendly tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import zipfile

# --- Mount Google Drive ---
from google.colab import drive
drive.mount('/content/drive')

# --- Configuration ---
INDEX_FILE_URL = "https://data-argo.ifremer.fr/ar_index_global_prof.txt"
DOWNLOAD_DIR = "/content/argo_data" # A temporary directory inside Colab
FILE_LIMIT = 1000
MAX_WORKERS = 20 # We can use more workers in the cloud

# Bounding box for the Indian Ocean
MIN_LAT, MAX_LAT = -30, 30
MIN_LON, MAX_LON = 30, 110

def download_file(url, local_path):
    """Downloads a single file via HTTP."""
    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            return url
    except requests.exceptions.RequestException:
        return None

# --- Main Script ---
print("🚀 Starting Cloud Download Process...")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
index_local_path = "/content/ar_index_global_prof.txt"

print("   - Downloading master index file...")
download_file(INDEX_FILE_URL, index_local_path)

print("   - Parsing and filtering index file...")
df = pd.read_csv(index_local_path, comment='#', header=None, parse_dates=[8], names=[
    'file', 'date', 'latitude', 'longitude', 'ocean', 'profiler_type', 'institution', 'date_update'
], low_memory=False)

df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
df.dropna(subset=['latitude', 'longitude'], inplace=True)

region_df = df[(df['latitude'] >= MIN_LAT) & (df['latitude'] <= MAX_LAT) & (df['longitude'] >= MIN_LON) & (df['longitude'] <= MAX_LON)].copy()
region_df.sort_values(by='date_update', ascending=False, inplace=True)
limited_df = region_df.head(FILE_LIMIT)
print(f"   - Found {len(limited_df)} files to download.")

# --- Parallel Download ---
urls_to_download = [f"https://data-argo.ifremer.fr/{row['file']}" for _, row in limited_df.iterrows()]
local_paths = [os.path.join(DOWNLOAD_DIR, os.path.basename(url)) for url in urls_to_download]

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(download_file, url, path) for url, path in zip(urls_to_download, local_paths)]
    success_count = 0
    for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading files"):
        if future.result() is not None:
            success_count += 1
print(f"   - Successfully downloaded {success_count}/{len(futures)} files.")


# --- Zip the results and save to Google Drive ---
print("\n   - Zipping files...")
zip_path = "/content/drive/MyDrive/argo_data.zip"
with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for root, _, files in os.walk(DOWNLOAD_DIR):
        for file in tqdm(files, desc="Zipping"):
            zipf.write(os.path.join(root, file), arcname=file)

print(f"\n✅ DONE! All files are zipped and saved to your Google Drive as 'argo_data.zip'")
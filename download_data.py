import os
import requests
from tqdm import tqdm

# --- Configuration ---
# This is the direct HTTP URL to the file
FILE_URL = "https://data-argo.ifremer.fr/dac/incois/1900121/1900121_prof.nc"
DOWNLOAD_DIR = "data"
# Extract the filename from the URL
FILENAME = FILE_URL.split('/')[-1]
LOCAL_FILEPATH = os.path.join(DOWNLOAD_DIR, FILENAME)

# --- Main Script ---
print("🚀 Starting HTTP download...")

if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

try:
    # Use requests to get the file, stream=True allows downloading large files
    response = requests.get(FILE_URL, stream=True)
    # Raise an error if the download failed (e.g., 404 Not Found)
    response.raise_for_status() 

    # Get the total file size from the headers
    total_size = int(response.headers.get('content-length', 0))

    print(f"Downloading {FILENAME} ({total_size / 1_000_000:.2f} MB)...")

    # Open the local file in binary write mode and create a tqdm progress bar
    with open(LOCAL_FILEPATH, 'wb') as f, tqdm(
        total=total_size,
        unit='B',
        unit_scale=True,
        unit_divisor=1024,
        desc=FILENAME
    ) as pbar:
        # Download the file in chunks and update the progress bar
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            pbar.update(len(chunk))

    print(f"✅ Finished downloading {FILENAME}")

except requests.exceptions.RequestException as e:
    print(f"❌ An error occurred: {e}")

print("Download script finished.")
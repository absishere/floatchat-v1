# ingest_data.py

import os
import glob
import xarray as xr
import pandas as pd
from sqlalchemy import create_engine, text # <-- IMPORT 'text' HERE
from sqlalchemy.orm import sessionmaker

# --- Database Configuration ---
DB_USER = 'postgres'
DB_PASSWORD = '123456' # <-- IMPORTANT: DOUBLE-CHECK THIS
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'argo_db'

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Setup Database Connection ---
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# --- Main Ingestion Logic ---
def process_nc_file(filepath, session):
    """Reads a single NetCDF file and populates the database."""
    print(f"Processing {filepath}...")
    try:
        with xr.open_dataset(filepath, decode_times=True) as ds:
            wmo_id = ds.attrs.get('platform_number', os.path.basename(filepath).split('_')[0]).strip()
            
            # 1. Get or create the float record
            # Use text() to wrap the raw SQL string
            select_float_sql = text("SELECT float_id FROM floats WHERE wmo_id = :wmo_id")
            float_obj = session.execute(select_float_sql, {'wmo_id': wmo_id}).fetchone()

            if not float_obj:
                insert_float_sql = text("INSERT INTO floats (wmo_id) VALUES (:wmo_id) RETURNING float_id")
                result = session.execute(insert_float_sql, {'wmo_id': wmo_id})
                float_id = result.fetchone()[0]
            else:
                float_id = float_obj[0]
            
            # 2. Loop through each profile
            num_profiles = ds.dims['N_PROF']
            for i in range(num_profiles):
                profile_data = ds.isel(N_PROF=i)
                cycle_num = int(profile_data['CYCLE_NUMBER'].item())
                
                # Check if this profile already exists
                select_profile_sql = text("SELECT profile_id FROM profiles WHERE float_id = :fid AND cycle_number = :cn")
                profile_exists = session.execute(select_profile_sql, {'fid': float_id, 'cn': cycle_num}).fetchone()

                if profile_exists:
                    print(f"  Skipping profile {cycle_num} for float {wmo_id} (already exists).")
                    continue

                # Insert the new profile record
                profile_sql = text("""
                INSERT INTO profiles (float_id, cycle_number, profile_date, latitude, longitude)
                VALUES (:fid, :cn, :p_date, :lat, :lon)
                RETURNING profile_id;
                """)
                profile_params = {
                    'fid': float_id,
                    'cn': cycle_num,
                    'p_date': pd.to_datetime(profile_data['JULD'].values).to_pydatetime(),
                    'lat': profile_data['LATITUDE'].item(),
                    'lon': profile_data['LONGITUDE'].item()
                }
                # Insert new profile record
                profile_id = session.execute(profile_sql, profile_params).fetchone()[0]
                session.flush()  # send it to DB immediately

                # 3. Create a DataFrame for all measurements
                measurements_df = pd.DataFrame({
                    'profile_id': profile_id,
                    'pressure': profile_data['PRES'].values,
                    'temperature': profile_data['TEMP'].values,
                    'salinity': profile_data['PSAL'].values,
                })
                measurements_df.dropna(subset=['pressure', 'temperature', 'salinity'], how='all', inplace=True)

                # 4. Bulk insert measurements
                if not measurements_df.empty:
                    conn = session.connection()
                    measurements_df.to_sql('measurements', conn, if_exists='append', index=False)

                print(f"  Successfully ingested profile {cycle_num} for float {wmo_id}.")

            
            session.commit() # Commit all changes for this file at once

    except Exception as e:
        session.rollback()
        print(f"ERROR processing {filepath}: {e}")

if __name__ == "__main__":
    data_dir = "data"
    nc_files = glob.glob(os.path.join(data_dir, '*.nc'))
    
    if not nc_files:
        print("No NetCDF files found in the 'data' directory.")
    else:
        with Session() as session:
            for f in nc_files:
                process_nc_file(f, session)
        print("\nData ingestion complete.")
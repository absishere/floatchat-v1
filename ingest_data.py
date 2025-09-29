# ingest_data.py (Final Version with Progress Bar)

import os
import glob
import xarray as xr
import pandas as pd
from tqdm import tqdm  # Import tqdm for the progress bar
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# --- Database Configuration ---
DB_USER = 'postgres'
DB_PASSWORD = '123654'  # Your password
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'argo_db'
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def process_nc_file(filepath, session):
    """Reads a single NetCDF file and populates the database."""
    try:
        with xr.open_dataset(filepath, decode_times=True) as ds:
            wmo_id = ds.attrs.get('platform_number', os.path.basename(filepath).split('_')[0]).strip()
            
            select_float_sql = text("SELECT float_id FROM floats WHERE wmo_id = :wmo_id")
            float_obj = session.execute(select_float_sql, {'wmo_id': wmo_id}).fetchone()

            if not float_obj:
                insert_float_sql = text("INSERT INTO floats (wmo_id) VALUES (:wmo_id) RETURNING float_id")
                result = session.execute(insert_float_sql, {'wmo_id': wmo_id})
                float_id = result.fetchone()[0]
            else:
                float_id = float_obj[0]
            
            num_profiles = ds.dims['N_PROF']
            for i in range(num_profiles):
                profile_data = ds.isel(N_PROF=i)
                cycle_num = int(profile_data['CYCLE_NUMBER'].item())
                
                select_profile_sql = text("SELECT profile_id FROM profiles WHERE float_id = :fid AND cycle_number = :cn")
                if session.execute(select_profile_sql, {'fid': float_id, 'cn': cycle_num}).fetchone():
                    continue

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
                profile_id = session.execute(profile_sql, profile_params).fetchone()[0]
                
                measurements_df = pd.DataFrame({
                    'profile_id': profile_id,
                    'pressure': profile_data['PRES'].values,
                    'temperature': profile_data['TEMP'].values,
                    'salinity': profile_data['PSAL'].values,
                })
                measurements_df.dropna(how='all', subset=['pressure', 'temperature', 'salinity'], inplace=True)
                
                if not measurements_df.empty:
                    measurements_df.to_sql('measurements', session.get_bind(), if_exists='append', index=False)
            
            session.commit()
    except Exception:
        session.rollback()
        # Suppress individual file errors to allow the batch to continue
        pass

if __name__ == "__main__":
    print("🚀 Starting local file ingestion...")
    data_dir = "data"
    nc_files = glob.glob(os.path.join(data_dir, '*.nc'))
    
    if not nc_files:
        print("❌ No NetCDF files found in the 'data' directory.")
    else:
        print(f"Found {len(nc_files)} files to ingest into the database.")
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        
        # Use tqdm for a progress bar
        for f in tqdm(nc_files, desc="Ingesting files"):
            with Session() as session:
                process_nc_file(f, session)
        
        print("\n✅ Data ingestion complete!")
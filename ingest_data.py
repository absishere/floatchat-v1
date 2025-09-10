import os
import glob
import xarray as xr
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# --- Database Configuration ---
DB_USER = 'postgres'
DB_PASSWORD = 'your_postgres_password' # <-- IMPORTANT: CHANGE THIS
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
        with xr.open_dataset(filepath) as ds:
            # 1. Get or create the float record
            # The WMO ID is usually in the filename or attributes. Here we parse from filename.
            wmo_id = os.path.basename(filepath).split('_')[0]
            
            # Check if float exists
            float_obj = session.execute(f"SELECT float_id FROM floats WHERE wmo_id = '{wmo_id}'").fetchone()
            if not float_obj:
                # Insert if not exists and get the new float_id
                result = session.execute(f"INSERT INTO floats (wmo_id) VALUES ('{wmo_id}') RETURNING float_id;")
                float_id = result.fetchone()[0]
                session.commit()
            else:
                float_id = float_obj[0]
            
            # 2. Loop through each profile in the file
            num_profiles = ds.dims['N_PROF']
            for i in range(num_profiles):
                # Extract profile metadata
                profile_data = ds.isel(N_PROF=i)
                cycle_num = int(profile_data['CYCLE_NUMBER'].item())
                
                # Check if this profile already exists to avoid duplicates
                profile_exists = session.execute(f"SELECT profile_id FROM profiles WHERE float_id = {float_id} AND cycle_number = {cycle_num}").fetchone()
                if profile_exists:
                    print(f"  Skipping profile {cycle_num} for float {wmo_id} (already exists).")
                    continue

                # Insert the new profile record
                profile_sql = f"""
                INSERT INTO profiles (float_id, cycle_number, profile_date, latitude, longitude)
                VALUES ({float_id}, {cycle_num}, '{profile_data['JULD'].dt.strftime('%Y-%m-%d %H:%M:%S %Z').item()}', {profile_data['LATITUDE'].item()}, {profile_data['LONGITUDE'].item()})
                RETURNING profile_id;
                """
                profile_id = session.execute(profile_sql).fetchone()[0]
                
                # 3. Create a DataFrame for all measurements in this profile
                measurements_df = pd.DataFrame({
                    'profile_id': profile_id,
                    'pressure': profile_data['PRES'].values,
                    'temperature': profile_data['TEMP'].values,
                    'salinity': profile_data['PSAL'].values,
                })
                
                # Drop rows with no valid measurements
                measurements_df.dropna(subset=['pressure', 'temperature', 'salinity'], how='all', inplace=True)
                
                # 4. Bulk insert measurements into the database
                if not measurements_df.empty:
                    measurements_df.to_sql('measurements', engine, if_exists='append', index=False)
                
                session.commit()
                print(f"  Successfully ingested profile {cycle_num} for float {wmo_id}.")

    except Exception as e:
        session.rollback()
        print(f"ERROR processing {filepath}: {e}")

if __name__ == "__main__":
    data_dir = "data"
    nc_files = glob.glob(os.path.join(data_dir, '*.nc'))
    
    if not nc_files:
        print("No NetCDF files found in the 'data' directory. Run download_data.py first.")
    else:
        with Session() as session:
            for f in nc_files:
                process_nc_file(f, session)
        print("\nData ingestion complete.")
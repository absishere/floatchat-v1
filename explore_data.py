import xarray as xr

# Open the downloaded NetCDF file
file_path = 'data/1900121_prof.nc'
ds = xr.open_dataset(file_path)

# Print the dataset summary
print("--- Dataset Summary ---")
print(ds)

# Print specific variables to see their structure
print("\n--- Salinity Data (first 5 profiles, first 10 depth levels) ---")
print(ds['PSAL'].isel(N_PROF=slice(0, 5), N_LEVELS=slice(0, 10)))

# Let's see the latitude and longitude for the first 5 profiles
print("\n--- Location Data (first 5 profiles) ---")
print(ds['LATITUDE'].isel(N_PROF=slice(0, 5)))
print(ds['LONGITUDE'].isel(N_PROF=slice(0, 5)))
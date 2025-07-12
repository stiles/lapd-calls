import requests
import pandas as pd
import sqlite3
from datetime import datetime
import os
from tqdm import tqdm
import time

def get_lapd_datasets():
    """Fetch all LAPD calls for service datasets from the catalog"""
    print("Fetching LAPD datasets from catalog...")
    
    url = "https://api.us.socrata.com/api/catalog/v1"
    params = {
        "domains": "data.lacity.org",
        "q": "LAPD calls for service"
    }
    
    res = requests.get(url, params=params)
    res.raise_for_status()
    results = res.json()['results']
    
    # Filter for main calls for service datasets (exclude summary/count datasets)
    datasets = []
    for r in results:
        name = r["resource"]["name"]
        # Skip summary datasets and focus on main calls for service data
        if ("calls for service" in name.lower() and 
            "count" not in name.lower() and 
            "summary" not in name.lower() and
            "excluding ambulance" not in name.lower()):
            datasets.append({
                "name": name,
                "endpoint": r["resource"]["id"],
                "year": extract_year_from_name(name)
            })
    
    # Sort by year
    datasets.sort(key=lambda x: x['year'] if x['year'] else 0)
    return datasets

def extract_year_from_name(name):
    """Extract year from dataset name"""
    import re
    # Look for 4-digit year
    match = re.search(r'20\d{2}', name)
    if match:
        return int(match.group())
    elif "2024 to Present" in name:
        return 2024
    return None

def fetch_dataset(endpoint, dataset_name, batch_size=50000):
    """Fetch all data from a single dataset with pagination"""
    print(f"Fetching data from {dataset_name}...")
    
    base_url = f"https://data.lacity.org/resource/{endpoint}.json"
    all_data = []
    offset = 0
    
    while True:
        params = {
            "$limit": batch_size,
            "$offset": offset
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            batch_data = response.json()
            
            if not batch_data:
                break
                
            all_data.extend(batch_data)
            offset += batch_size
            
            print(f"  Fetched {len(all_data)} records so far...")
            
            # Small delay to be respectful to the API
            time.sleep(0.1)
            
        except requests.exceptions.RequestException as e:
            print(f"  Error fetching batch at offset {offset}: {e}")
            break
    
    print(f"  Total records fetched: {len(all_data)}")
    return all_data

def clean_and_process_data(df):
    """Clean and standardize the data"""
    print("Cleaning and processing data...")
    
    # Make a copy to avoid modifying original
    df = df.copy()
    
    # Standardize column names (some datasets might have slight variations)
    column_mapping = {
        'date_rptd': 'report_date',
        'date_occ': 'occurrence_date', 
        'time_occ': 'occurrence_time',
        'dispatch_date': 'dispatch_date',
        'dispatch_time': 'dispatch_time',
        'call_type_text': 'call_type',
        'call_type_description': 'call_type',
    }
    # If both call_type_text and call_type_description exist, prefer call_type_text
    if 'call_type_text' in df.columns:
        df['call_type'] = df['call_type_text']
    elif 'call_type_description' in df.columns:
        df['call_type'] = df['call_type_description']
    # Repeat for other fields as needed
    for old, new in column_mapping.items():
        if old in df.columns and old not in ['call_type_text', 'call_type_description']:
            df.rename(columns={old: new}, inplace=True)
    
    # Process dates - try different date columns that might exist
    date_columns = ['report_date', 'occurrence_date', 'dispatch_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Create a primary date column (prefer occurrence_date, then report_date, then dispatch_date)
    if 'occurrence_date' in df.columns:
        df['primary_date'] = df['occurrence_date']
    elif 'report_date' in df.columns:
        df['primary_date'] = df['report_date']
    elif 'dispatch_date' in df.columns:
        df['primary_date'] = df['dispatch_date']
    else:
        df['primary_date'] = pd.NaT
    
    # Add year column
    df['year'] = df['primary_date'].dt.year
    
    # Add month and day columns for easier analysis
    df['month'] = df['primary_date'].dt.month
    df['day_of_week'] = df['primary_date'].dt.day_name()
    df['hour'] = pd.NaT
    
    # Process time columns
    time_columns = ['occurrence_time', 'dispatch_time']
    for col in time_columns:
        if col in df.columns:
            try:
                df[f'{col}_hour'] = pd.to_datetime(df[col], format='%H:%M:%S', errors='coerce').dt.hour
                if 'hour' not in df.columns or df['hour'].isna().all():
                    df['hour'] = df[f'{col}_hour']
            except:
                pass
    
    # Clean up call type text
    if 'call_type' in df.columns:
        df['call_type'] = df['call_type'].str.strip()
    
    # Clean up area names
    if 'area_occ' in df.columns:
        df['area_occ'] = df['area_occ'].str.strip()
    
    # Remove rows with no valid date
    initial_count = len(df)
    df = df.dropna(subset=['primary_date'])
    print(f"  Removed {initial_count - len(df)} records with invalid dates")
    
    return df

def main():
    print("LAPD Calls for Service Data Processor")
    print("=" * 50)
    
    # Get all datasets
    datasets = get_lapd_datasets()
    print(f"\nFound {len(datasets)} datasets:")
    for ds in datasets:
        print(f"  - {ds['name']} ({ds['year']})")
    
    # Fetch all data
    all_dataframes = []
    
    for dataset in tqdm(datasets, desc="Processing datasets"):
        try:
            data = fetch_dataset(dataset['endpoint'], dataset['name'])
            if data:
                df = pd.DataFrame(data)
                df['source_dataset'] = dataset['name']
                df['source_year'] = dataset['year']
                all_dataframes.append(df)
        except Exception as e:
            print(f"Error processing {dataset['name']}: {e}")
            continue
    
    if not all_dataframes:
        print("No data was successfully fetched!")
        return
    
    # Concatenate all data
    print(f"\nConcatenating {len(all_dataframes)} datasets...")
    combined_df = pd.concat(all_dataframes, ignore_index=True, sort=False)
    print(f"Total records before cleaning: {len(combined_df)}")
    
    # Clean and process
    final_df = clean_and_process_data(combined_df)
    print(f"Total records after cleaning: {len(final_df)}")
    
    # Show summary statistics
    print(f"\nData Summary:")
    print(f"Date range: {final_df['primary_date'].min()} to {final_df['primary_date'].max()}")
    print(f"Years covered: {sorted(final_df['year'].dropna().unique())}")
    print(f"Top call types:")
    if 'call_type' in final_df.columns:
        print(final_df['call_type'].value_counts().head())
    
    # Export to Parquet
    parquet_file = "lapd_calls_for_service.parquet"
    print(f"\nExporting to {parquet_file}...")
    final_df.to_parquet(parquet_file, index=False)
    print(f"Parquet file saved: {os.path.getsize(parquet_file) / (1024*1024):.1f} MB")
    
    # Export to SQLite
    sqlite_file = "lapd_calls_for_service.db"
    print(f"\nExporting to {sqlite_file}...")
    
    with sqlite3.connect(sqlite_file) as conn:
        final_df.to_sql('calls_for_service', conn, if_exists='replace', index=False)
        
        # Create useful indexes
        conn.execute('CREATE INDEX IF NOT EXISTS idx_primary_date ON calls_for_service(primary_date)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_year ON calls_for_service(year)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_call_type ON calls_for_service(call_type)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_area ON calls_for_service(area_occ)')
        
        print(f"SQLite database saved: {os.path.getsize(sqlite_file) / (1024*1024):.1f} MB")
    
    print(f"\n✅ Processing complete!")
    print(f"Files created:")
    print(f"  - {parquet_file}")
    print(f"  - {sqlite_file}")
    print(f"\nYou can now analyze the data using pandas (Parquet) or SQL queries (SQLite)")

if __name__ == "__main__":
    main()

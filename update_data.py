#!/usr/bin/env python3
"""
LAPD Data Updater

Updates the LAPD calls for service data by fetching only the most recent dataset
(2024 to Present) which is updated weekly, and merging it with existing data.
"""

import requests
import pandas as pd
import sqlite3
import os
from datetime import datetime, timedelta
import time

class LAPDDataUpdater:
    def __init__(self):
        self.current_dataset_endpoint = "xjgu-z4ju"  # 2024 to Present
        self.parquet_file = "lapd_calls_for_service.parquet"
        self.sqlite_file = "lapd_calls_for_service.db"
        self.backup_dir = "backups"
        
    def check_for_updates(self):
        """Check if the current dataset has been updated recently"""
        print("Checking for updates...")
        
        # Get dataset metadata
        catalog_url = "https://api.us.socrata.com/api/catalog/v1"
        params = {
            "domains": "data.lacity.org",
            "ids": self.current_dataset_endpoint
        }
        
        try:
            response = requests.get(catalog_url, params=params)
            response.raise_for_status()
            results = response.json()['results']
            
            if results:
                last_updated = results[0]['resource']['updatedAt']
                last_updated_date = pd.to_datetime(last_updated)
                days_since_update = (datetime.now() - last_updated_date.tz_localize(None)).days
                
                print(f"Dataset last updated: {last_updated_date.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Days since update: {days_since_update}")
                
                return last_updated_date, days_since_update < 7  # Consider "recent" if < 7 days
            else:
                print("Could not find dataset information")
                return None, False
                
        except Exception as e:
            print(f"Error checking for updates: {e}")
            return None, False
    
    def backup_existing_data(self):
        """Create backup of existing data files"""
        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Backup parquet file
        if os.path.exists(self.parquet_file):
            backup_parquet = f"{self.backup_dir}/lapd_calls_backup_{timestamp}.parquet"
            os.rename(self.parquet_file, backup_parquet)
            print(f"Backed up parquet file to: {backup_parquet}")
        
        # Backup SQLite file
        if os.path.exists(self.sqlite_file):
            backup_sqlite = f"{self.backup_dir}/lapd_calls_backup_{timestamp}.db"
            os.rename(self.sqlite_file, backup_sqlite)
            print(f"Backed up SQLite file to: {backup_sqlite}")
    
    def fetch_current_dataset(self):
        """Fetch the current 2024+ dataset"""
        print("Fetching current dataset (2024 to Present)...")
        
        base_url = f"https://data.lacity.org/resource/{self.current_dataset_endpoint}.json"
        all_data = []
        offset = 0
        batch_size = 50000
        
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
                time.sleep(0.1)  # Be respectful to the API
                
            except requests.exceptions.RequestException as e:
                print(f"  Error fetching batch at offset {offset}: {e}")
                break
        
        print(f"  Total records fetched: {len(all_data)}")
        return all_data
    
    def load_historical_data(self):
        """Load existing historical data (2010-2023)"""
        if os.path.exists(f"{self.backup_dir}"):
            # Find most recent backup
            backup_files = [f for f in os.listdir(self.backup_dir) if f.endswith('.parquet')]
            if backup_files:
                latest_backup = sorted(backup_files)[-1]
                backup_path = os.path.join(self.backup_dir, latest_backup)
                print(f"Loading historical data from backup: {backup_path}")
                
                df = pd.read_parquet(backup_path)
                # Filter out 2024+ data (we'll replace it with fresh data)
                df = df[df['year'] < 2024]
                print(f"Loaded {len(df)} historical records (2010-2023)")
                return df
        
        print("No historical data found. Run full data processing first.")
        return None
    
    def process_and_merge_data(self, current_data, historical_data=None):
        """Process current data and merge with historical data"""
        print("Processing and merging data...")
        
        # Process current data
        current_df = pd.DataFrame(current_data)
        current_df['source_dataset'] = 'LAPD Calls for Service 2024 to Present'
        current_df['source_year'] = 2024
        
        # Apply same cleaning as main processor
        current_df = self.clean_data(current_df)
        
        if historical_data is not None:
            # Merge with historical data
            print(f"Merging {len(current_df)} current records with {len(historical_data)} historical records")
            final_df = pd.concat([historical_data, current_df], ignore_index=True, sort=False)
        else:
            final_df = current_df
        
        # Remove duplicates based on incident_number
        initial_count = len(final_df)
        final_df = final_df.drop_duplicates(subset=['incident_number'], keep='last')
        print(f"Removed {initial_count - len(final_df)} duplicate records")
        
        return final_df
    
    def clean_data(self, df):
        """Clean and standardize data (simplified version of main processor)"""
        df = df.copy()
        
        # Standardize column names
        column_mapping = {
            'date_rptd': 'report_date',
            'date_occ': 'occurrence_date', 
            'time_occ': 'occurrence_time',
            'dispatch_date': 'dispatch_date',
            'dispatch_time': 'dispatch_time'
        }
        df.rename(columns=column_mapping, inplace=True)
        
        # Process dates
        date_columns = ['report_date', 'occurrence_date', 'dispatch_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Create primary date
        if 'occurrence_date' in df.columns:
            df['primary_date'] = df['occurrence_date']
        elif 'report_date' in df.columns:
            df['primary_date'] = df['report_date']
        elif 'dispatch_date' in df.columns:
            df['primary_date'] = df['dispatch_date']
        else:
            df['primary_date'] = pd.NaT
        
        # Add time components
        df['year'] = df['primary_date'].dt.year
        df['month'] = df['primary_date'].dt.month
        df['day_of_week'] = df['primary_date'].dt.day_name()
        
        # Clean up text fields
        if 'call_type_text' in df.columns:
            df['call_type_text'] = df['call_type_text'].str.strip()
        if 'area_occ' in df.columns:
            df['area_occ'] = df['area_occ'].str.strip()
        
        # Remove invalid dates
        df = df.dropna(subset=['primary_date'])
        
        return df
    
    def export_data(self, df):
        """Export data to parquet and SQLite"""
        print("Exporting updated data...")
        
        # Export to Parquet
        df.to_parquet(self.parquet_file, index=False)
        print(f"Parquet file saved: {os.path.getsize(self.parquet_file) / (1024*1024):.1f} MB")
        
        # Export to SQLite
        with sqlite3.connect(self.sqlite_file) as conn:
            df.to_sql('calls_for_service', conn, if_exists='replace', index=False)
            
            # Create indexes
            conn.execute('CREATE INDEX IF NOT EXISTS idx_primary_date ON calls_for_service(primary_date)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_year ON calls_for_service(year)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_call_type ON calls_for_service(call_type_text)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_area ON calls_for_service(area_occ)')
            
        print(f"SQLite database saved: {os.path.getsize(self.sqlite_file) / (1024*1024):.1f} MB")
    
    def update(self, force=False):
        """Main update method"""
        print("LAPD Data Updater")
        print("=" * 50)
        
        # Check for updates
        last_updated, is_recent = self.check_for_updates()
        
        if not force and not is_recent:
            print("No recent updates found. Use --force to update anyway.")
            return
        
        # Backup existing data
        self.backup_existing_data()
        
        # Fetch current data
        current_data = self.fetch_current_dataset()
        if not current_data:
            print("No current data fetched. Aborting update.")
            return
        
        # Load historical data
        historical_data = self.load_historical_data()
        
        # Process and merge
        final_df = self.process_and_merge_data(current_data, historical_data)
        
        # Export
        self.export_data(final_df)
        
        print(f"\n✅ Update complete!")
        print(f"Total records: {len(final_df)}")
        print(f"Date range: {final_df['primary_date'].min()} to {final_df['primary_date'].max()}")

def main():
    import sys
    
    updater = LAPDDataUpdater()
    force = '--force' in sys.argv
    
    updater.update(force=force)

if __name__ == "__main__":
    main() 
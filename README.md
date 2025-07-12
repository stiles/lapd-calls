# LAPD calls analysis

This project analyzes Los Angeles Police Department calls for service data from the LA City data portal.

## What this does

- Fetches all available LAPD calls for service datasets from data.lacity.org
- Combines data from multiple years (2010-2024+) into a single dataset
- Cleans and standardizes the data with proper date handling
- Exports to both Parquet and SQLite formats for analysis
- Provides tools to explore the broader LA City data portal

## Setup

This project uses uv for dependency management. Dependencies are already installed.

### Current status
✅ Environment set up with uv  
✅ Dependencies installed (requests, pandas, pyarrow, tqdm, matplotlib, seaborn)  
✅ Exploration tool ready  
✅ Data processing script ready  
✅ **Data processed: 19M+ records (2010-2025)**  
✅ Update strategy implemented  
✅ Analysis framework ready

## Scripts

### `explore_portal.py` - Data portal exploration
General exploration tool for the LA City data portal:
- Search for datasets by keyword
- Browse available datasets  
- Get sample data from any dataset

```bash
# Search for datasets
python explore_portal.py search "police"
python explore_portal.py search "traffic"
python explore_portal.py search "budget"

# Browse all categories
python explore_portal.py categories

# Get sample data from any dataset (use API endpoint)
python explore_portal.py sample xjgu-z4ju
```

### `process_lapd_data.py` - LAPD data processor
Main data processing script that:
- Downloads all LAPD calls for service data (2010-2024+)
- Cleans and processes the data
- Exports to `lapd_calls_for_service.parquet` and `lapd_calls_for_service.db`

```bash
python process_lapd_data.py
```

**Note:** This script will take some time to run as it downloads several years of data. Progress will be shown.

### `update_data.py` - Incremental data updates
Smart updater that only fetches the most recent dataset (2024+) and merges with existing data:
- Checks for recent updates (weekly)
- Creates backups before updating
- Only downloads current year data
- Merges with historical data
- Removes duplicates

```bash
# Check for updates and update if recent
python update_data.py

# Force update regardless of last update time
python update_data.py --force
```

## Analysis scripts

The `analysis/` directory contains focused analysis scripts for specific insights:

### `analysis/fireworks_analysis.py` - Fireworks calls analysis
Comprehensive analysis of fireworks-related calls including:
- Identification of fireworks-related incidents using keyword matching
- Yearly trends and patterns
- Seasonal analysis (monthly patterns)
- Day of week patterns
- Geographic distribution by LAPD area
- Holiday period analysis (July 4th, New Year's)
- Generates charts and summary statistics

```bash
python analysis/fireworks_analysis.py
```

**Output:** Creates visualizations in the `analysis/` directory and prints detailed statistics.

## Data structure

The processed dataset includes:
- `incident_number` - Unique identifier for each call
- `primary_date` - Main date for the incident (occurrence, report, or dispatch date)
- `year`, `month`, `day_of_week`, `hour` - Time components for analysis
- `call_type_code`, `call_type_text` - Type of call
- `area_occ` - Area where call occurred
- `source_dataset`, `source_year` - Metadata about data source

## Analysis examples

### Using pandas (Parquet file)
```python
import pandas as pd

# Load the data
df = pd.read_parquet('lapd_calls_for_service.parquet')

# Calls by year
calls_by_year = df.groupby('year').size()

# Most common call types
top_call_types = df['call_type_text'].value_counts().head(10)

# Calls by hour of day
calls_by_hour = df.groupby('hour').size()
```

### Using SQL (SQLite database)
```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('lapd_calls_for_service.db')

# Calls by year
yearly_calls = pd.read_sql("""
    SELECT year, COUNT(*) as call_count 
    FROM calls_for_service 
    GROUP BY year 
    ORDER BY year
""", conn)

# Top call types by area
top_calls_by_area = pd.read_sql("""
    SELECT area_occ, call_type_text, COUNT(*) as count
    FROM calls_for_service 
    WHERE area_occ IS NOT NULL
    GROUP BY area_occ, call_type_text
    ORDER BY count DESC
    LIMIT 20
""", conn)
```

## Data strategy

### For local analysis
- **Data files are stored locally** (not in Git due to size)
- **Parquet file** (262 MB) - efficient for pandas
- **SQLite database** (4.1 GB) - for SQL analysis
- **Automatic backups** created during updates

### For sharing/collaboration
- **Repository contains only code** - no data files
- **Others can run the scripts** to generate their own data
- **Update script** keeps data current with weekly updates
- **Documentation** explains the data structure and sources

### Update workflow
- **Historical data** (2010-2023): Static, rarely changes
- **Current data** (2024+): Updated weekly by LAPD
- **Smart updates**: Only fetch recent data, merge with historical

## Getting started

### First time setup
1. **Explore the data portal**:
   ```bash
   python explore_portal.py search "police"
   python explore_portal.py categories
   ```

2. **Process all historical data** (one-time, takes ~30 minutes):
   ```bash
   python process_lapd_data.py
   ```

### Ongoing maintenance
3. **Keep data updated** (weekly):
   ```bash
   python update_data.py
   ```

4. **Start analyzing** with 19M+ records ready to go!

## Data sources

Data comes from the LA City data portal (data.lacity.org) via the Socrata API. The project automatically discovers and processes all available LAPD calls for service datasets. 
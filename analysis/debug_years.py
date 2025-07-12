#!/usr/bin/env python3
"""
Debug script to check what years are available in the data
"""

import pandas as pd

# Load the data
df = pd.read_parquet("lapd_calls_for_service.parquet")

print("Checking years in the dataset...")
print("="*50)

# Check all years in the dataset
all_years = df['year'].value_counts().sort_index()
print("All years in dataset:")
print(all_years)

print(f"\nDataset columns: {list(df.columns)}")

print("\n" + "="*50)
print("Checking call_type_text column for missing years...")

missing_years = [2011, 2012, 2013, 2017]

for year in missing_years:
    year_data = df[df['year'] == year]
    print(f"\nYear {year} - Total calls: {len(year_data)}")
    
    # Check call_type_text column
    print(f"call_type_text null count: {year_data['call_type_text'].isnull().sum()}")
    print(f"call_type_text non-null count: {year_data['call_type_text'].notnull().sum()}")
    
    # Check unique values
    unique_values = year_data['call_type_text'].dropna().unique()
    print(f"Unique call_type_text values: {len(unique_values)}")
    
    if len(unique_values) > 0:
        print("Sample values:")
        print(unique_values[:10])
    
    # Check other columns that might have call information
    for col in ['call_type_code', 'source_dataset']:
        if col in year_data.columns:
            print(f"{col} sample values:")
            print(year_data[col].value_counts().head(5))
    
    # Check a few sample records
    print("Sample records:")
    sample_cols = ['primary_date', 'call_type_text', 'call_type_code', 'source_dataset']
    available_cols = [col for col in sample_cols if col in year_data.columns]
    print(year_data[available_cols].head(3))

print("\n" + "="*50)
print("Checking source datasets for missing years...")

for year in missing_years:
    year_data = df[df['year'] == year]
    if 'source_dataset' in year_data.columns:
        print(f"\nYear {year} source datasets:")
        print(year_data['source_dataset'].value_counts())

print("\n" + "="*50)
print("Now checking fireworks-related calls by year...")

# Replicate the fireworks identification logic
fireworks_keywords = [
    'firework', 'fireworks', 'firecracker', 'firecrackers',
    'roman candle', 'bottle rocket', 'sparkler', 'sparklers',
    'cherry bomb', 'm-80', 'illegal fireworks', 'pyrotechnic',
    'explosive', 'loud noise', 'noise complaint'
]

# Create search text
df['search_text'] = df['call_type_text'].fillna('').str.lower()

# Search for fireworks-related keywords
fireworks_mask = df['search_text'].str.contains(
    '|'.join(fireworks_keywords), 
    case=False, 
    na=False
)

# Also look for specific call types
fireworks_call_types = [
    'FIREWORKS', 'NOISE COMPLAINT', 'LOUD NOISE', 'DISTURBING THE PEACE',
    'ILLEGAL FIREWORKS', 'PYROTECHNIC'
]

call_type_mask = df['call_type_text'].str.contains(
    '|'.join(fireworks_call_types),
    case=False,
    na=False
)

# Combine both search methods
combined_mask = fireworks_mask | call_type_mask
fireworks_df = df[combined_mask]

print(f"Total fireworks calls found: {len(fireworks_df)}")

# Check years with fireworks calls
fireworks_years = fireworks_df['year'].value_counts().sort_index()
print("\nFireworks calls by year:")
print(fireworks_years)

# Check what years are missing
all_years_set = set(all_years.index)
fireworks_years_set = set(fireworks_years.index)
missing_years = all_years_set - fireworks_years_set

print(f"\nMissing years from fireworks analysis: {sorted(missing_years)}")

# For missing years, let's check what call types exist
for year in sorted(missing_years):
    year_data = df[df['year'] == year]
    print(f"\nYear {year} - Total calls: {len(year_data)}")
    print(f"Top call types in {year}:")
    print(year_data['call_type_text'].value_counts().head(10))
    
    # Check if there are any potential fireworks calls we missed
    potential_fireworks = year_data[year_data['call_type_text'].str.contains('NOISE|LOUD|DISTURB|FIRE', case=False, na=False)]
    print(f"Potential fireworks-related calls in {year}: {len(potential_fireworks)}")
    if len(potential_fireworks) > 0:
        print("Sample call types:")
        print(potential_fireworks['call_type_text'].value_counts().head(5)) 
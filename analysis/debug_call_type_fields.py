#!/usr/bin/env python3
"""
Debug call type fields and CODE 6 representation in LAPD data
"""
import pandas as pd

df = pd.read_parquet("lapd_calls_for_service.parquet")

print("Columns in DataFrame:")
print(list(df.columns))

# Check value counts for call_type (standardized)
if 'call_type' in df.columns:
    print(f"\nTop values in call_type:")
    print(df['call_type'].value_counts().head(10))
else:
    print("\nColumn 'call_type' not found.")

# Check how CODE 6 appears in call_type
if 'call_type' in df.columns:
    print(f"\nSample records with 'CODE 6' in call_type:")
    print(df[df['call_type'].str.contains('CODE 6', case=False, na=False)][['call_type', 'call_type_code']].head(10))

# Check for 006 in call_type_code
if 'call_type_code' in df.columns:
    print("\nSample records with call_type_code == '006':")
    print(df[df['call_type_code'] == '006'][['call_type_code', 'call_type']].head(10)) 
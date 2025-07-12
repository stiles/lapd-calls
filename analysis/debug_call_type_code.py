#!/usr/bin/env python3
"""
Check for multiple or variant call_type_code values in LAPD data
"""
import pandas as pd

df = pd.read_parquet("lapd_calls_for_service.parquet")

# Check for delimiters in call_type_code
multi_code_mask = df['call_type_code'].astype(str).str.contains(r'[ ,/]', na=False)
multi_code_examples = df[multi_code_mask]['call_type_code'].value_counts().head(20)
print("Most common call_type_code values with delimiters (potentially multiple codes):")
print(multi_code_examples)

# Check for any codes that contain 507F but are not exactly 507F
variant_mask = df['call_type_code'].astype(str).str.contains('507F', na=False) & (df['call_type_code'] != '507F')
variant_examples = df[variant_mask]['call_type_code'].value_counts().head(20)
print("\nMost common call_type_code values containing '507F' but not exactly '507F':")
print(variant_examples)

# Show a few sample records for manual inspection
print("\nSample records with call_type_code containing '507F':")
print(df[variant_mask][['call_type_code', 'call_type_text']].head(10)) 
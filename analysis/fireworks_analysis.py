#!/usr/bin/env python3
"""
Fireworks Calls Analysis

Analyzes LAPD calls for service data to identify and analyze fireworks-related incidents.
Examines patterns by year, season, time of day, and geographic distribution.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3
import numpy as np
from datetime import datetime
import os
import re

# Set CNN style for all charts
plt.rcParams.update({
    "font.family": "CNN Sans",  
    "font.size": 12,
    "axes.titlesize": 14,
    "axes.labelcolor": "#6E6E6E",
    "xtick.color": "#6E6E6E",
    "ytick.color": "#6E6E6E",
    "text.color": "#262626",
    "axes.edgecolor": "#B1B1B1",
    "axes.linewidth": 0.5,
    "figure.facecolor": "#FEFEFE",
    "axes.facecolor": "#FEFEFE",
    "grid.color": "#B1B1B1",
    "grid.linewidth": 0.25
})

# CNN palette for categories
CNN_COLORS = ["#5194C3", "#F8C153", "#C52622", "#53A796", "#F18851", "#7C4EA5"]

class FireworksAnalyzer:
    def __init__(self, data_file="lapd_calls_for_service.parquet"):
        """Initialize the analyzer with the LAPD data"""
        self.data_file = data_file
        self.df = None
        self.fireworks_df = None
        
        # Using call_type_code '507F' for fireworks identification
        
        # Setup plotting style
        sns.set_palette("husl")
        
    def load_data(self):
        """Load the LAPD calls data"""
        if not os.path.exists(self.data_file):
            print(f"Data file {self.data_file} not found.")
            print("Run 'python process_lapd_data.py' first to create the dataset.")
            return False
        
        print(f"Loading data from {self.data_file}...")
        self.df = pd.read_parquet(self.data_file)
        print(f"Loaded {len(self.df):,} total records")
        print(f"Date range: {self.df['primary_date'].min()} to {self.df['primary_date'].max()}")
        return True
    
    def identify_fireworks_calls(self):
        """Identify calls that are likely related to fireworks, excluding Code 6 calls"""
        print("\nIdentifying fireworks-related calls (excluding Code 6)...")
        
        # Exclude Code 6 and variants using standardized 'call_type' and call_type_code
        code6_mask = (
            (self.df['call_type_code'] == '006') |
            (self.df['call_type'].str.contains('^CODE 6', case=False, na=False))
        )
        filtered_df = self.df[~code6_mask]
        
        # Simple approach: just filter for call_type_code = '507F' (fireworks code)
        fireworks_mask = filtered_df['call_type_code'] == '507F'
        
        self.fireworks_df = filtered_df[fireworks_mask].copy()
        
        print(f"Found {len(self.fireworks_df):,} fireworks-related calls (507F code, non-Code 6)")
        print(f"That's {len(self.fireworks_df)/len(self.df)*100:.2f}% of all calls")
        
        return len(self.fireworks_df) > 0
    
    def analyze_by_year(self):
        """Analyze fireworks calls by year"""
        print("\nAnalyzing fireworks calls by year...")
        
        yearly_counts = self.fireworks_df.groupby('year').size().reset_index(name='fireworks_calls')
        total_yearly = self.df.groupby('year').size().reset_index(name='total_calls')
        
        yearly_analysis = yearly_counts.merge(total_yearly, on='year')
        yearly_analysis['percentage'] = (yearly_analysis['fireworks_calls'] / yearly_analysis['total_calls']) * 100
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), dpi=120)
        
        # Bar chart: absolute counts (all blue)
        bars = ax1.bar(yearly_analysis['year'], yearly_analysis['fireworks_calls'], color=CNN_COLORS[0], alpha=0.85)
        ax1.set_title('Fireworks-related calls by year', loc='left', fontsize=14, fontweight='medium', color='#262626')
        ax1.set_xlabel('Year', fontsize=12, color='#6E6E6E')
        ax1.set_ylabel('Number of calls', fontsize=12, color='#6E6E6E')
        ax1.grid(True, axis='y', linestyle='-', linewidth=0.25, color='#B1B1B1', alpha=0.7)
        ax1.set_axisbelow(True)
        years = yearly_analysis['year']
        ax1.set_xlim(years.min() - 0.5, years.max() + 0.5)
        for i, v in enumerate(yearly_analysis['fireworks_calls']):
            ax1.text(yearly_analysis['year'].iloc[i], v + 10, str(v), ha='center', va='bottom', fontsize=10, color='#262626')
        
        # Line chart: percentage of total calls (blue)
        ax2.plot(yearly_analysis['year'], yearly_analysis['percentage'], marker='o', linewidth=2, markersize=6, color=CNN_COLORS[0])
        ax2.set_title('Fireworks calls as percentage of total calls', loc='left', fontsize=14, fontweight='medium', color='#262626')
        ax2.set_xlabel('Year', fontsize=12, color='#6E6E6E')
        ax2.set_ylabel('Percentage (%)', fontsize=12, color='#6E6E6E')
        ax2.grid(True, axis='y', linestyle='-', linewidth=0.25, color='#B1B1B1', alpha=0.7)
        ax2.set_axisbelow(True)
        ax2.set_xlim(years.min() - 0.5, years.max() + 0.5)
        for i, v in enumerate(yearly_analysis['percentage']):
            ax2.text(yearly_analysis['year'].iloc[i], v + 0.02, f"{v:.2f}", ha='center', va='bottom', fontsize=10, color='#262626')
        
        plt.tight_layout(rect=[0, 0.05, 1, 1])
        plt.figtext(0.01, 0.01, "Source: LAPD Calls for Service via data.lacity.org", fontsize=12, color="#8e8e8e")
        plt.savefig('analysis/fireworks_by_year.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("\nYearly Summary:")
        print(yearly_analysis.to_string(index=False))
        
        return yearly_analysis
    
    def analyze_by_month(self):
        """Analyze fireworks calls by month to identify seasonal patterns"""
        print("\nAnalyzing fireworks calls by month...")
        
        monthly_counts = self.fireworks_df.groupby('month').size().reset_index(name='calls')
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        monthly_counts['month_name'] = [month_names[i-1] for i in monthly_counts['month']]
        
        plt.figure(figsize=(10, 6), dpi=120)
        bar_colors = [CNN_COLORS[0]] * 12
        bar_colors[6] = CNN_COLORS[2]  # Highlight July in red
        bars = plt.bar(monthly_counts['month_name'], monthly_counts['calls'], color=bar_colors, alpha=0.85)
        plt.title('Fireworks-related calls by month (all years)', loc='left', fontsize=14, fontweight='medium', color='#262626')
        plt.xlabel('Month', fontsize=12, color='#6E6E6E')
        plt.ylabel('Number of calls', fontsize=12, color='#6E6E6E')
        plt.grid(True, axis='y', linestyle='-', linewidth=0.25, color='#B1B1B1', alpha=0.7)
        plt.gca().set_axisbelow(True)
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 200,
                    f'{int(height)}', ha='center', va='bottom', fontsize=10, color='#262626')
        plt.tight_layout(rect=[0, 0.05, 1, 1])
        plt.figtext(0.01, 0.01, "Source: LAPD Calls for Service via data.lacity.org", fontsize=12, color="#8e8e8e")
        plt.savefig('analysis/fireworks_by_month.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("\nMonthly Summary:")
        print(monthly_counts[['month_name', 'calls']].to_string(index=False))
        
        return monthly_counts
    
    def analyze_by_day_of_week(self):
        """Analyze fireworks calls by day of week"""
        print("\nAnalyzing fireworks calls by day of week...")
        
        dow_counts = self.fireworks_df['day_of_week'].value_counts()
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        dow_counts = dow_counts.reindex(day_order)
        
        plt.figure(figsize=(9, 6), dpi=120)
        bars = plt.bar(dow_counts.index, dow_counts.values, color=CNN_COLORS[0], alpha=0.85)
        plt.title('Fireworks-related calls by day of week', loc='left', fontsize=14, fontweight='medium', color='#262626')
        plt.xlabel('Day of week', fontsize=12, color='#6E6E6E')
        plt.ylabel('Number of calls', fontsize=12, color='#6E6E6E')
        plt.grid(True, axis='y', linestyle='-', linewidth=0.25, color='#B1B1B1', alpha=0.7)
        plt.gca().set_axisbelow(True)
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 200,
                    f'{int(height)}', ha='center', va='bottom', fontsize=10, color='#262626')
        plt.tight_layout(rect=[0, 0.05, 1, 1])
        plt.figtext(0.01, 0.01, "Source: LAPD Calls for Service via data.lacity.org", fontsize=12, color="#8e8e8e")
        plt.savefig('analysis/fireworks_by_day_of_week.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("\nDay of Week Summary:")
        print(dow_counts.to_string())
        
        return dow_counts
    
    def analyze_by_area(self):
        """Analyze fireworks calls by LAPD area"""
        print("\nAnalyzing fireworks calls by LAPD area...")
        
        area_counts = self.fireworks_df['area_occ'].value_counts().head(15)
        
        plt.figure(figsize=(10, 8), dpi=120)
        bars = plt.barh(range(len(area_counts)), area_counts.values, color=CNN_COLORS[0], alpha=0.85)
        plt.yticks(range(len(area_counts)), area_counts.index, fontsize=12, color='#6E6E6E')
        plt.title('Top 15 LAPD areas for fireworks-related calls', loc='left', fontsize=14, fontweight='medium', color='#262626')
        plt.xlabel('Number of calls', fontsize=12, color='#6E6E6E')
        plt.ylabel('LAPD area', fontsize=12, color='#6E6E6E')
        plt.grid(True, axis='x', linestyle='-', linewidth=0.25, color='#B1B1B1', alpha=0.7)
        plt.gca().set_axisbelow(True)
        for i, bar in enumerate(bars):
            width = bar.get_width()
            plt.text(width + 100, bar.get_y() + bar.get_height()/2.,
                    f'{int(width)}', ha='left', va='center', fontsize=10, color='#262626')
        plt.tight_layout(rect=[0, 0.05, 1, 1])
        plt.figtext(0.01, 0.01, "Source: LAPD Calls for Service via data.lacity.org", fontsize=12, color="#8e8e8e")
        plt.savefig('analysis/fireworks_by_area.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("\nTop Areas Summary:")
        print(area_counts.to_string())
        
        return area_counts
    
    def analyze_july_4th_by_year(self):
        """Analyze fireworks calls specifically around July 4th (July 1-5) by year."""
        print("\nAnalyzing fireworks calls for July 1-5 by year...")

        # Filter for July 1-5
        july_4th_mask = (
            (self.fireworks_df['primary_date'].dt.month == 7) &
            (self.fireworks_df['primary_date'].dt.day.between(1, 5))
        )
        july_4th_df = self.fireworks_df[july_4th_mask]

        if july_4th_df.empty:
            print("No fireworks calls found between July 1-5 in any year.")
            return None

        # Group by year and count
        yearly_july_4th_counts = july_4th_df.groupby('year').size().reset_index(name='calls')

        # Plotting
        plt.figure(figsize=(10, 6), dpi=120)
        
        # All bars blue, highlight 2025 in dark blue
        dark_blue = '#3E74A3'
        bar_colors = [CNN_COLORS[0]] * len(yearly_july_4th_counts)
        try:
            year_2025_idx = yearly_july_4th_counts[yearly_july_4th_counts['year'] == 2025].index[0]
            bar_colors[year_2025_idx] = dark_blue
        except IndexError:
            pass # 2025 not in data

        bars = plt.bar(yearly_july_4th_counts['year'], yearly_july_4th_counts['calls'], color=bar_colors, alpha=0.85)
        plt.title('Fireworks calls from July 1-5 by year', loc='left', fontsize=14, fontweight='medium', color='#262626')
        plt.xlabel('Year', fontsize=12, color='#6E6E6E')
        plt.ylabel('Number of calls', fontsize=12, color='#6E6E6E')
        plt.grid(True, axis='y', linestyle='-', linewidth=0.25, color='#B1B1B1', alpha=0.7)
        plt.gca().set_axisbelow(True)

        years = yearly_july_4th_counts['year']
        plt.xlim(years.min() - 0.5, years.max() + 0.5)
        
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width() / 2., height + 10,
                    f'{int(height)}', ha='center', va='bottom', fontsize=10, color='#262626')

        plt.tight_layout(rect=[0, 0.05, 1, 1])
        plt.figtext(0.01, 0.01, "Source: LAPD Calls for Service via data.lacity.org", fontsize=12, color="#8e8e8e")
        plt.savefig('analysis/fireworks_july_4th_by_year.png', dpi=300, bbox_inches='tight')
        plt.show()

        print("\nJuly 4th Holiday (July 1-5) Summary:")
        print(yearly_july_4th_counts.to_string(index=False))

        return yearly_july_4th_counts
    
    def analyze_holiday_patterns(self):
        """Analyze fireworks calls around major holidays"""
        print("\nAnalyzing fireworks calls around major holidays...")
        
        holidays = {
            'New Year': (1, 1),
            'Independence Day': (7, 4),
            'New Year Eve': (12, 31)
        }
        holiday_analysis = {}
        
        plt.figure(figsize=(10, 6), dpi=120)
        for i, (holiday_name, (month, day)) in enumerate(holidays.items()):
            holiday_mask = (
                (self.fireworks_df['month'] == month) & 
                (self.fireworks_df['primary_date'].dt.day.between(day-3, day+3))
            )
            holiday_calls = self.fireworks_df[holiday_mask]
            holiday_analysis[holiday_name] = len(holiday_calls)
            plt.bar(holiday_name, len(holiday_calls), color=CNN_COLORS[i], alpha=0.85)
            plt.text(holiday_name, len(holiday_calls) + 100, f'{len(holiday_calls)}', ha='center', va='bottom', fontsize=12, color='#262626')
        plt.title('Fireworks-related calls around major holidays (±3 days)', loc='left', fontsize=14, fontweight='medium', color='#262626')
        plt.xlabel('Holiday', fontsize=12, color='#6E6E6E')
        plt.ylabel('Number of calls', fontsize=12, color='#6E6E6E')
        plt.grid(True, axis='y', linestyle='-', linewidth=0.25, color='#B1B1B1', alpha=0.7)
        plt.gca().set_axisbelow(True)
        plt.tight_layout(rect=[0, 0.05, 1, 1])
        plt.figtext(0.01, 0.01, "Source: LAPD Calls for Service via data.lacity.org", fontsize=12, color="#8e8e8e")
        plt.savefig('analysis/fireworks_by_holiday.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        for holiday_name, count in holiday_analysis.items():
            print(f"\n{holiday_name} (±3 days): {count} calls")
            yearly_holiday = self.fireworks_df[(self.fireworks_df['month'] == holidays[holiday_name][0]) & (self.fireworks_df['primary_date'].dt.day.between(holidays[holiday_name][1]-3, holidays[holiday_name][1]+3))].groupby('year').size()
            if len(yearly_holiday) > 0:
                print(f"  Average per year: {yearly_holiday.mean():.1f}")
                print(f"  Peak year: {yearly_holiday.idxmax()} ({yearly_holiday.max()} calls)")
        
        return holiday_analysis
    
    def generate_summary_report(self):
        """Generate a comprehensive summary report"""
        print("\n" + "="*60)
        print("FIREWORKS CALLS ANALYSIS SUMMARY")
        print("="*60)
        
        total_calls = len(self.df)
        fireworks_calls = len(self.fireworks_df)
        percentage = (fireworks_calls / total_calls) * 100
        
        print(f"Total LAPD calls analyzed: {total_calls:,}")
        print(f"Fireworks-related calls: {fireworks_calls:,}")
        print(f"Percentage of total calls: {percentage:.3f}%")
        
        # Time range
        date_range = self.fireworks_df['primary_date']
        print(f"Date range: {date_range.min().strftime('%Y-%m-%d')} to {date_range.max().strftime('%Y-%m-%d')}")
        
        # Peak periods
        yearly_counts = self.fireworks_df.groupby('year').size()
        monthly_counts = self.fireworks_df.groupby('month').size()
        
        print(f"\nPeak year: {yearly_counts.idxmax()} ({yearly_counts.max():,} calls)")
        print(f"Peak month: {monthly_counts.idxmax()} ({monthly_counts.max():,} calls)")
        
        # Most common call types
        print(f"\nMost common call types:")
        call_types = self.fireworks_df['call_type'].value_counts().head(5)
        for call_type, count in call_types.items():
            print(f"  {call_type}: {count:,} calls")
        
        print("\nAnalysis complete! Check the 'analysis/' directory for charts.")
    
    def run_full_analysis(self):
        """Run the complete fireworks analysis"""
        print("LAPD Fireworks Calls Analysis")
        print("="*40)
        
        # Load data
        if not self.load_data():
            return
        
        # Identify fireworks calls
        if not self.identify_fireworks_calls():
            print("No fireworks-related calls found.")
            return
        
        # Run all analyses
        self.analyze_by_year()
        self.analyze_by_month()
        self.analyze_by_day_of_week()
        self.analyze_by_area()
        self.analyze_holiday_patterns()
        self.analyze_july_4th_by_year()
        
        # Generate summary
        self.generate_summary_report()

def main():
    analyzer = FireworksAnalyzer()
    analyzer.run_full_analysis()

if __name__ == "__main__":
    main() 
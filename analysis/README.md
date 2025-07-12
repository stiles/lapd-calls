# Analysis scripts

This directory contains focused analysis scripts for specific insights into the LAPD calls data.

## Available analyses

### `fireworks_analysis.py`
**Purpose:** Analyze fireworks-related calls over time to understand patterns and trends.

**What it does:**
- Identifies fireworks-related calls using keyword matching
- Analyzes trends by year, month, day of week, and LAPD area
- Examines holiday patterns (July 4th, New Year's)
- Generates visualizations and summary statistics

**Usage:**
```bash
python analysis/fireworks_analysis.py
```

**Output:**
- Charts saved as PNG files in the `analysis/` directory
- Console output with detailed statistics
- Summary report with key findings

## Adding new analyses

To add a new analysis:

1. **Create a new Python file** in this directory
2. **Follow the naming pattern**: `[topic]_analysis.py`
3. **Include comprehensive documentation** in the script
4. **Generate visualizations** and save them here
5. **Update this README** with the new analysis

## Analysis structure template

```python
#!/usr/bin/env python3
"""
[Topic] Analysis

Brief description of what this analysis does.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

class [Topic]Analyzer:
    def __init__(self, data_file="lapd_calls_for_service.parquet"):
        self.data_file = data_file
        self.df = None
        
    def load_data(self):
        # Load and prepare data
        pass
        
    def identify_relevant_calls(self):
        # Filter for calls relevant to your analysis
        pass
        
    def analyze_patterns(self):
        # Perform analysis and create visualizations
        pass
        
    def generate_report(self):
        # Generate summary statistics and findings
        pass
        
    def run_analysis(self):
        # Main method that runs the complete analysis
        pass

def main():
    analyzer = [Topic]Analyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main()
```

## Tips for analysis scripts

- **Be specific**: Focus on one topic or question per script
- **Document everything**: Include docstrings and comments
- **Save visualizations**: Use descriptive filenames
- **Print summaries**: Include key statistics in console output
- **Handle errors**: Check if data files exist before processing
- **Use consistent styling**: Follow the established patterns 
#!/usr/bin/env python3
"""
LA City Data Portal Explorer

A tool for exploring datasets available on data.lacity.org
"""

import requests
import pandas as pd
import sys
from typing import Optional, List, Dict

class LACityDataExplorer:
    """Explore datasets on the LA City data portal"""
    
    def __init__(self):
        self.base_url = "https://api.us.socrata.com/api/catalog/v1"
        self.domain = "data.lacity.org"
    
    def search_datasets(self, query: str, limit: int = 20) -> List[Dict]:
        """Search for datasets by keyword"""
        print(f"Searching for '{query}' on {self.domain}...")
        
        params = {
            "domains": self.domain,
            "q": query,
            "limit": limit
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            results = response.json()['results']
            
            datasets = []
            for r in results:
                resource = r["resource"]
                datasets.append({
                    "name": resource["name"],
                    "description": resource.get("description", "")[:200] + "..." if len(resource.get("description", "")) > 200 else resource.get("description", ""),
                    "type": resource.get("type", ""),
                    "updated": resource.get("updatedAt", ""),
                    "permalink": r["permalink"],
                    "api_endpoint": resource["id"]
                })
            
            return datasets
            
        except requests.exceptions.RequestException as e:
            print(f"Error searching datasets: {e}")
            return []
    
    def browse_categories(self) -> List[Dict]:
        """Browse all available categories"""
        print(f"Browsing categories on {self.domain}...")
        
        params = {
            "domains": self.domain,
            "limit": 100
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            results = response.json()['results']
            
            # Extract categories
            categories = {}
            for r in results:
                resource = r["resource"]
                category = resource.get("category", "Uncategorized")
                if category not in categories:
                    categories[category] = 0
                categories[category] += 1
            
            return [{"category": cat, "count": count} for cat, count in sorted(categories.items())]
            
        except requests.exceptions.RequestException as e:
            print(f"Error browsing categories: {e}")
            return []
    
    def get_sample_data(self, endpoint: str, limit: int = 5) -> Optional[pd.DataFrame]:
        """Get sample data from a dataset"""
        print(f"Fetching sample data from endpoint: {endpoint}")
        
        url = f"https://data.lacity.org/resource/{endpoint}.json"
        params = {"$limit": limit}
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data:
                df = pd.DataFrame(data)
                return df
            else:
                print("No data returned")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching sample data: {e}")
            return None
    
    def display_datasets(self, datasets: List[Dict]):
        """Display datasets in a formatted table"""
        if not datasets:
            print("No datasets found.")
            return
        
        df = pd.DataFrame(datasets)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_colwidth', 50)
        pd.set_option('display.width', None)
        
        print(f"\nFound {len(datasets)} datasets:")
        print("=" * 80)
        
        for i, dataset in enumerate(datasets, 1):
            print(f"{i}. {dataset['name']}")
            if dataset['description']:
                print(f"   Description: {dataset['description']}")
            print(f"   Type: {dataset['type']}")
            print(f"   Updated: {dataset['updated']}")
            print(f"   API Endpoint: {dataset['api_endpoint']}")
            print(f"   URL: {dataset['permalink']}")
            print("-" * 80)

def main():
    explorer = LACityDataExplorer()
    
    if len(sys.argv) < 2:
        print("LA City Data Portal Explorer")
        print("=" * 40)
        print("Usage:")
        print("  python explore_portal.py search <keyword>")
        print("  python explore_portal.py categories")
        print("  python explore_portal.py sample <api_endpoint>")
        print("\nExamples:")
        print("  python explore_portal.py search 'police'")
        print("  python explore_portal.py search 'traffic'")
        print("  python explore_portal.py sample xjgu-z4ju")
        return
    
    command = sys.argv[1].lower()
    
    if command == "search":
        if len(sys.argv) < 3:
            print("Please provide a search term")
            return
        
        query = " ".join(sys.argv[2:])
        datasets = explorer.search_datasets(query)
        explorer.display_datasets(datasets)
        
    elif command == "categories":
        categories = explorer.browse_categories()
        if categories:
            print("\nAvailable categories:")
            print("=" * 40)
            for cat in categories:
                print(f"{cat['category']}: {cat['count']} datasets")
        
    elif command == "sample":
        if len(sys.argv) < 3:
            print("Please provide an API endpoint")
            return
        
        endpoint = sys.argv[2]
        df = explorer.get_sample_data(endpoint)
        if df is not None:
            print(f"\nSample data (first 5 rows):")
            print("=" * 50)
            print(df.head())
            print(f"\nColumns: {list(df.columns)}")
            print(f"Data types:\n{df.dtypes}")
    
    else:
        print(f"Unknown command: {command}")
        print("Use 'search', 'categories', or 'sample'")

if __name__ == "__main__":
    main() 
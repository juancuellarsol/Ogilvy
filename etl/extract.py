"""
Data Extraction Module

Handles extraction of data from various sources.
"""

import pandas as pd
import json
import csv
import logging
from typing import Any, Dict, List, Optional
from pathlib import Path


class DataExtractor:
    """Handles data extraction from various sources."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def extract_from_csv(self, file_path: str, **kwargs) -> pd.DataFrame:
        """Extract data from CSV file."""
        try:
            self.logger.info(f"Extracting data from CSV: {file_path}")
            data = pd.read_csv(file_path, **kwargs)
            self.logger.info(f"Successfully extracted {len(data)} rows from CSV")
            return data
        except Exception as e:
            self.logger.error(f"Failed to extract from CSV {file_path}: {e}")
            raise
    
    def extract_from_json(self, file_path: str) -> pd.DataFrame:
        """Extract data from JSON file."""
        try:
            self.logger.info(f"Extracting data from JSON: {file_path}")
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Convert to DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                raise ValueError("JSON data must be a list or dictionary")
            
            self.logger.info(f"Successfully extracted {len(df)} rows from JSON")
            return df
        except Exception as e:
            self.logger.error(f"Failed to extract from JSON {file_path}: {e}")
            raise
    
    def extract_from_dict(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Extract data from dictionary."""
        try:
            self.logger.info("Extracting data from dictionary")
            df = pd.DataFrame([data]) if not isinstance(data, list) else pd.DataFrame(data)
            self.logger.info(f"Successfully extracted {len(df)} rows from dictionary")
            return df
        except Exception as e:
            self.logger.error(f"Failed to extract from dictionary: {e}")
            raise
    
    def extract_sample_data(self) -> pd.DataFrame:
        """Generate sample data for testing."""
        self.logger.info("Generating sample data")
        sample_data = {
            'id': range(1, 101),
            'name': [f'User_{i}' for i in range(1, 101)],
            'age': [20 + (i % 50) for i in range(100)],
            'city': ['New York', 'London', 'Tokyo', 'Paris', 'Sydney'] * 20,
            'salary': [50000 + (i * 1000) for i in range(100)]
        }
        df = pd.DataFrame(sample_data)
        self.logger.info(f"Generated {len(df)} rows of sample data")
        return df
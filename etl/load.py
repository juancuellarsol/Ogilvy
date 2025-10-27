"""
Data Loading Module

Handles loading of processed data to various destinations.
"""

import pandas as pd
import numpy as np
import json
import logging
from typing import Any, Dict, Optional
from pathlib import Path


class DataLoader:
    """Handles data loading to various destinations."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def load_to_csv(self, df: pd.DataFrame, file_path: str, **kwargs) -> None:
        """Load data to CSV file."""
        try:
            self.logger.info(f"Loading {len(df)} rows to CSV: {file_path}")
            
            # Create directory if it doesn't exist
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            df.to_csv(file_path, index=False, **kwargs)
            self.logger.info(f"Successfully loaded data to CSV: {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to load to CSV {file_path}: {e}")
            raise
    
    def load_to_json(self, df: pd.DataFrame, file_path: str, orient: str = 'records') -> None:
        """Load data to JSON file."""
        try:
            self.logger.info(f"Loading {len(df)} rows to JSON: {file_path}")
            
            # Create directory if it doesn't exist
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            df.to_json(file_path, orient=orient, indent=2)
            self.logger.info(f"Successfully loaded data to JSON: {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to load to JSON {file_path}: {e}")
            raise
    
    def load_to_excel(self, df: pd.DataFrame, file_path: str, sheet_name: str = 'Sheet1') -> None:
        """Load data to Excel file."""
        try:
            self.logger.info(f"Loading {len(df)} rows to Excel: {file_path}")
            
            # Create directory if it doesn't exist
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            df.to_excel(file_path, sheet_name=sheet_name, index=False)
            self.logger.info(f"Successfully loaded data to Excel: {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to load to Excel {file_path}: {e}")
            raise
    
    def load_multiple_sheets(self, data_dict: Dict[str, pd.DataFrame], file_path: str) -> None:
        """Load multiple DataFrames to different sheets in Excel."""
        try:
            self.logger.info(f"Loading multiple sheets to Excel: {file_path}")
            
            # Create directory if it doesn't exist
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                for sheet_name, df in data_dict.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    self.logger.info(f"Loaded {len(df)} rows to sheet: {sheet_name}")
            
            self.logger.info(f"Successfully loaded {len(data_dict)} sheets to Excel")
        except Exception as e:
            self.logger.error(f"Failed to load multiple sheets to Excel {file_path}: {e}")
            raise
    
    def save_summary(self, df: pd.DataFrame, file_path: str) -> None:
        """Save data summary statistics to file."""
        try:
            self.logger.info(f"Saving data summary to: {file_path}")
            
            # Create directory if it doesn't exist
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            summary = {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'column_names': list(df.columns),
                'data_types': df.dtypes.astype(str).to_dict(),
                'missing_values': df.isnull().sum().to_dict(),
                'summary_statistics': df.describe().to_dict() if len(df.select_dtypes(include=[np.number]).columns) > 0 else {}
            }
            
            with open(file_path, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            self.logger.info(f"Successfully saved data summary")
        except Exception as e:
            self.logger.error(f"Failed to save summary to {file_path}: {e}")
            raise
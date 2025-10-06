"""
Data Transformation Module

Handles data transformation and cleaning operations.
"""

import pandas as pd
import numpy as np
import logging
from typing import Any, Callable, Dict, List, Optional


class DataTransformer:
    """Handles data transformation operations."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Basic data cleaning operations."""
        try:
            self.logger.info("Starting data cleaning")
            original_rows = len(df)
            
            # Remove duplicates
            df = df.drop_duplicates()
            
            # Handle missing values using new pandas methods
            df = df.ffill().bfill()
            
            # Remove rows where all values are null
            df = df.dropna(how='all')
            
            cleaned_rows = len(df)
            self.logger.info(f"Data cleaning complete: {original_rows} -> {cleaned_rows} rows")
            return df
        except Exception as e:
            self.logger.error(f"Failed to clean data: {e}")
            raise
    
    def filter_data(self, df: pd.DataFrame, condition: str) -> pd.DataFrame:
        """Filter data based on condition."""
        try:
            self.logger.info(f"Filtering data with condition: {condition}")
            original_rows = len(df)
            filtered_df = df.query(condition)
            filtered_rows = len(filtered_df)
            self.logger.info(f"Filtering complete: {original_rows} -> {filtered_rows} rows")
            return filtered_df
        except Exception as e:
            self.logger.error(f"Failed to filter data: {e}")
            raise
    
    def add_calculated_column(self, df: pd.DataFrame, column_name: str, 
                            calculation: Callable[[pd.DataFrame], pd.Series]) -> pd.DataFrame:
        """Add a calculated column to the DataFrame."""
        try:
            self.logger.info(f"Adding calculated column: {column_name}")
            df = df.copy()
            df[column_name] = calculation(df)
            self.logger.info(f"Successfully added column: {column_name}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to add calculated column {column_name}: {e}")
            raise
    
    def normalize_column(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Normalize a numeric column to 0-1 range."""
        try:
            self.logger.info(f"Normalizing column: {column}")
            df = df.copy()
            min_val = df[column].min()
            max_val = df[column].max()
            df[f'{column}_normalized'] = (df[column] - min_val) / (max_val - min_val)
            self.logger.info(f"Successfully normalized column: {column}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to normalize column {column}: {e}")
            raise
    
    def group_and_aggregate(self, df: pd.DataFrame, group_by: List[str], 
                          aggregations: Dict[str, str]) -> pd.DataFrame:
        """Group data and apply aggregations."""
        try:
            self.logger.info(f"Grouping by {group_by} with aggregations: {aggregations}")
            grouped_df = df.groupby(group_by).agg(aggregations).reset_index()
            # Flatten column names if multi-level
            if isinstance(grouped_df.columns, pd.MultiIndex):
                grouped_df.columns = ['_'.join(col).strip() for col in grouped_df.columns.values]
            self.logger.info(f"Grouping complete: {len(grouped_df)} groups created")
            return grouped_df
        except Exception as e:
            self.logger.error(f"Failed to group and aggregate data: {e}")
            raise
    
    def apply_custom_transformation(self, df: pd.DataFrame, 
                                  transformation_func: Callable[[pd.DataFrame], pd.DataFrame]) -> pd.DataFrame:
        """Apply a custom transformation function."""
        try:
            self.logger.info("Applying custom transformation")
            transformed_df = transformation_func(df)
            self.logger.info("Custom transformation complete")
            return transformed_df
        except Exception as e:
            self.logger.error(f"Failed to apply custom transformation: {e}")
            raise
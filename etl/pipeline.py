"""
ETL Pipeline Orchestrator

Main pipeline that coordinates the Extract, Transform, and Load operations.
"""

import pandas as pd
import logging
import yaml
from typing import Any, Dict, List, Optional
from pathlib import Path

from .extract import DataExtractor
from .transform import DataTransformer
from .load import DataLoader


class ETLPipeline:
    """Main ETL pipeline orchestrator."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.loader = DataLoader()
        
        # Load configuration if provided
        self.config = {}
        if config_path and Path(config_path).exists():
            self.load_config(config_path)
    
    def load_config(self, config_path: str) -> None:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            self.logger.info(f"Configuration loaded from {config_path}")
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            raise
    
    def run_pipeline(self, extract_config: Dict[str, Any], 
                    transform_config: Optional[Dict[str, Any]] = None,
                    load_config: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Run the complete ETL pipeline."""
        try:
            self.logger.info("Starting ETL pipeline")
            
            # Extract phase
            data = self.extract_data(extract_config)
            self.logger.info(f"Extract phase complete: {len(data)} rows")
            
            # Transform phase
            if transform_config:
                data = self.transform_data(data, transform_config)
                self.logger.info(f"Transform phase complete: {len(data)} rows")
            
            # Load phase
            if load_config:
                self.load_data(data, load_config)
                self.logger.info("Load phase complete")
            
            self.logger.info("ETL pipeline completed successfully")
            return data
            
        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {e}")
            raise
    
    def extract_data(self, config: Dict[str, Any]) -> pd.DataFrame:
        """Extract data based on configuration."""
        source_type = config.get('type', 'sample')
        
        if source_type == 'csv':
            return self.extractor.extract_from_csv(config['file_path'], **config.get('options', {}))
        elif source_type == 'json':
            return self.extractor.extract_from_json(config['file_path'])
        elif source_type == 'dict':
            return self.extractor.extract_from_dict(config['data'])
        elif source_type == 'sample':
            return self.extractor.extract_sample_data()
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def transform_data(self, data: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Transform data based on configuration."""
        transformations = config.get('operations', [])
        
        for transformation in transformations:
            operation = transformation.get('type')
            
            if operation == 'clean':
                data = self.transformer.clean_data(data)
            
            elif operation == 'filter':
                condition = transformation.get('condition')
                data = self.transformer.filter_data(data, condition)
            
            elif operation == 'add_column':
                column_name = transformation.get('column_name')
                # Simple calculated columns based on configuration
                if 'calculation' in transformation:
                    calc_type = transformation['calculation']['type']
                    if calc_type == 'multiply':
                        col1 = transformation['calculation']['column']
                        factor = transformation['calculation']['factor']
                        data = self.transformer.add_calculated_column(
                            data, column_name, lambda df: df[col1] * factor
                        )
            
            elif operation == 'normalize':
                column = transformation.get('column')
                data = self.transformer.normalize_column(data, column)
            
            elif operation == 'group':
                group_by = transformation.get('group_by', [])
                aggregations = transformation.get('aggregations', {})
                data = self.transformer.group_and_aggregate(data, group_by, aggregations)
        
        return data
    
    def load_data(self, data: pd.DataFrame, config: Dict[str, Any]) -> None:
        """Load data based on configuration."""
        destinations = config.get('destinations', [])
        
        for destination in destinations:
            dest_type = destination.get('type')
            file_path = destination.get('file_path')
            
            if dest_type == 'csv':
                self.loader.load_to_csv(data, file_path, **destination.get('options', {}))
            
            elif dest_type == 'json':
                orient = destination.get('orient', 'records')
                self.loader.load_to_json(data, file_path, orient)
            
            elif dest_type == 'excel':
                sheet_name = destination.get('sheet_name', 'Sheet1')
                self.loader.load_to_excel(data, file_path, sheet_name)
            
            elif dest_type == 'summary':
                self.loader.save_summary(data, file_path)
    
    def run_from_config(self) -> pd.DataFrame:
        """Run pipeline using loaded configuration."""
        if not self.config:
            raise ValueError("No configuration loaded. Use load_config() or provide config_path in constructor.")
        
        extract_config = self.config.get('extract', {})
        transform_config = self.config.get('transform')
        load_config = self.config.get('load')
        
        return self.run_pipeline(extract_config, transform_config, load_config)
#!/usr/bin/env python3
"""
Test ETL with CSV input data
"""

import sys
from pathlib import Path
import logging

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from etl import ETLPipeline

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def main():
    """Test ETL with CSV input."""
    setup_logging()
    
    print("🔄 Testing ETL with CSV input data...")
    
    # Initialize pipeline
    pipeline = ETLPipeline()
    
    # Configuration for CSV input
    extract_config = {
        'type': 'csv',
        'file_path': 'data/input/sample_employees.csv'
    }
    
    transform_config = {
        'operations': [
            {'type': 'clean'},
            {'type': 'filter', 'condition': 'age >= 30'},
            {
                'type': 'add_column',
                'column_name': 'salary_increase',
                'calculation': {
                    'type': 'multiply',
                    'column': 'salary',
                    'factor': 0.05
                }
            }
        ]
    }
    
    load_config = {
        'destinations': [
            {'type': 'csv', 'file_path': 'data/output/employees_processed.csv'},
            {'type': 'summary', 'file_path': 'data/output/employees_summary.json'}
        ]
    }
    
    # Run pipeline
    result = pipeline.run_pipeline(extract_config, transform_config, load_config)
    
    print(f"✅ CSV ETL completed! Processed {len(result)} employees")
    print("\n📋 Processed employees:")
    print(result[['name', 'department', 'age', 'salary', 'salary_increase']].to_string())

if __name__ == "__main__":
    main()
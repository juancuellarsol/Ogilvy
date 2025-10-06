#!/usr/bin/env python3
"""
Test ETL with JSON input data
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
    """Test ETL with JSON input."""
    setup_logging()
    
    print("🔄 Testing ETL with JSON input data...")
    
    # Initialize pipeline
    pipeline = ETLPipeline()
    
    # Configuration for JSON input
    extract_config = {
        'type': 'json',
        'file_path': 'data/input/sample_products.json'
    }
    
    transform_config = {
        'operations': [
            {'type': 'clean'},
            {'type': 'filter', 'condition': 'price > 50'},
            {
                'type': 'add_column',
                'column_name': 'total_value',
                'calculation': {
                    'type': 'multiply',
                    'column': 'price',
                    'factor': 1  # We'll multiply by stock_quantity in a custom way
                }
            }
        ]
    }
    
    load_config = {
        'destinations': [
            {'type': 'csv', 'file_path': 'data/output/products_processed.csv'},
            {'type': 'json', 'file_path': 'data/output/products_processed.json'},
            {'type': 'summary', 'file_path': 'data/output/products_summary.json'}
        ]
    }
    
    # Run pipeline
    result = pipeline.run_pipeline(extract_config, transform_config, load_config)
    
    # Add custom calculation for total value
    result['total_inventory_value'] = result['price'] * result['stock_quantity']
    
    print(f"✅ JSON ETL completed! Processed {len(result)} products")
    print("\n📋 Processed products:")
    print(result[['product_name', 'category', 'price', 'stock_quantity', 'total_inventory_value']].to_string())

if __name__ == "__main__":
    main()
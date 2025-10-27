#!/usr/bin/env python3
"""
Sample ETL Process Example

This example demonstrates how to use the ETL framework
with custom configurations and transformations.
"""

import sys
from pathlib import Path
import logging

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from etl import ETLPipeline, DataExtractor, DataTransformer, DataLoader


def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def example_basic_etl():
    """Example of a basic ETL process without configuration file."""
    print("🔄 Running Basic ETL Example...")
    
    # Initialize components
    extractor = DataExtractor()
    transformer = DataTransformer()
    loader = DataLoader()
    
    # Extract sample data
    data = extractor.extract_sample_data()
    print(f"📥 Extracted {len(data)} rows")
    
    # Transform data
    data = transformer.clean_data(data)
    data = transformer.filter_data(data, "age >= 30")
    data = transformer.add_calculated_column(
        data, 'salary_category', 
        lambda df: df['salary'].apply(lambda x: 'High' if x > 75000 else 'Medium' if x > 50000 else 'Low')
    )
    print(f"🔄 Transformed data: {len(data)} rows remaining")
    
    # Load data
    loader.load_to_csv(data, "data/output/example_basic.csv")
    loader.load_to_json(data, "data/output/example_basic.json")
    loader.save_summary(data, "data/output/example_basic_summary.json")
    
    print("✅ Basic ETL example completed!")
    return data


def example_pipeline_etl():
    """Example using the ETL pipeline with custom configuration."""
    print("\n🔄 Running Pipeline ETL Example...")
    
    # Initialize pipeline
    pipeline = ETLPipeline()
    
    # Define custom configuration
    extract_config = {
        'type': 'sample'
    }
    
    transform_config = {
        'operations': [
            {'type': 'clean'},
            {'type': 'filter', 'condition': 'salary > 60000'},
            {
                'type': 'add_column',
                'column_name': 'annual_bonus',
                'calculation': {
                    'type': 'multiply',
                    'column': 'salary',
                    'factor': 0.15
                }
            },
            {'type': 'normalize', 'column': 'salary'}
        ]
    }
    
    load_config = {
        'destinations': [
            {'type': 'csv', 'file_path': 'data/output/example_pipeline.csv'},
            {'type': 'json', 'file_path': 'data/output/example_pipeline.json'},
            {'type': 'summary', 'file_path': 'data/output/example_pipeline_summary.json'}
        ]
    }
    
    # Run pipeline
    result = pipeline.run_pipeline(extract_config, transform_config, load_config)
    
    print(f"✅ Pipeline ETL example completed! Processed {len(result)} rows")
    return result


def example_custom_transformation():
    """Example with custom transformation function."""
    print("\n🔄 Running Custom Transformation Example...")
    
    extractor = DataExtractor()
    transformer = DataTransformer()
    loader = DataLoader()
    
    # Extract data
    data = extractor.extract_sample_data()
    
    # Custom transformation function
    def custom_transform(df):
        """Custom transformation: categorize by age groups and salary."""
        df = df.copy()
        
        # Age groups
        df['age_group'] = df['age'].apply(
            lambda x: 'Young' if x < 30 else 'Middle' if x < 50 else 'Senior'
        )
        
        # Salary tier
        df['salary_tier'] = df['salary'].apply(
            lambda x: 'Entry' if x < 60000 else 'Mid' if x < 90000 else 'Senior'
        )
        
        # Performance score (synthetic)
        df['performance_score'] = (df['salary'] / 1000 + df['age']) / 10
        
        return df
    
    # Apply transformations
    data = transformer.clean_data(data)
    data = transformer.apply_custom_transformation(data, custom_transform)
    
    # Load results
    loader.load_to_csv(data, "data/output/example_custom.csv")
    loader.save_summary(data, "data/output/example_custom_summary.json")
    
    print(f"✅ Custom transformation example completed! Processed {len(data)} rows")
    return data


def main():
    """Run all examples."""
    setup_logging()
    
    print("🚀 ETL Framework Examples")
    print("=" * 50)
    
    try:
        # Run examples
        example_basic_etl()
        example_pipeline_etl()
        example_custom_transformation()
        
        print("\n🎉 All examples completed successfully!")
        print("📂 Check the 'data/output/' directory for results")
        
    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
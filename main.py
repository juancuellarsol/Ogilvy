#!/usr/bin/env python3
"""
ETL Process - Main Entry Point

A simple ETL pipeline for personal data processing tasks.
"""

import logging
import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from etl import ETLPipeline


def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('etl_process.log')
        ]
    )


def main():
    """Main entry point for the ETL process."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting ETL process")
        
        # Initialize pipeline with configuration
        config_path = "config/config.yaml"
        pipeline = ETLPipeline(config_path)
        
        # Run the pipeline
        result_data = pipeline.run_from_config()
        
        logger.info(f"ETL process completed successfully. Processed {len(result_data)} rows.")
        print(f"\n✅ ETL Process Complete!")
        print(f"📊 Processed {len(result_data)} rows of data")
        print(f"📂 Output files saved in: data/output/")
        print(f"📋 Check 'etl_process.log' for detailed logs")
        
        # Display sample of processed data
        print(f"\n📋 Sample of processed data:")
        print(result_data.head().to_string())
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        print(f"\n❌ ETL Process Failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
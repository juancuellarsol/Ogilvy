# Ogilvy ETL Process

A simple and modular ETL (Extract, Transform, Load) process framework for personal data processing tasks.

## Overview

This repository contains a lightweight ETL pipeline that can:
- **Extract** data from various sources (CSV, JSON, APIs)
- **Transform** data using customizable transformation functions
- **Load** processed data to different destinations (CSV, JSON, databases)

## Structure

```
├── etl/
│   ├── __init__.py
│   ├── extract.py      # Data extraction modules
│   ├── transform.py    # Data transformation functions
│   ├── load.py         # Data loading modules
│   └── pipeline.py     # Main ETL orchestrator
├── config/
│   └── config.yaml     # Configuration settings
├── data/
│   ├── input/          # Sample input data
│   └── output/         # Processed output data
├── examples/
│   └── sample_etl.py   # Example usage
├── requirements.txt    # Python dependencies
└── main.py            # Entry point
```

## Quick Start

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the sample ETL process:
   ```bash
   python main.py
   ```

3. Check the output in `data/output/` directory

## Configuration

Edit `config/config.yaml` to customize:
- Data sources and destinations
- Transformation rules
- Processing options

## Features

- Modular design for easy extension
- Configuration-driven processing
- Logging and error handling
- Support for multiple data formats
- Simple and clean API
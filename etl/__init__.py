"""
ETL (Extract, Transform, Load) Package

A simple and modular ETL framework for data processing tasks.
"""

__version__ = "1.0.0"
__author__ = "Juan Cuellar"

from .pipeline import ETLPipeline
from .extract import DataExtractor
from .transform import DataTransformer
from .load import DataLoader

__all__ = ['ETLPipeline', 'DataExtractor', 'DataTransformer', 'DataLoader']
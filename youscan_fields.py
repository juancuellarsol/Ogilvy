# youscan_fields.py

# This script extracts and maps specific fields from Sprinklr exports.

import pandas as pd

# Define the mapping of fields
field_mappings = {
    'Author': 'author',
    'Text snippet': 'message',
    'URL': 'link',
    'Date': 'date_original',
    'Time': 'hour',
    'Source': 'source',
    'Sentiment': 'sentiment',
    'Country': 'country',
    'Engagement': 'engagement',
    'Views': 'views'
    
}

# Function to extract and map fields
def extract_and_map_fields(df):
    # Create a new DataFrame with mapped fields
    mapped_df = df.rename(columns=field_mappings)
    return mapped_df

# Example usage
if __name__ == '__main__':
    # Load your Sprinklr export data here, for example:
    # df = pd.read_csv('sprinklr_export.csv')
    # mapped_data = extract_and_map_fields(df)
    pass

# sprinklr_fields.py

# This script extracts and maps specific fields from Sprinklr exports.

import pandas as pd

# Define the mapping of fields
field_mappings = {
    'From User': 'author',
    'Conversation Stream': 'message',
    'Sender Profile Image Url': 'link',
    'Created Time': 'date_original',
    'snTypeColumn': 'source',
    'Sentiment': 'sentiment',
    'Country': 'country',
    'Reach (SUM)': 'reach',
    'Earned Engagements (Recalibrated) (SUM)': 'engagement',
    'Mentions (SUM)': 'mentions'
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

import os
csv_path = os.getenv('ZILLOW_DATA')

import json
if not csv_path:
    with open('config.json', 'r') as f:
        config = json.load(f)
    csv_path = config['zillow_data']

import pandas as pd

# Now load the CSV file using pandas
df = pd.read_csv(csv_path)

print("Successfully loaded Zillow data!")
print(df.head())
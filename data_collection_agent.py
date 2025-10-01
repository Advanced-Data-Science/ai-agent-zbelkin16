import os
import json
import pandas as pd
import logging
from datetime import datetime
import time
import random


class ZillowCSVDataAgent:
    def __init__(self, config_file='agent/config.json'):
        self.config = self.load_config(config_file)
        self.setup_logging()
        self.data = None
        self.collection_stats = {
            'start_time': datetime.now(),
            'rows_loaded': 0,
            'columns_loaded': 0,
            'missing_values': 0,
            'quality_score': 0
        }
        self.delay_multiplier = 1.0

    def load_config(self, config_file):
        with open(config_file, 'r') as f:
            config = json.load(f)

        # Check for path in config first, then env
        csv_path = config.get('csv_path') or os.getenv('ZILLOW_DATA')

        if not csv_path:
            raise ValueError("CSV path not specified in config or environment.")

        # Prepend directory if not absolute
        if not os.path.isabs(csv_path):
            csv_path = os.path.join('data', 'raw', csv_path)

        config['csv_path'] = csv_path
        config['base_delay'] = config.get('base_delay', 1.0)
        return config

    def setup_logging(self):
        os.makedirs('logs', exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/collection.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def respectful_delay(self):
        base_delay = self.config.get('base_delay', 1.0)
        jitter = random.uniform(0.5, 1.5)
        delay = base_delay * self.delay_multiplier * jitter
        self.logger.info(f"Sleeping for {delay:.2f} seconds to respect collection pacing.")
        time.sleep(delay)

    def load_csv_data(self):
        csv_path = self.config.get('csv_path')
        if not csv_path or not os.path.exists(csv_path):
            self.logger.error(f"CSV file not found at {csv_path}")
            raise FileNotFoundError(f"CSV file not found at {csv_path}")

        self.data = pd.read_csv(csv_path)
        self.collection_stats['rows_loaded'] = self.data.shape[0]
        self.collection_stats['columns_loaded'] = self.data.shape[1]
        self.logger.info(f"Loaded CSV data with {self.data.shape[0]} rows and {self.data.shape[1]} columns.")

    def assess_data_quality(self):
        if self.data is None:
            self.logger.warning("No data loaded for quality assessment")
            return 0

        missing_values = self.data.isnull().sum().sum()
        self.collection_stats['missing_values'] = missing_values

        total_values = self.data.size
        completeness = (total_values - missing_values) / total_values

        date_cols = self.data.columns[9:]

        try:
            date_values = pd.to_datetime(date_cols, format='%m/%d/%Y', errors='coerce')
            consistency = 1.0 if date_values.is_monotonic_increasing else 0.5
        except Exception as e:
            self.logger.error(f"Error parsing date columns: {e}")
            consistency = 0.5

        last_date_str = date_cols[-1]
        last_date = pd.to_datetime(last_date_str, format='%m/%d/%Y', errors='coerce')
        days_diff = (pd.Timestamp.today() - last_date).days if pd.notna(last_date) else None
        timeliness = 1.0 if days_diff is not None and days_diff < 60 else 0.5

        quality_score = (completeness + consistency + timeliness) / 3
        self.collection_stats['quality_score'] = quality_score

        self.logger.info(
            f"Data Quality - Completeness: {completeness:.2f}, "
            f"Consistency: {consistency:.2f}, "
            f"Timeliness: {timeliness:.2f if timeliness else 'N/A'}, "
            f"Overall: {quality_score:.2f}"
        )
        return quality_score

    def save_processed_data(self):
        if self.data is None:
            self.logger.error("No data to save")
            return
        os.makedirs('data/processed', exist_ok=True)
        output_path = os.path.join('data', 'processed', 'processed_zillow_data.csv')
        self.data.to_csv(output_path, index=False)
        self.logger.info(f"Processed data saved to {output_path}")

    def run(self):
        self.logger.info("Starting Zillow CSV data agent")
        self.load_csv_data()
        self.respectful_delay()
        self.assess_data_quality()
        self.respectful_delay()
        self.save_processed_data()
        self.logger.info("Zillow CSV data agent finished successfully")


# Example usage
if __name__ == "__main__":
    agent = ZillowCSVDataAgent()
    agent.run()

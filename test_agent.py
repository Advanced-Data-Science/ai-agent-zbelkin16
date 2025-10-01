import os
import pytest
from agent.data_collection_agent import ZillowCSVDataAgent

# Absolute path to config.json (from test_agent.py location)
CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config.json'))

def test_load_config():
    agent = ZillowCSVDataAgent(config_file=CONFIG_PATH)
    assert agent.config['csv_path'] is not None
    # Check that csv_path points to an existing file
    csv_path = os.path.abspath(os.path.join(os.path.dirname(CONFIG_PATH), agent.config['csv_path']))
    assert os.path.exists(csv_path), f"CSV file not found at {csv_path}"

def test_load_csv_data():
    agent = ZillowCSVDataAgent(config_file=CONFIG_PATH)
    agent.load_csv_data()
    assert agent.data is not None
    assert agent.data.shape[0] > 0  # At least one row

def test_assess_data_quality():
    agent = ZillowCSVDataAgent(config_file=CONFIG_PATH)
    agent.load_csv_data()
    quality_score = agent.assess_data_quality()
    assert 0 <= quality_score <= 1  # Quality score is a ratio

def test_save_processed_data(tmp_path):
    agent = ZillowCSVDataAgent(config_file=CONFIG_PATH)
    agent.load_csv_data()
    output_file = tmp_path / "processed_zillow_data.csv"
    agent.save_processed_data(output_path=str(output_file))
    assert output_file.exists()
    # Optional: verify saved file is not empty
    assert output_file.stat().st_size > 0

def test_run_entire_agent(tmp_path):
    agent = ZillowCSVDataAgent(config_file=CONFIG_PATH)
    # Override save path to temp folder
    agent.save_processed_data = lambda output_path=str(tmp_path / "processed_zillow_data.csv"): agent.data.to_csv(output_path, index=False)
    agent.run()
    output_file = tmp_path / "processed_zillow_data.csv"
    assert output_file.exists()

import requests
import json
import logging
from time import sleep

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Make API call to get a random cat fact
def get_cat_fact():
    url = "https://catfact.ninja/fact"

    try:
        response = requests.get(url, timeout=5)  # timeout for safety

        if response.status_code == 200:
            data = response.json()
            return data.get('fact')
        else:
            logging.error(f"HTTP Error: {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None

# Get 5 unique cat facts
def get_multiple_cat_facts(count=5):
    facts = []
    attempts = 0

    while len(facts) < count and attempts < count * 2:  # prevent infinite loop
        fact = get_cat_fact()
        if fact and fact not in facts:
            facts.append(fact)
            logging.info(f"Collected fact #{len(facts)}")
        else:
            logging.warning("Duplicate or invalid fact encountered. Retrying...")
        attempts += 1
        sleep(1)  # avoid hitting the server too quickly

    if len(facts) < count:
        logging.warning("Could not collect the desired number of unique facts.")

    return facts

# Save facts to a JSON file
def save_facts_to_file(facts, filename="cat_facts.json"):
    try:
        with open(filename, "w") as f:
            json.dump(facts, f, indent=4)
        logging.info(f"Saved {len(facts)} cat facts to {filename}")
    except IOError as e:
        logging.error(f"Failed to write to file: {e}")

# Main execution
if __name__ == "__main__":
    cat_facts = get_multiple_cat_facts(5)
    print("Cat Facts:")
    for fact in cat_facts:
        print(f"- {fact}")
    save_facts_to_file(cat_facts)

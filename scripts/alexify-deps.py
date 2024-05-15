import os
import json
import time
import logging
from fuzzywuzzy import fuzz
import pyalex
import bibtexparser
import concurrent.futures
from requests.exceptions import HTTPError, RequestException

# Configuration for pyalex
pyalex.config.max_retries = 25
pyalex.config.retry_backoff_factor = 0.3
pyalex.config.retry_http_codes = [429, 500, 503]

# Directory paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data")
DEP_DIR = os.path.join(SCRIPT_DIR, "..", "data", "deps")
LOG_DIR = os.path.join(SCRIPT_DIR, "..", "logs")

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "alexify.deps.log")),
        logging.StreamHandler(),
    ],
)

def save_json(data, folder, original_filename):
    """Saves data as JSON in the specified folder."""
    filename = f"{original_filename}.json"
    with open(os.path.join(folder, filename), "w") as json_file:
        json.dump(data, json_file)

def process_deps(url_list, dep_dir, requests_per_day=100000):
    """Process URLs to fetch data from OpenAlex."""
    processed_count = 0
    start_time = time.time()
    for i, full_id_link in enumerate(url_list):
        if processed_count >= requests_per_day:
            logging.info(f"Reached daily quota of {requests_per_day} requests. Stopping processing.")
            break
        work_id = full_id_link.rsplit("/", 1)[-1]
        file_path = os.path.join(dep_dir, f"{work_id}.json")
        if os.path.exists(file_path):
            logging.info(f"File {work_id}.json already exists. Skipping download.")
            continue
        try:
            work = pyalex.Works()[work_id]
            save_json(work, dep_dir, work_id)
            processed_count += 1
            logging.info(f"Successfully processed and saved {work_id}.json")
        except Exception as e:
            logging.error(f"Error fetching work for ID {work_id}: {e}")
        if (i + 1) % 1000 == 0:
            elapsed_time = time.time() - start_time
            logging.info(f"Processed {i + 1}/{len(url_list)} URLs in {elapsed_time//3600}h {elapsed_time%3600//60}m.")

def unique_deps(directory):
    """Collect unique URLs from JSON files in a directory."""
    urls = set()
    for root, _, files in os.walk(directory):
        if os.path.abspath(root).startswith(os.path.abspath(DEP_DIR)):
            continue
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        urls.update(data.get("referenced_works", []))
                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")
    return urls

if __name__ == "__main__":
    os.makedirs(DEP_DIR, exist_ok=True)
    deps_list = unique_deps(DATA_DIR)
    process_deps(deps_list, DEP_DIR)

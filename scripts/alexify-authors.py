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
AUTHORS_DIR = os.path.join(SCRIPT_DIR, "..", "data", "authors")
LOG_DIR = os.path.join(SCRIPT_DIR, "..", "logs")

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "alexify.authors.log")),
        logging.StreamHandler(),
    ],
)

def save_json(data, folder, original_filename):
    """Saves data as JSON in the specified folder."""
    filename = f"{original_filename}.json"
    with open(os.path.join(folder, filename), "w") as json_file:
        json.dump(data, json_file)

def unique_authors(data_dir):
    """
    Scans all JSON files in the specified data directory and its subdirectories,
    extracting and returning a set of unique authors based on their OpenAlex IDs.
    
    Args:
    data_dir (str): The directory to search for JSON files containing author data.
    
    Returns:
    set: A set of unique authors represented by their OpenAlex IDs.
    """
    unique_authors = set()
    for root, dirs, files in os.walk(data_dir):
        if os.path.abspath(root).startswith(os.path.abspath(DEP_DIR)):
            continue
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        authorships = data.get("authorships", [])
                        for authorship in authorships:
                            author = authorship.get("author", [])
                            author_id = author.get("id", "")
                            if author_id:
                                unique_authors.add(author_id)
                except json.JSONDecodeError as e:
                    logging.error(f"JSON decoding error in file {file_path}: {e}")
                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")
    return unique_authors

def process_authors(url_list, author_dir, requests_per_day=100000):
    """Process URLs to fetch data from OpenAlex."""
    processed_count = 0
    start_time = time.time()
    for i, full_id_link in enumerate(url_list):
        if processed_count >= requests_per_day:
            logging.info(f"Reached daily quota of {requests_per_day} requests. Stopping processing.")
            break
        author_id = full_id_link.rsplit("/", 1)[-1]
        file_path = os.path.join(author_dir, f"{author_id}.json")
        if os.path.exists(file_path):
            logging.info(f"File {author_id}.json already exists. Skipping download.")
            continue
        try:
            author = pyalex.Authors()[author_id]
            save_json(author, author_dir, author_id)
            processed_count += 1
            logging.info(f"Successfully processed and saved {author_id}.json")
        except Exception as e:
            logging.error(f"Error fetching work for ID {author_id}: {e}")
        if (i + 1) % 1000 == 0:
            elapsed_time = time.time() - start_time
            logging.info(f"Processed {i + 1}/{len(url_list)} URLs in {elapsed_time//3600}h {elapsed_time%3600//60}m.")

if __name__ == "__main__":
    os.makedirs(AUTHORS_DIR, exist_ok=True)
    author_list = unique_authors(DATA_DIR)
    logging.info(f"Found {len(author_list)} unique authors.")
    process_authors(author_list, AUTHORS_DIR)

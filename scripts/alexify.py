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
LOG_DIR = os.path.join(SCRIPT_DIR, "..", "logs")
CONFERENCES = ["pmlr", "neurips", "jmlr", "tmlr", "mloss"]
FUZZY_MATCH_THRESHOLD = 75

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "alexify.log")),
        logging.StreamHandler(),
    ],
)

def create_subfolder_for_bib(bib_file, conf):
    """Creates a subfolder in the OpenAlex directory for each .bib file."""
    openalex_path = os.path.join(DATA_DIR, conf, "openalex")
    subfolder_name = os.path.splitext(bib_file)[0]
    subfolder_path = os.path.join(openalex_path, subfolder_name)
    os.makedirs(subfolder_path, exist_ok=True)
    return subfolder_path

def save_json(data, folder, original_filename):
    """Saves data as JSON in the specified folder."""
    filename = f"{original_filename}.json"
    with open(os.path.join(folder, filename), "w") as json_file:
        json.dump(data, json_file)

def fuzzy_match_titles(extracted_title, work_title):
    """Check if the two titles match based on a fuzzy matching algorithm."""
    return fuzz.ratio(extracted_title, work_title) > FUZZY_MATCH_THRESHOLD

def process_search_results(bib_file_path, openalex_folder):
    """Process search results to find matching titles."""
    updates = []
    truncated_path = os.path.relpath(bib_file_path, start=os.getcwd())
    with open(bib_file_path, "r") as bib_file:
        bib_data = bibtexparser.load(bib_file)
        for entry in bib_data.entries:
            if entry.get("title") and not entry.get("openalex"):
                entry_key = entry.get("ID")
                extracted_title = entry.get("title")
                match_not_found = True
                try:
                    results = pyalex.Works().search(extracted_title).get()
                    for work in results:
                        if fuzzy_match_titles(extracted_title, work.get("title", "")):
                            detailed_work = pyalex.Works()[work.get("id").rsplit("/", 1)[-1]]
                            save_json(detailed_work, openalex_folder, work.get("id").rsplit("/", 1)[-1])
                            updates.append((extracted_title, work.get("id"), detailed_work.get("abstract"), detailed_work.get("title")))
                            logging.info(f"Processed {entry_key} ./{truncated_path} {work.get('id')} {work.get('title')}")
                            match_not_found = False
                            break
                except (HTTPError, RequestException) as e:
                    logging.error(f"Error searching for {entry_key} ./{truncated_path} {extracted_title}: {e}")
                if match_not_found:
                    logging.warning(f"No match for {entry_key} ./{truncated_path} {extracted_title}")
    return updates

def process_bib_file(bib_file_path, openalex_folder):
    """Processes each .bib file to fetch OpenAlex data for all entries."""
    updates = process_search_results(bib_file_path, openalex_folder)
    if updates:
        update_bib_file(bib_file_path, updates)

def update_bib_file(bib_file_path, updates):
    """Update .bib file with OpenAlex and abstract fields."""
    truncated_path = os.path.relpath(bib_file_path, start=os.getcwd())
    with open(bib_file_path, "r") as bib_file:
        bib_database = bibtexparser.load(bib_file)
        for entry in bib_database.entries:
            for update in updates:
                extracted_title, work_id, abstract, oa_title = update
                if entry.get("title") == extracted_title:
                    entry["openalex"] = work_id
                    entry["abstract"] = abstract
                    entry["title"] = oa_title
                    break
    with open(bib_file_path, "w") as bib_file:
        bib_file.write(bibtexparser.dumps(bib_database))
    logging.info(f"Updated {truncated_path} with {len(updates)} entries")

def main():
    """Process each conference and its bibliography to fetch OpenAlex data."""
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for conf in CONFERENCES:
            openalex_path = os.path.join(DATA_DIR, conf, "openalex")
            os.makedirs(openalex_path, exist_ok=True)
            bib_folder = os.path.join(DATA_DIR, conf, "bibliography")
            if os.path.exists(bib_folder):
                for bib_file in os.listdir(bib_folder):
                    if bib_file.endswith(".bib"):
                        bib_file_path = os.path.join(bib_folder, bib_file)
                        openalex_folder = create_subfolder_for_bib(bib_file, conf)
                        futures.append(executor.submit(process_bib_file, bib_file_path, openalex_folder))
            else:
                logging.warning(f"Bibliography folder not found: {bib_folder}")
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error processing BibTeX file: {e}")

if __name__ == "__main__":
    main()
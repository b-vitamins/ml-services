import os
import json
import time
import logging
from fuzzywuzzy import fuzz
import pyalex
import bibtexparser
import concurrent.futures
from requests.exceptions import HTTPError, RequestException

pyalex.config.max_retries = 25
pyalex.config.retry_backoff_factor = 0.3
pyalex.config.retry_http_codes = [429, 500, 503]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data")
DEP_DIR = os.path.join(SCRIPT_DIR, "..", "data", "deps")
LOG_DIR = os.path.join(SCRIPT_DIR, "..", "logs")
CONFERENCES = [
    "pmlr",
    "neurips",
    "jmlr",
    "tmlr",
    "mloss",
]
FUZZY_MATCH_THRESHOLD = 75

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "alexify.deps.log")),
        logging.StreamHandler(),
    ],
)


def create_subfolder_for_bib(bib_file, conf):
    """Creates a subfolder in the OpenAlex directory for each .bib file."""
    openalex_path = os.path.join(DATA_DIR, conf, "openalex")
    subfolder_name = os.path.splitext(bib_file)[0]
    subfolder_path = os.path.join(openalex_path, subfolder_name)
    if not os.path.exists(subfolder_path):
        os.makedirs(subfolder_path)
    return subfolder_path


def save_json(data, folder, original_filename):
    """Saves data as JSON in the specified folder."""
    filename = f"{original_filename}.json"
    with open(os.path.join(folder, filename), "w") as json_file:
        json.dump(data, json_file)


def fuzzy_match_titles(extracted_title, work_title):
    """Check if the two titles match based on a fuzzy matching algorithm."""
    if extracted_title and work_title:
        return fuzz.ratio(extracted_title, work_title) > FUZZY_MATCH_THRESHOLD
    else:
        return False

def process_search_results(bib_file_path, openalex_folder):
    """Process search results to find matching titles."""
    updates = []
    truncated_path = os.path.relpath(bib_file_path, start=os.getcwd())
    with open(bib_file_path, "r") as bib_file:
        bib_data = bibtexparser.load(bib_file)
        for entry in bib_data.entries:
            if entry.get("title", "") != "" and entry.get("openalex", "") == "":
                entry_key = entry.get("ID")
                extracted_title = entry.get("title", "")
                match_not_found = True
                try:
                    results = pyalex.Works().search(extracted_title).get()
                except (HTTPError, RequestException) as e:
                    logging.error(
                        f"Error searching for {entry_key} ./{truncated_path} {extracted_title}: {e}"
                    )
                for work in results:
                    top_result_title = work.get("title", "")
                    full_id_link = work.get("id")
                    work_id = (
                        full_id_link.rsplit("/", 1)[-1]
                        if full_id_link
                        else None
                    )
                    if work_id:
                        if fuzzy_match_titles(
                            extracted_title, top_result_title
                        ):
                            detailed_work = pyalex.Works()[work_id]
                            if detailed_work:
                                save_json(
                                    detailed_work, openalex_folder, work_id
                                )
                                updates.append(
                                    (
                                        extracted_title,
                                        work_id,
                                        detailed_work["abstract"],
                                        detailed_work.get("title", ""),
                                    )
                                )
                                logging.info(
                                    f"Processed {entry_key} ./{truncated_path} {work_id} {top_result_title}"
                                )
                                match_not_found = False
                                break
            if match_not_found:
                logging.warning(
                    f"No match for {entry_key} ./{truncated_path} {extracted_title}"
                )
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
                if entry.get("title", "") == extracted_title:
                    if work_id:
                        entry["openalex"] = work_id
                    if abstract:
                        entry["abstract"] = abstract
                    if oa_title:
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
            if not os.path.exists(openalex_path):
                os.makedirs(openalex_path)
                logging.info(f"Created OpenAlex directory: {openalex_path}")
            bib_folder = os.path.join(DATA_DIR, conf, "bibliography")
            if os.path.exists(bib_folder):
                for bib_file in os.listdir(bib_folder):
                    if bib_file.endswith(".bib"):
                        bib_file_path = os.path.join(bib_folder, bib_file)
                        openalex_folder = create_subfolder_for_bib(
                            bib_file, conf
                        )
                        futures.append(
                            executor.submit(
                                process_bib_file, bib_file_path, openalex_folder
                            )
                        )
            else:
                logging.warning(f"Bibliography folder not found: {bib_folder}")

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error processing BibTeX file: {e}")

def unique_urls(directory):
    urls = set()
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        referenced_works = data.get("referenced_works", [])
                        urls.update(referenced_works)
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")
    return urls

def process_urls(url_list, dep_dir, requests_per_day=100000):
    processed_count = 0
    start_time = time.time()

    for i, full_id_link in enumerate(url_list):
        if processed_count >= requests_per_day:
            logging.info(f"Reached daily quota of {requests_per_day} requests. Stopping processing.")
            break

        work_id = full_id_link.rsplit("/", 1)[-1] if full_id_link else None
        if work_id:
            file_path = os.path.join(dep_dir, f"{work_id}.json")
            if os.path.exists(file_path):
                logging.info(f"File {work_id}.json already exists. Skipping download.")
                continue

            try:
                work = pyalex.Works()[work_id]
                if work:
                    save_json(work, dep_dir, work_id)
                    processed_count += 1
                    logging.info(f"Successfully processed and saved {work_id}.json")
                else:
                    logging.warning(f"No work found for ID {work_id}")
            except Exception as e:
                logging.error(f"Error fetching work for ID {work_id}: {e}")
        
        if (i + 1) % 1000 == 0:
            elapsed_time = time.time() - start_time
            logging.info(f"Processed {i + 1}/{len(url_list)} URLs in {elapsed_time//3600}h {elapsed_time%3600//60}m.")


if __name__ == "__main__":
    if not os.path.exists(DEP_DIR):
        os.makedirs(DEP_DIR)
    url_list = unique_urls(DATA_DIR)
    process_urls(url_list, DEP_DIR)

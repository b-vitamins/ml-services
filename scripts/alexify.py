import os
import json
from pybtex.database import parse_file
import logging
from fuzzywuzzy import fuzz
import pyalex
import bibtexparser
from requests.exceptions import HTTPError, RequestException

pyalex.config.max_retries = 15
pyalex.config.retry_backoff_factor = 0.1
pyalex.config.retry_http_codes = [429, 500, 503]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data")
LOG_DIR = os.path.join(SCRIPT_DIR, "..", "logs")
CONFERENCES = ["jmlr", "mloss", "neurips", "pmlr", "tmlr"]
FUZZY_MATCH_THRESHOLD = 80

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "alexify.log")),
        logging.StreamHandler(),
    ],
)

PROCESSED_ENTRIES_LOG = os.path.join(LOG_DIR, "processed_entries.log")


def load_processed_entries():
    """Load previously processed BibTeX entries to avoid redundant processing."""
    if os.path.exists(PROCESSED_ENTRIES_LOG):
        with open(PROCESSED_ENTRIES_LOG, "r") as log_file:
            return set(log_file.read().splitlines())
    return set()


def log_processed_entry(entry_id):
    """Log processed entries to avoid redundant processing in subsequent runs."""
    with open(PROCESSED_ENTRIES_LOG, "a") as log_file:
        log_file.write(f"{entry_id}\n")


def create_subfolder_for_bib(bib_file, conf):
    """Creates a subfolder in the OpenAlex directory for each .bib file."""
    openalex_path = os.path.join(DATA_DIR, conf, "openalex")
    subfolder_name = os.path.splitext(bib_file)[0]
    subfolder_path = os.path.join(openalex_path, subfolder_name)
    if not os.path.exists(subfolder_path):
        os.makedirs(subfolder_path)
    return subfolder_path


def normalize_title(title):
    """Normalize a title string for better matching."""
    return title.strip().lower().replace("-", " ")


def extract_titles_from_bib(file_path):
    """Extracts titles from a .bib file."""
    try:
        bib_data = parse_file(file_path)
        return [
            normalize_title(entry.fields.get("title", ""))
            for entry in bib_data.entries.values()
            if entry.fields.get("title")
        ]
    except Exception as e:
        logging.error(f"Error reading {file_path}: {e}")
    return []


def save_json(data, folder, original_filename):
    """Saves data as JSON in the specified folder."""
    filename = f"{original_filename}.json"
    with open(os.path.join(folder, filename), "w") as json_file:
        json.dump(data, json_file)


def fuzzy_match_titles(extracted_title, work_title):
    """Check if the two titles match based on a fuzzy matching algorithm."""
    return (
        fuzz.ratio(extracted_title, normalize_title(work_title))
        > FUZZY_MATCH_THRESHOLD
    )


def log_no_match(bib_file_path, extracted_title):
    """Log when no matching title is found."""
    logging.info(f"No close match for {bib_file_path}. Title: '{extracted_title}'.")


def process_search_results(
    bib_file_path, extracted_titles, results, paper_folder, processed_entries
):
    """Process search results to find a matching title."""
    for extracted_title in extracted_titles:
        for work in results:
            top_result_title = work.get("title", "")
            full_id_link = work.get("id")
            work_id = full_id_link.rsplit("/", 1)[-1] if full_id_link else None

            if work_id and work_id not in processed_entries and (
                extracted_title and top_result_title and fuzzy_match_titles(extracted_title, top_result_title)
            ):
                try:
                    detailed_work = pyalex.Works()[work_id]
                    if detailed_work:
                        save_json(detailed_work, paper_folder, work_id)
                        truncated_path = os.path.relpath(
                            bib_file_path, start=os.getcwd()
                        )
                        logging.info(
                            f"Processed {work_id}: {top_result_title} ({truncated_path})"
                        )
                        update_bib_file(
                            bib_file_path, extracted_title, work_id, detailed_work.get("abstract", "")
                        )
                        log_processed_entry(work_id)
                        return True
                except (HTTPError, RequestException) as e:
                    logging.error(f"API error for {work_id}: {e}")
        log_no_match(bib_file_path, extracted_title)


def update_bib_file(bib_file_path, title, work_id, abstract):
    """Update .bib file with OpenAlex and abstract fields."""
    try:
        with open(bib_file_path, "r") as bib_file:
            bib_database = bibtexparser.load(bib_file)
        entry_updated = False
        for entry in bib_database.entries:
            if normalize_title(entry.get("title", "")) == title:
                if work_id:
                    entry["openalex"] = work_id
                if abstract:
                    entry["abstract"] = abstract
                entry_updated = True
                break
        if not entry_updated:
            logging.warning(f"Title '{title}' not found in {bib_file_path}")
        else:
            with open(bib_file_path, "w") as bib_file:
                bib_file.write(bibtexparser.dumps(bib_database))
            logging.info(f"Updated {bib_file_path} with OpenAlex ID: {work_id}")
    except Exception as e:
        logging.error(f"Error updating {bib_file_path}: {e}")


def process_bib_file(bib_file_path, openalex_folder, processed_entries):
    """Processes each .bib file to fetch OpenAlex data for all entries."""
    extracted_titles = extract_titles_from_bib(bib_file_path)
    for title in extracted_titles:
        try:
            results = pyalex.Works().search(title).get()
            process_search_results(bib_file_path, [title], results, openalex_folder, processed_entries)
        except (HTTPError, RequestException) as e:
            logging.error(f"Error searching for '{title}': {e}")


def process_conference_bibliographies():
    """Process each conference and its bibliography to fetch OpenAlex data."""
    processed_entries = load_processed_entries()

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
                    openalex_folder = create_subfolder_for_bib(bib_file, conf)
                    process_bib_file(bib_file_path, openalex_folder, processed_entries)
        else:
            logging.warning(f"Bibliography folder not found: {bib_folder}")


if __name__ == "__main__":
    process_conference_bibliographies()

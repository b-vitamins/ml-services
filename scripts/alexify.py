import os
import json
from pybtex.database import parse_file
import logging
from fuzzywuzzy import fuzz
import pyalex
import bibtexparser

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


def create_subfolder_for_bib(bib_file, conf):
    """Creates a subfolder in the openalex directory for each .bib file."""
    openalex_path = os.path.join(DATA_DIR, conf, "openalex")
    subfolder_name = os.path.splitext(bib_file)[0]
    subfolder_path = os.path.join(openalex_path, subfolder_name)
    if not os.path.exists(subfolder_path):
        os.makedirs(subfolder_path)
    return subfolder_path


def extract_title_from_bib(file_path):
    """Extracts titles from a .bib file."""
    try:
        bib_data = parse_file(file_path)
        return [
            entry.fields.get("title") for entry in bib_data.entries.values()
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
        fuzz.ratio(extracted_title.lower(), work_title.lower())
        > FUZZY_MATCH_THRESHOLD
    )


def log_no_match(bib_file_path, extracted_title, results):
    """Log when no matching title is found."""
    logging.info(
        f"No close match for {bib_file_path}. Actual title: '{extracted_title}'."
    )


def process_search_results(
    bib_file_path, extracted_titles, results, paper_folder
):
    """Process search results to find a matching title."""
    for extracted_title in extracted_titles:
        for work in results:
            top_result_title = work.get("title", "")
            if (
                extracted_title
                and top_result_title
                and fuzzy_match_titles(extracted_title, top_result_title)
            ):
                title = top_result_title
                full_id_link = work.get("id")
                if full_id_link:
                    work_id = work.get("id").rsplit("/", 1)[-1]
                    detailed_work = pyalex.Works()[work_id]
                    abstract = detailed_work["abstract"]
                    if detailed_work:
                        save_json(detailed_work, paper_folder, work_id)
                        truncated_path = os.path.relpath(
                            bib_file_path, start=os.getcwd()
                        )
                        logging.info(
                            f"Processed {work_id}: {title} ({truncated_path})"
                        )
                        update_bib_file(
                            bib_file_path, extracted_title, work_id, abstract
                        )
                        return True
                    else:
                        logging.error(
                            f"Failed to retrieve detailed data for {work_id}: {title}"
                        )
                else:
                    logging.error(
                        f"No OpenAlex ID found for matched work in {extracted_title}"
                    )
        log_no_match(bib_file_path, extracted_title, results)


def update_bib_file(bib_file_path, title, work_id, abstract):
    """Update .bib file with openalex and abstract field."""
    try:
        with open(bib_file_path, "r") as bib_file:
            bib_database = bibtexparser.load(bib_file)
        entry_updated = False
        for entry in bib_database.entries:
            if entry.get("title") == title:
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


def process_bib_file(bib_file_path, openalex_folder):
    """Processes each .bib file to fetch OpenAlex data for all entries."""
    extracted_titles = extract_title_from_bib(bib_file_path)
    for title in extracted_titles:
        results = pyalex.Works().search(title).get()
        process_search_results(bib_file_path, [title], results, openalex_folder)


def process_conference_bibliographies():
    """Process each conference and its bibliography to fetch OpenAlex data."""
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
                    process_bib_file(bib_file_path, openalex_folder)
        else:
            logging.warning(f"Bibliography folder not found: {bib_folder}")


if __name__ == "__main__":
    process_conference_bibliographies()

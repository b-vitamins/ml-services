import os
import json
import time
import logging
import requests
from requests.exceptions import HTTPError, RequestException

# Directory paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data")
DEP_DIR = os.path.join(SCRIPT_DIR, "..", "data", "deps")
TOPICS_DIR = os.path.join(SCRIPT_DIR, "..", "data", "topics")
LOG_DIR = os.path.join(SCRIPT_DIR, "..", "logs")

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "alexify.topics.log")),
        logging.StreamHandler(),
    ],
)


def save_json(data, folder, original_filename):
    """Saves data as JSON in the specified folder."""
    filename = f"{original_filename}.json"
    with open(os.path.join(folder, filename), "w") as json_file:
        json.dump(data, json_file)


def unique_topics(data_dir):
    """
    Scans all JSON files in the specified data directory and its subdirectories,
    extracting and returning a set of unique topics based on their OpenAlex IDs.

    Args:
    data_dir (str): The directory to search for JSON files containing topic data.

    Returns:
    set: A set of unique topics represented by their OpenAlex IDs.
    """
    unique_topics = set()
    for root, dirs, files in os.walk(data_dir):
        if os.path.abspath(root).startswith(os.path.abspath(DEP_DIR)):
            continue
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, "r") as f:
                        data = json.load(f)
                        topics = data.get("topics", [])
                        for topic in topics:
                            topic_id = topic.get("id", "")
                            if topic_id:
                                unique_topics.add(topic_id)
                except json.JSONDecodeError as e:
                    logging.error(
                        f"JSON decoding error in file {file_path}: {e}"
                    )
                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")
    return unique_topics


def process_topics(url_list, topic_dir, requests_per_day=100000):
    """Process URLs to fetch data from OpenAlex."""
    processed_count = 0
    start_time = time.time()

    for i, full_id_link in enumerate(url_list):
        if processed_count >= requests_per_day:
            logging.info(
                f"Reached daily quota of {requests_per_day} requests. Stopping processing."
            )
            break

        topic_id = full_id_link.rsplit("/", 1)[-1]
        file_path = os.path.join(topic_dir, f"{topic_id}.json")

        if os.path.exists(file_path):
            logging.info(
                f"File {topic_id}.json already exists. Skipping download."
            )
            continue

        try:
            url = f"https://api.openalex.org/topics/{topic_id}"
            response = requests.get(url)
            response.raise_for_status()  # Will raise an HTTPError for bad responses
            topic = response.json()
            save_json(topic, topic_dir, topic_id)
            processed_count += 1
            logging.info(f"Successfully processed and saved {topic_id}.json")

        except Exception as e:
            logging.error(f"Error fetching data for topic ID {topic_id}: {e}")

        if (i + 1) % 1000 == 0:
            elapsed_time = time.time() - start_time
            logging.info(
                f"Processed {i + 1}/{len(url_list)} URLs in {elapsed_time//3600}h {elapsed_time%3600//60}m."
            )


if __name__ == "__main__":
    os.makedirs(TOPICS_DIR, exist_ok=True)
    topic_list = unique_topics(DATA_DIR)
    logging.info(f"Found {len(topic_list)} unique topics.")
    process_topics(topic_list, TOPICS_DIR)

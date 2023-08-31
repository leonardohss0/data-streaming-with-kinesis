import requests
import json

from app_flow.resources.configs import WIKIPEDIA_STREAM_URL


def fetch_latest_wikipedia_data(last_timestamp):
    response = requests.get(WIKIPEDIA_STREAM_URL, stream=True)
    for line in response.iter_lines(decode_unicode=True):
        if line.startswith("data: "):
            json_data = line[len("data: "):]
            change = json.loads(json_data)
            change_timestamp = change.get("timestamp")
            if change_timestamp:
                change_timestamp = int(change_timestamp)  # Convert to integer
                if change_timestamp > last_timestamp:
                    yield json_data
                    last_timestamp = change_timestamp

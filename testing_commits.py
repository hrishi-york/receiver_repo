"""
Here in this file, testing commits with another git repo
"""

# data is returning but the branch is unknown, in this code

import os
import json
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
REPO_OWNER = "hrishi-york"
REPO_NAME = "remote_exmpl"
OUTPUT_FILE = "commits_remote_exmpl.json"

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

BASE_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}"


def fetch_all_commits():
    page = 1
    per_page = 100
    all_records = []

    while True:
        url = f"{BASE_URL}/commits"
        params = {"per_page": per_page, "page": page}

        response = requests.get(url, headers=HEADERS, params=params)

        if response.status_code != 200:
            raise Exception(response.text)

        commits = response.json()

        if not commits:
            break

        for c in commits:
            record = {
                "event_type": "commit",
                "commit_sha": c["sha"],
                "commit_timestamp": c["commit"]["author"]["date"],
                "repo_name": REPO_NAME,
                "author": c["commit"]["author"]["name"].strip("“”\"'"),
                "author_email": c["commit"]["author"]["email"],
                "branch": "unknown",
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }

            all_records.append(record)

        page += 1

    write_to_json(all_records)


def write_to_json(records):
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
    else:
        existing_data = []

    existing_data.extend(records)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, indent=2)


if __name__ == "__main__":
    fetch_all_commits()

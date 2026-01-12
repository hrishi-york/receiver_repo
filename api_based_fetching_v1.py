import os
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
REPO_OWNER = "hrishi-york"
REPO_NAME = "receiver_repo"

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28"
}

BASE_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}"

def fetch_all_commits():
    page = 1
    per_page = 100

    while True:
        url = f"{BASE_URL}/commits"
        params = {
            "per_page": per_page,
            "page": page
        }

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
                "ingested_at": datetime.now(timezone.utc).isoformat()
            }

            print(record)

        page += 1


if __name__ == "__main__":
    fetch_all_commits()

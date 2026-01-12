# Updated code (branch-aware) - no solution over    "branch": "unknown",

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


def fetch_branches():
    branches = []
    page = 1

    while True:
        url = f"{BASE_URL}/branches"
        params = {"per_page": 100, "page": page}

        r = requests.get(url, headers=HEADERS, params=params)
        if r.status_code != 200:
            raise Exception(r.text)

        data = r.json()
        if not data:
            break

        branches.extend([b["name"] for b in data])
        page += 1

    return branches


def commit_in_branch(branch, sha):
    url = f"{BASE_URL}/compare/{branch}...{sha}"
    r = requests.get(url, headers=HEADERS)

    if r.status_code != 200:
        return False

    result = r.json()

    return result.get("status") in ["ahead", "identical"]


def resolve_branch(sha, branches):
    for branch in branches:
        if commit_in_branch(branch, sha):
            return branch
    return "unknown"


def fetch_all_commits():
    branches = fetch_branches()
    page = 1

    while True:
        url = f"{BASE_URL}/commits"
        params = {"per_page": 100, "page": page}

        r = requests.get(url, headers=HEADERS, params=params)
        if r.status_code != 200:
            raise Exception(r.text)

        commits = r.json()
        if not commits:
            break

        for c in commits:
            sha = c["sha"]
            branch = resolve_branch(sha, branches)

            record = {
                "event_type": "commit",
                "commit_sha": sha,
                "commit_timestamp": c["commit"]["author"]["date"],
                "repo_name": REPO_NAME,
                "author": c["commit"]["author"]["name"].strip("“”\"'"),
                "author_email": c["commit"]["author"]["email"],
                "branch": branch,
                "ingested_at": datetime.now(timezone.utc).isoformat()
            }

            print(record)

        page += 1


if __name__ == "__main__":
    fetch_all_commits()

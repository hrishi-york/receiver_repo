"""
Branch-aware commit ingestion for multiple GitHub repositories
"""

import os
import json
import requests
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# CONFIG
# -----------------------------

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
if not ACCESS_TOKEN:
    raise RuntimeError("ACCESS_TOKEN is not set")

REPO_OWNER = "hrishi-york"

REPOSITORIES = [
    "remote_exmpl",
    "receiver_repo",
    "experimental_1",
]

OUTPUT_FILE = "github_commits_branch_aware.json"

BASE_URL = "https://api.github.com/repos"
PER_PAGE = 100
TIMEOUT = 10

HEADERS = {
    "Authorization": f"token {ACCESS_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

session = requests.Session()
session.headers.update(HEADERS)

# -----------------------------
# CORE HTTP HELPER
# -----------------------------


def github_get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    r = session.get(url, params=params, timeout=TIMEOUT)

    if r.status_code == 404:
        return None

    if r.status_code == 401:
        raise RuntimeError("Unauthorized: check token permissions")

    r.raise_for_status()
    return r.json()


# -----------------------------
# FETCH BRANCHES
# -----------------------------


def fetch_branches(repo_name: str) -> List[str]:
    data = github_get(f"{BASE_URL}/{REPO_OWNER}/{repo_name}/branches")

    if data is None:
        print(f"⚠️  Skipping repo '{repo_name}' (not found or no access)")
        return []

    branches: List[str] = [b["name"] for b in data]
    return branches


# -----------------------------
# FETCH COMMITS PER BRANCH
# -----------------------------


def fetch_commits_for_repo_and_branch(
    repo_name: str, branch: str
) -> List[Dict[str, Any]]:

    page = 1
    records: List[Dict[str, Any]] = []

    while True:
        commits = github_get(
            f"{BASE_URL}/{REPO_OWNER}/{repo_name}/commits",
            params={"sha": branch, "per_page": PER_PAGE, "page": page},
        )

        if not commits:
            break

        for c in commits:
            records.append(
                {
                    "event_type": "commit",
                    "commit_sha": c["sha"],
                    "commit_timestamp": c["commit"]["author"]["date"],
                    "repo_owner": REPO_OWNER,
                    "repo_name": repo_name,
                    "branch": branch,
                    "author": c["commit"]["author"]["name"].strip("“”\"'"),
                    "author_email": c["commit"]["author"]["email"],
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
            )

        page += 1

    return records


# -----------------------------
# WRITE JSON
# -----------------------------


def write_to_json(records: List[Dict[str, Any]]) -> None:
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)


# -----------------------------
# MAIN
# -----------------------------

if __name__ == "__main__":
    all_commits: List[Dict[str, Any]] = []

    for repo in REPOSITORIES:
        print(f"\nProcessing repository: {repo}")

        branches = fetch_branches(repo)
        if not branches:
            continue

        for branch in branches:
            print(f"  ↳ Fetching commits for branch: {branch}")
            branch_commits = fetch_commits_for_repo_and_branch(repo, branch)
            all_commits.extend(branch_commits)

    write_to_json(all_commits)

    print("\n-----------------------------------")
    print(f"Total records written: {len(all_commits)}")
    print(f"Output file: {OUTPUT_FILE}")

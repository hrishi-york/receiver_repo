"""
Incremental, append-safe commit ingestion using existing historical JSON
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

REPOSITORIES = ["remote_exmpl", "receiver_repo"]

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
# CORE HTTP
# -----------------------------


def github_get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    r = session.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


# -----------------------------
# LOAD EXISTING DATA
# -----------------------------


def load_existing_records() -> List[Dict[str, Any]]:
    if not os.path.exists(OUTPUT_FILE):
        return []
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


# -----------------------------
# BUILD CHECKPOINT FROM HISTORY
# -----------------------------


def build_checkpoint(records: List[Dict[str, Any]]) -> Dict[str, str]:
    checkpoint: Dict[str, str] = {}

    for r in records:
        key = f"{r['repo_name']}:{r['branch']}"
        ts = r["commit_timestamp"]

        if key not in checkpoint or ts > checkpoint[key]:
            checkpoint[key] = ts

    return checkpoint


def build_existing_sha_set(records: List[Dict[str, Any]]) -> set[str]:
    return {r["commit_sha"] for r in records}


# -----------------------------
# FETCH BRANCHES
# -----------------------------


def fetch_branches(repo_name: str) -> List[str]:
    data = github_get(f"{BASE_URL}/{REPO_OWNER}/{repo_name}/branches")
    return [b["name"] for b in data]


# -----------------------------
# FETCH INCREMENTAL COMMITS
# -----------------------------


def fetch_incremental_commits(
    repo_name: str, branch: str, since_ts: Optional[str]
) -> List[Dict[str, Any]]:

    page = 1
    records: List[Dict[str, Any]] = []

    while True:
        params = {"sha": branch, "per_page": PER_PAGE, "page": page}

        if since_ts:
            params["since"] = since_ts

        commits = github_get(
            f"{BASE_URL}/{REPO_OWNER}/{repo_name}/commits", params=params
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
# MAIN
# -----------------------------

if __name__ == "__main__":

    existing_records = load_existing_records()
    checkpoint = build_checkpoint(existing_records)
    existing_shas = build_existing_sha_set(existing_records)

    new_records: List[Dict[str, Any]] = []

    for repo in REPOSITORIES:
        print(f"\nProcessing repository: {repo}")
        branches = fetch_branches(repo)

        for branch in branches:
            key = f"{repo}:{branch}"
            since_ts = checkpoint.get(key)

            print(f"  ↳ Incremental fetch for {branch} | since={since_ts}")

            commits = fetch_incremental_commits(repo, branch, since_ts)

            for c in commits:
                if c["commit_sha"] in existing_shas:
                    continue
                new_records.append(c)
                existing_shas.add(c["commit_sha"])

    if new_records:
        all_records = existing_records + new_records
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(all_records, f, indent=2)

        print("\n-----------------------------------")
        print(f"New records appended: {len(new_records)}")
        print(f"Total records now: {len(all_records)}")
    else:
        print("\n-----------------------------------")
        print("No new commits found")

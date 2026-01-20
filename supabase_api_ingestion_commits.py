"""
Incremental, append-safe commit ingestion using GitHub API → Supabase
"""

import os
import json
import requests
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# -----------------------------
# CONFIG
# -----------------------------

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not ACCESS_TOKEN or not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing required environment variables")

# type narrowing
ACCESS_TOKEN = str(ACCESS_TOKEN)
SUPABASE_URL = str(SUPABASE_URL)
SUPABASE_SERVICE_ROLE_KEY = str(SUPABASE_SERVICE_ROLE_KEY)

REPO_OWNER = "hrishi-york"
REPOSITORIES = ["remote_exmpl", "receiver_repo", "experimental_1"]

BASE_URL = "https://api.github.com/repos"
PER_PAGE = 100
TIMEOUT = 10

# -----------------------------
# CLIENTS
# -----------------------------

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

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
# SUPABASE HELPERS
# -----------------------------


def load_existing_records():
    res = (
        supabase.table("commits_api")
        .select("commit_sha, repo_name, branch, commit_timestamp")
        .execute()
    )

    return res.data or []


def build_checkpoint(records):
    checkpoint = {}
    for r in records:
        key = f"{r['repo_name']}:{r['branch']}"
        ts = r["commit_timestamp"]

        if key not in checkpoint or ts > checkpoint[key]:
            checkpoint[key] = ts

    return checkpoint


def build_existing_sha_set(records):
    return {r["commit_sha"] for r in records}


# -----------------------------
# GITHUB FETCH
# -----------------------------


def fetch_branches(repo_name: str):
    data = github_get(f"{BASE_URL}/{REPO_OWNER}/{repo_name}/branches")
    return [b["name"] for b in data]


def fetch_incremental_commits(repo_name, branch, since_ts):
    page = 1
    records = []

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

    new_records = []

    for repo in REPOSITORIES:
        print(f"\nProcessing repo: {repo}")

        branches = fetch_branches(repo)

        for branch in branches:
            key = f"{repo}:{branch}"
            since_ts = checkpoint.get(key)

            print(f"  → {branch} | since={since_ts}")

            commits = fetch_incremental_commits(repo, branch, since_ts)

            for c in commits:
                if c["commit_sha"] in existing_shas:
                    continue

                new_records.append(c)
                existing_shas.add(c["commit_sha"])

    if new_records:
        supabase.table("commits_api").insert(new_records).execute()

        print("\n--------------------------------")
        print(f"Inserted {len(new_records)} new records")
    else:
        print("\n--------------------------------")
        print("No new commits found")

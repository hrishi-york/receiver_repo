"""
Incremental Pull Request Created ingestion
(creation-only, append-safe)
"""

import os
import json
import time
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
REPO_NAME = "remote_exmpl"

OUTPUT_FILE = "incremental_github_pr_created_events.json"
WATERMARK_FILE = "incremental_github_pr_created_watermark.json"

BASE_URL = "https://api.github.com"
PER_PAGE = 100
TIMEOUT = 10
MAX_RETRIES = 3
RETRY_SLEEP = 5

HEADERS = {
    "Authorization": f"token {ACCESS_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

session = requests.Session()
session.headers.update(HEADERS)

# -----------------------------
# RATE-LIMIT SAFE HTTP
# -----------------------------


def github_get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    for _ in range(MAX_RETRIES):
        r = session.get(url, params=params, timeout=TIMEOUT)

        if r.status_code == 403 and "rate limit" in r.text.lower():
            time.sleep(RETRY_SLEEP)
            continue

        r.raise_for_status()
        return r.json()

    raise RuntimeError(f"GitHub API failed after retries: {url}")


# -----------------------------
# WATERMARK
# -----------------------------


def load_watermark() -> Optional[str]:
    if not os.path.exists(WATERMARK_FILE):
        return None
    with open(WATERMARK_FILE, "r") as f:
        return json.load(f).get("last_updated_at")


def save_watermark(ts: str) -> None:
    with open(WATERMARK_FILE, "w") as f:
        json.dump({"last_updated_at": ts}, f, indent=2)


# -----------------------------
# EXISTING DATA (IDEMPOTENCY)
# -----------------------------


def load_existing_pr_ids() -> set[int]:
    if not os.path.exists(OUTPUT_FILE):
        return set()
    with open(OUTPUT_FILE, "r") as f:
        records = json.load(f)
    return {r["pr_id"] for r in records}


# -----------------------------
# FETCH PRs (INCREMENTAL)
# -----------------------------
# pulls API is used because:
# - created_at is authoritative
# - updated_at enables safe incremental scans


def fetch_pull_requests(since_ts: Optional[str]) -> List[Dict[str, Any]]:

    page = 1
    results: List[Dict[str, Any]] = []

    while True:
        prs = github_get(
            f"{BASE_URL}/repos/{REPO_OWNER}/{REPO_NAME}/pulls",
            params={
                "state": "all",
                "sort": "updated",
                "direction": "asc",
                "per_page": PER_PAGE,
                "page": page,
            },
        )

        if not prs:
            break

        for pr in prs:
            if since_ts and pr["updated_at"] <= since_ts:
                continue
            results.append(pr)

        page += 1

    return results


# -----------------------------
# MAIN
# -----------------------------

if __name__ == "__main__":

    last_watermark = load_watermark()
    existing_pr_ids = load_existing_pr_ids()

    new_records: List[Dict[str, Any]] = []
    max_updated_at = last_watermark

    pull_requests = fetch_pull_requests(last_watermark)

    for pr in pull_requests:
        pr_id = pr["id"]

        if pr_id in existing_pr_ids:
            continue

        new_records.append(
            {
                "pr_id": pr_id,
                "pr_number": pr["number"],
                "created_at": pr["created_at"],
                "author": pr["user"]["login"],
                "base_ref": pr["base"]["ref"],
                "head_ref": pr["head"]["ref"],
                "head_sha": pr["head"]["sha"],
                "repo_name": REPO_NAME,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
        )

        existing_pr_ids.add(pr_id)

        if not max_updated_at or pr["updated_at"] > max_updated_at:
            max_updated_at = pr["updated_at"]

    if new_records:
        if os.path.exists(OUTPUT_FILE):
            with open(OUTPUT_FILE, "r") as f:
                existing = json.load(f)
        else:
            existing = []

        with open(OUTPUT_FILE, "w") as f:
            json.dump(existing + new_records, f, indent=2)

    if max_updated_at:
        save_watermark(max_updated_at)

    print("-----------------------------------")
    print(f"New PR_CREATED events: {len(new_records)}")

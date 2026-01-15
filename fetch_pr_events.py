"""
Incremental GitHub Pull Request lifecycle ingestion
Tracks:
- PR_CREATED
- PR_MERGED (confirmed via events API)
"""

import os
import json
import time
import requests
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
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

OUTPUT_FILE = "github_pr_events.json"
WATERMARK_FILE = "github_pr_watermark.json"

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
# RATE-LIMIT AWARE HTTP
# -----------------------------


def github_get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    for attempt in range(MAX_RETRIES):
        r = session.get(url, params=params, timeout=TIMEOUT)

        if r.status_code == 403 and "rate limit" in r.text.lower():
            time.sleep(RETRY_SLEEP)
            continue

        r.raise_for_status()
        return r.json()

    raise RuntimeError(f"Failed after retries: {url}")


# -----------------------------
# WATERMARK HANDLING
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
# EXISTING EVENT LOAD (IDEMPOTENCY)
# -----------------------------


def load_existing_events() -> set[tuple]:
    if not os.path.exists(OUTPUT_FILE):
        return set()
    with open(OUTPUT_FILE, "r") as f:
        records = json.load(f)
    return {(r["pr_id"], r["event_type"]) for r in records}


# -----------------------------
# FETCH PULL REQUESTS (INCREMENTAL)
# -----------------------------
# pulls API is used because:
# - It exposes created_at, updated_at, merged_at
# - Supports sorting by updated_at for incremental scans


def fetch_pull_requests(since_ts: Optional[str]) -> List[Dict[str, Any]]:

    page = 1
    records: List[Dict[str, Any]] = []

    while True:
        pulls = github_get(
            f"{BASE_URL}/repos/{REPO_OWNER}/{REPO_NAME}/pulls",
            params={
                "state": "all",
                "sort": "updated",
                "direction": "asc",
                "per_page": PER_PAGE,
                "page": page,
            },
        )

        if not pulls:
            break

        for pr in pulls:
            if since_ts and pr["updated_at"] <= since_ts:
                continue
            records.append(pr)

        page += 1

    return records


# -----------------------------
# FETCH EVENTS (MERGE CONFIRMATION)
# -----------------------------
# events API is used because:
# - merged_at alone is not ground truth
# - PullRequestEvent confirms merge action explicitly


def fetch_repo_events() -> List[Dict[str, Any]]:
    return github_get(f"{BASE_URL}/repos/{REPO_OWNER}/{REPO_NAME}/events") or []


# -----------------------------
# MAIN
# -----------------------------

if __name__ == "__main__":

    last_watermark = load_watermark()
    existing_events = load_existing_events()
    repo_events = fetch_repo_events()

    pr_merge_events = {
        e["payload"]["pull_request"]["id"]: e
        for e in repo_events
        if e["type"] == "PullRequestEvent"
        and e["payload"]["action"] == "closed"
        and e["payload"].get("pull_request", {}).get("merged") is True
    }

    new_records: List[Dict[str, Any]] = []
    max_updated_at = last_watermark

    pull_requests = fetch_pull_requests(last_watermark)

    for pr in pull_requests:
        pr_id = pr["id"]

        # PR CREATED
        created_key = (pr_id, "PR_CREATED")
        if created_key not in existing_events:
            new_records.append(
                {
                    "pr_id": pr_id,
                    "pr_number": pr["number"],
                    "event_type": "PR_CREATED",
                    "event_timestamp": pr["created_at"],
                    "repo_name": REPO_NAME,
                    "base_branch": pr["base"]["ref"],
                    "head_sha": pr["head"]["sha"],
                    "author": pr["user"]["login"],
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            existing_events.add(created_key)

        # PR MERGED (confirmed)
        if pr_id in pr_merge_events:
            merged_key = (pr_id, "PR_MERGED")
            if merged_key not in existing_events:
                new_records.append(
                    {
                        "pr_id": pr_id,
                        "pr_number": pr["number"],
                        "event_type": "PR_MERGED",
                        "event_timestamp": pr["merged_at"],
                        "repo_name": REPO_NAME,
                        "base_branch": pr["base"]["ref"],
                        "head_sha": pr["head"]["sha"],
                        "author": pr["user"]["login"],
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                existing_events.add(merged_key)

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
    print(f"New PR lifecycle events: {len(new_records)}")

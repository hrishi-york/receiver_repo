"""
Confirmed Pull Request Merged ingestion
Authoritative, incremental, idempotent
"""

import os
import json
import time
import requests
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
if not ACCESS_TOKEN:
    raise RuntimeError("ACCESS_TOKEN is not set")

REPO_OWNER = "hrishi-york"
REPO_NAME = "remote_exmpl"

OUTPUT_FILE = "github_pr_merged_confirmed_events.json"
WATERMARK_FILE = "github_pr_merged_watermark.json"

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

# -------------------------------------------------
# RATE-LIMIT SAFE HTTP
# -------------------------------------------------


def github_get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    for _ in range(MAX_RETRIES):
        r = session.get(url, params=params, timeout=TIMEOUT)

        if r.status_code == 403 and "rate limit" in r.text.lower():
            time.sleep(RETRY_SLEEP)
            continue

        r.raise_for_status()
        return r.json()

    raise RuntimeError(f"GitHub API failed after retries: {url}")


# -------------------------------------------------
# WATERMARK (updated_at)
# -------------------------------------------------


def load_watermark() -> Optional[str]:
    if not os.path.exists(WATERMARK_FILE):
        return None
    with open(WATERMARK_FILE, "r") as f:
        return json.load(f).get("last_processed_updated_at")


def save_watermark(ts: str) -> None:
    with open(WATERMARK_FILE, "w") as f:
        json.dump({"last_processed_updated_at": ts}, f, indent=2)


# -------------------------------------------------
# IDEMPOTENCY
# -------------------------------------------------


def load_existing_pr_ids() -> set[int]:
    if not os.path.exists(OUTPUT_FILE):
        return set()
    with open(OUTPUT_FILE, "r") as f:
        records = json.load(f)
    return {r["pr_id"] for r in records}


# -------------------------------------------------
# FETCH PULL REQUESTS (HISTORICAL TRUTH)
# Gap 5 enforced here
# -------------------------------------------------


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


# -------------------------------------------------
# MAIN
# -------------------------------------------------

if __name__ == "__main__":

    last_watermark = load_watermark()
    emitted_pr_ids = load_existing_pr_ids()

    new_events: List[Dict[str, Any]] = []
    max_seen_updated_at = last_watermark

    pull_requests = fetch_pull_requests(last_watermark)

    for pr in pull_requests:

        # -------------------------------------------------
        # Gap 1 — Closed ≠ Merged
        # -------------------------------------------------
        if pr["merged_at"] is None:
            continue

        pr_id = pr["id"]

        # -------------------------------------------------
        # Gap 3 — Idempotency
        # -------------------------------------------------
        if pr_id in emitted_pr_ids:
            continue

        # -------------------------------------------------
        # Gap 2 — Squash / Rebase Safe
        # (No commit inference, no branch checks)
        # -------------------------------------------------

        new_events.append(
            {
                "pr_id": pr_id,
                "pr_number": pr["number"],
                "merged_at": pr["merged_at"],
                "merge_commit_sha": pr["merge_commit_sha"],
                "base_ref": pr["base"]["ref"],
                "repo_name": REPO_NAME,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
        )

        emitted_pr_ids.add(pr_id)

        if not max_seen_updated_at or pr["updated_at"] > max_seen_updated_at:
            max_seen_updated_at = pr["updated_at"]

    # -------------------------------------------------
    # PERSIST RESULTS
    # -------------------------------------------------

    if new_events:
        if os.path.exists(OUTPUT_FILE):
            with open(OUTPUT_FILE, "r") as f:
                existing = json.load(f)
        else:
            existing = []

        with open(OUTPUT_FILE, "w") as f:
            json.dump(existing + new_events, f, indent=2)

    if max_seen_updated_at:
        save_watermark(max_seen_updated_at)

    print("-----------------------------------")
    print(f"Confirmed PR_MERGED events emitted: {len(new_events)}")

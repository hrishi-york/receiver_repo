"""
Incremental, append-safe PR ingestion (merged PRs only)
Supports:
- Multi-repo
- Historical backfill
- Incremental updates
- Merge type detection
"""

import os
import json
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

REPOSITORIES = [
    "remote_exmpl",
    "Netlify_Deployment",
    "experimental_1",
]

OUTPUT_FILE = "github_pr_merged_events.json"

BASE_URL = "https://api.github.com"
PER_PAGE = 100
TIMEOUT = 10

HEADERS = {
    "Authorization": f"token {ACCESS_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

session = requests.Session()
session.headers.update(HEADERS)

# -------------------------------------------------
# HTTP
# -------------------------------------------------


def github_get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    r = session.get(url, params=params, timeout=TIMEOUT)

    if r.status_code == 401:
        raise RuntimeError("Unauthorized")

    if r.status_code == 404:
        return None

    r.raise_for_status()
    return r.json()


# -------------------------------------------------
# LOAD EXISTING DATA
# -------------------------------------------------


def load_existing_records() -> List[Dict[str, Any]]:
    if not os.path.exists(OUTPUT_FILE):
        return []
    with open(OUTPUT_FILE, "r") as f:
        return json.load(f)


def build_checkpoint(records: List[Dict[str, Any]]) -> Dict[str, str]:
    checkpoint = {}

    for r in records:
        # Only process PR records
        if "repo_name" not in r:
            continue

        # Skip records that don't have updated_at (old deployment records)
        updated_at = r.get("updated_at")
        if not updated_at:
            continue

        repo = r["repo_name"]

        if repo not in checkpoint or updated_at > checkpoint[repo]:
            checkpoint[repo] = updated_at

    return checkpoint


def existing_pr_ids(records: List[Dict[str, Any]]) -> set:
    return {r["pr_id"] for r in records}


# -------------------------------------------------
# MERGE TYPE INFERENCE
# -------------------------------------------------


def infer_merge_type(pr: Dict[str, Any]) -> str:
    if pr.get("merged_at") and pr.get("merge_commit_sha"):
        if pr.get("commits", 0) > 1:
            return "merge"
        return "squash"

    if pr.get("merged_at"):
        return "rebase"

    return "unknown"


# -------------------------------------------------
# FETCH PRs
# -------------------------------------------------


def fetch_pull_requests(repo: str, since_ts: Optional[str]) -> List[Dict[str, Any]]:
    page = 1
    results = []

    while True:
        prs = github_get(
            f"{BASE_URL}/repos/{REPO_OWNER}/{repo}/pulls",
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

    existing = load_existing_records()
    checkpoint = build_checkpoint(existing)
    seen_pr_ids = existing_pr_ids(existing)

    new_events: List[Dict[str, Any]] = []

    for repo in REPOSITORIES:
        print(f"\nFetching PRs for repo: {repo}")

        since_ts = checkpoint.get(repo)
        prs = fetch_pull_requests(repo, since_ts)

        for pr in prs:
            if not pr.get("merged_at"):
                continue

            if pr["id"] in seen_pr_ids:
                continue

            record = {
                "pr_id": pr["id"],
                "pr_number": pr["number"],
                "source_branch": pr["head"]["ref"],
                "target_branch": pr["base"]["ref"],
                "merge_type": infer_merge_type(pr),
                "merge_commit_sha": pr["merge_commit_sha"],
                "repo_owner": REPO_OWNER,
                "repo_name": repo,
                "created_at": pr["created_at"],
                "updated_at": pr["updated_at"],
                "merged_at": pr["merged_at"],
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }

            new_events.append(record)
            seen_pr_ids.add(pr["id"])

    if new_events:
        all_records = existing + new_events
        with open(OUTPUT_FILE, "w") as f:
            json.dump(all_records, f, indent=2)

        print("\n-----------------------------------")
        print(f"New PRs ingested: {len(new_events)}")
        print(f"Total PRs stored: {len(all_records)}")
    else:
        print("\n-----------------------------------")
        print("No new PRs found")

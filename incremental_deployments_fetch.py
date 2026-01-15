"""
Incremental, append-safe deployment ingestion using existing historical JSON
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
    "Netlify_Deployment",
    "experimental_1",
]

OUTPUT_FILE = "github_deployments_multi_repo.json"

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

    if r.status_code == 404:
        return None

    if r.status_code == 401:
        raise RuntimeError("Unauthorized: check token permissions")

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


def build_checkpoint(records: List[Dict[str, Any]]) -> Dict[str, int]:
    checkpoint: Dict[str, int] = {}

    for r in records:
        repo = r["repo_name"]
        deployment_id = r["deployment_id"]

        if repo not in checkpoint or deployment_id > checkpoint[repo]:
            checkpoint[repo] = deployment_id

    return checkpoint


def build_existing_deployment_id_set(records: List[Dict[str, Any]]) -> set[int]:
    return {r["deployment_id"] for r in records}


# -----------------------------
# FETCH INCREMENTAL DEPLOYMENTS
# -----------------------------


def fetch_incremental_deployments_for_repo(
    repo_name: str, last_deployment_id: Optional[int]
) -> List[Dict[str, Any]]:

    page = 1
    records: List[Dict[str, Any]] = []

    while True:
        deployments = github_get(
            f"{BASE_URL}/{REPO_OWNER}/{repo_name}/deployments",
            params={"per_page": PER_PAGE, "page": page},
        )

        if not deployments:
            break

        for d in deployments:
            if last_deployment_id and d["id"] <= last_deployment_id:
                return records

            statuses = (
                github_get(
                    f"{BASE_URL}/{REPO_OWNER}/{repo_name}/deployments/{d['id']}/statuses"
                )
                or []
            )

            latest_status = statuses[0] if statuses else {}

            records.append(
                {
                    "event_type": "deployment",
                    "deployment_id": d["id"],
                    "repo_owner": REPO_OWNER,
                    "repo_name": repo_name,
                    "environment": d["environment"],
                    "ref": d["ref"],
                    "commit_sha": d["sha"],
                    "deployment_created_at": d["created_at"],
                    "status": latest_status.get("state"),
                    "status_created_at": latest_status.get("created_at"),
                    "performed_via": (d.get("performed_via_github_app") or {}).get(
                        "slug"
                    ),
                    "creator": d["creator"]["login"],
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
    existing_ids = build_existing_deployment_id_set(existing_records)

    new_records: List[Dict[str, Any]] = []

    for repo in REPOSITORIES:
        last_id = checkpoint.get(repo)
        print(f"\nIncremental fetch for repo: {repo} | last_id={last_id}")

        deployments = fetch_incremental_deployments_for_repo(repo, last_id)

        for d in deployments:
            if d["deployment_id"] in existing_ids:
                continue
            new_records.append(d)
            existing_ids.add(d["deployment_id"])

    if new_records:
        all_records = existing_records + new_records
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(all_records, f, indent=2)

        print("\n-----------------------------------")
        print(f"New deployments appended: {len(new_records)}")
        print(f"Total deployments now: {len(all_records)}")
    else:
        print("\n-----------------------------------")
        print("No new deployments found")

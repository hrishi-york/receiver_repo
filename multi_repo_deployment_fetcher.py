"""
Fetch deployments from multiple GitHub repositories
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

# 👇 MULTIPLE REPOS HERE
REPOSITORIES = [
    "remote_exmpl",
    "Netlify_Deployment",
]

OUTPUT_FILE = "github_deployments_multi_repo.json"

BASE_URL = "https://api.github.com/repos"
PER_PAGE = 100
TIMEOUT = 10

HEADERS = {
    "Authorization": f"token {ACCESS_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28"
}

session = requests.Session()
session.headers.update(HEADERS)

# -----------------------------
# CORE HTTP HELPER
# -----------------------------

def github_get(
    url: str,
    params: Optional[Dict[str, Any]] = None
) -> Any:
    r = session.get(url, params=params, timeout=TIMEOUT)

    if r.status_code == 404:
        return None

    if r.status_code == 401:
        raise RuntimeError("Unauthorized: check token permissions")

    r.raise_for_status()
    return r.json()

# -----------------------------
# FETCH DEPLOYMENTS (PER REPO)
# -----------------------------

def fetch_deployments_for_repo(repo_name: str) -> List[Dict[str, Any]]:
    page = 1
    records: List[Dict[str, Any]] = []

    while True:
        deployments = github_get(
            f"{BASE_URL}/{REPO_OWNER}/{repo_name}/deployments",
            params={"per_page": PER_PAGE, "page": page}
        )

        if not deployments:
            break

        for d in deployments:
            statuses = github_get(
                f"{BASE_URL}/{REPO_OWNER}/{repo_name}/deployments/{d['id']}/statuses"
            ) or []

            latest_status = statuses[0] if statuses else {}

            records.append({
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
                "performed_via": (
                    d.get("performed_via_github_app") or {}
                ).get("slug"),
                "creator": d["creator"]["login"],
                "ingested_at": datetime.now(timezone.utc).isoformat()
            })

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
    all_deployments: List[Dict[str, Any]] = []

    for repo in REPOSITORIES:
        print(f"Fetching deployments for repo: {repo}")
        repo_deployments = fetch_deployments_for_repo(repo)
        all_deployments.extend(repo_deployments)

    write_to_json(all_deployments)

    print("\n-----------------------------------")
    print(f"Total deployments written: {len(all_deployments)}")
    print(f"Output file: {OUTPUT_FILE}")

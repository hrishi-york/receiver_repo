"""
Incremental, append-safe deployment ingestion using GitHub API → Supabase
"""

import os
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

ACCESS_TOKEN = str(ACCESS_TOKEN)
SUPABASE_URL = str(SUPABASE_URL)
SUPABASE_SERVICE_ROLE_KEY = str(SUPABASE_SERVICE_ROLE_KEY)

REPO_OWNER = "hrishi-york"

REPOSITORIES = [
    "remote_exmpl",
    "Netlify_Deployment",
    "experimental_1",
]

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

    if r.status_code == 404:
        return None

    if r.status_code == 401:
        raise RuntimeError("Unauthorized: check token permissions")

    r.raise_for_status()
    return r.json()


# -----------------------------
# SUPABASE HELPERS
# -----------------------------


def load_existing_records():
    res = supabase.table("deployments_api").select("deployment_id, repo_name").execute()

    return res.data or []


def build_checkpoint(records):
    checkpoint = {}

    for r in records:
        repo = r["repo_name"]
        dep_id = r["deployment_id"]

        if repo not in checkpoint or dep_id > checkpoint[repo]:
            checkpoint[repo] = dep_id

    return checkpoint


def build_existing_id_set(records):
    return {r["deployment_id"] for r in records}


# -----------------------------
# FETCH DEPLOYMENTS
# -----------------------------


def fetch_incremental_deployments_for_repo(repo_name, last_deployment_id):
    page = 1
    records = []

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
    existing_ids = build_existing_id_set(existing_records)

    new_records = []

    for repo in REPOSITORIES:
        last_id = checkpoint.get(repo)
        print(f"\nProcessing repo: {repo} | last_id={last_id}")

        deployments = fetch_incremental_deployments_for_repo(repo, last_id)

        for d in deployments:
            if d["deployment_id"] in existing_ids:
                continue

            new_records.append(d)
            existing_ids.add(d["deployment_id"])

    if new_records:
        supabase.table("deployments_api").insert(new_records).execute()

        print("\n--------------------------------")
        print(f"Inserted {len(new_records)} new deployments")
    else:
        print("\n--------------------------------")
        print("No new deployments found")

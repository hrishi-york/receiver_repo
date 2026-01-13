import os
import sys
import requests
import json
from typing import Dict, List, Any, Optional

# -----------------------------
# CONFIG
# -----------------------------

OWNER = "hrishi-york"
REPO = "Netlify_Deployment"

BASE_URL = "https://api.github.com"
PER_PAGE = 100
TIMEOUT = 10

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

if not GITHUB_TOKEN:
    print("ERROR: GITHUB_TOKEN environment variable is not set")
    sys.exit(1)

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28"
}

# -----------------------------
# SESSION
# -----------------------------

session = requests.Session()
session.headers.update(HEADERS)

# -----------------------------
# CORE REQUEST HELPER
# -----------------------------

def github_get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    r = session.get(url, params=params, timeout=TIMEOUT)

    if r.status_code == 401:
        print("401 Unauthorized")
        print(r.text[:200])
        sys.exit(1)

    r.raise_for_status()
    return r.json()

# -----------------------------
# AUTH VALIDATION
# -----------------------------

def validate_auth() -> None:
    user_response: Dict[str, Any] = github_get(f"{BASE_URL}/user")
    print(f"Authenticated as: {user_response.get('login')}")

# -----------------------------
# DEPLOYMENTS
# -----------------------------

def fetch_deployments_page(page: int) -> List[Dict[str, Any]]:
    deployments: List[Dict[str, Any]] = github_get(
        f"{BASE_URL}/repos/{OWNER}/{REPO}/deployments",
        params={"per_page": PER_PAGE, "page": page}
    )
    return deployments

def fetch_deployment_statuses(deployment_id: int) -> List[Dict[str, Any]]:
    statuses: List[Dict[str, Any]] = github_get(
        f"{BASE_URL}/repos/{OWNER}/{REPO}/deployments/{deployment_id}/statuses"
    )
    return statuses

# -----------------------------
# NORMALIZATION
# -----------------------------

def normalize_deployment(
    deployment: Dict[str, Any],
    statuses: List[Dict[str, Any]]
) -> Dict[str, Any]:

    latest_status = statuses[0] if statuses else {}

    return {
        "deployment_id": deployment["id"],
        "repo": f"{OWNER}/{REPO}",
        "commit_sha": deployment["sha"],
        "ref": deployment["ref"],
        "environment": deployment["environment"],
        "created_at": deployment["created_at"],
        "status": latest_status.get("state"),
        "status_created_at": latest_status.get("created_at"),
        "performed_via": (
            deployment.get("performed_via_github_app") or {}
        ).get("slug"),
        "creator": deployment["creator"]["login"]
    }

# -----------------------------
# FETCH ALL DEPLOYMENTS
# -----------------------------

def fetch_all_deployments() -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    page = 1

    while True:
        deployments = fetch_deployments_page(page)
        if not deployments:
            break

        for d in deployments:
            statuses = fetch_deployment_statuses(d["id"])
            results.append(normalize_deployment(d, statuses))

        page += 1

    return results

def write_deployments_to_json(
    deployments: list[dict],
    file_path: str = "deployments_netlify.json"
) -> None:
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(deployments, f, indent=2, ensure_ascii=False)


# -----------------------------
# MAIN
# -----------------------------

if __name__ == "__main__":
    validate_auth()

    print("Fetching deployment history...")
    deployments = fetch_all_deployments()

    print(f"Total deployments fetched: {len(deployments)}")

    write_deployments_to_json(deployments)

    print("Deployments written to deployments.json")

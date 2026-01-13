import os
import sys
import requests

# -----------------------------
# CONFIG
# -----------------------------

OWNER = "hrishi-york"
REPO = "remote_exmpl"

BASE_URL = "https://api.github.com"
PER_PAGE = 100

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
# AUTH VALIDATION
# -----------------------------

def validate_auth():
    r = requests.get(f"{BASE_URL}/user", headers=HEADERS)
    if r.status_code != 200:
        print("AUTH FAILED")
        print("Status:", r.status_code)
        print("Body:", r.text[:200])
        sys.exit(1)

    user = r.json().get("login")
    print(f"Authenticated as: {user}")

# -----------------------------
# API HELPERS
# -----------------------------

def fetch_deployments_page(page: int):
    r = requests.get(
        f"{BASE_URL}/repos/{OWNER}/{REPO}/deployments",
        headers=HEADERS,
        params={"per_page": PER_PAGE, "page": page}
    )
    if r.status_code == 401:
        print("401 Unauthorized while fetching deployments")
        print(r.text[:200])
        sys.exit(1)

    r.raise_for_status()
    return r.json()


def fetch_deployment_statuses(deployment_id: int):
    r = requests.get(
        f"{BASE_URL}/repos/{OWNER}/{REPO}/deployments/{deployment_id}/statuses",
        headers=HEADERS
    )
    r.raise_for_status()
    return r.json()

# -----------------------------
# NORMALIZATION
# -----------------------------

def normalize_deployment(deployment, statuses):
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

def fetch_all_deployments():
    results = []
    page = 1

    while True:
        deployments = fetch_deployments_page(page)
        if not deployments:
            break

        for d in deployments:
            statuses = fetch_deployment_statuses(d["id"])
            record = normalize_deployment(d, statuses)
            results.append(record)

        page += 1

    return results

# -----------------------------
# MAIN
# -----------------------------

if __name__ == "__main__":
    validate_auth()

    print("Fetching deployment history...")
    deployments = fetch_all_deployments()

    print(f"\nTotal deployments fetched: {len(deployments)}\n")

    for d in deployments:
        print(d)

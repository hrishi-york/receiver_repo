# compare api

import os
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
REPO_OWNER = "hrishi-york"
REPO_NAME = "receiver_repo"

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28"
}

BASE_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}"

def get_branch_via_comparison(sha, branches):
    """
    Fallback: Checks if the commit exists in the history of known branches.
    Returns the first branch that contains the commit.
    """
    for branch in branches:
        # We check if 'sha' is behind 'branch'. 
        # If status is 'behind' or 'identical', it means the commit is part of this branch.
        url = f"{BASE_URL}/compare/{branch}...{sha}"
        r = requests.get(url, headers=HEADERS)
        if r.status_code == 200:
            status = r.json().get("status")
            if status in ["behind", "identical"]:
                return branch
    return None

def resolve_branch(sha, all_repo_branches):
    # 1. Try Pull Requests (Handles forks and merged feature branches)
    pulls_url = f"{BASE_URL}/commits/{sha}/pulls"
    r_pulls = requests.get(pulls_url, headers=HEADERS)
    if r_pulls.status_code == 200:
        pulls = r_pulls.json()
        if pulls:
            pr = pulls[0]
            # Check if the PR came from a fork
            is_fork = pr.get("head", {}).get("repo", {}).get("full_name") != f"{REPO_OWNER}/{REPO_NAME}"
            branch_name = pr.get("head", {}).get("ref")
            return f"fork:{branch_name}" if is_fork else branch_name

    # 2. Check if it's the tip (HEAD) of any branch
    head_url = f"{BASE_URL}/commits/{sha}/branches-where-head"
    r_head = requests.get(head_url, headers=HEADERS)
    if r_head.status_code == 200:
        heads = r_head.json()
        if heads:
            return heads[0].get("name")

    # 3. Last Resort: Scan branch histories
    # This prevents the 'unknown' error if the commit is buried in history
    found_in_history = get_branch_via_comparison(sha, all_repo_branches)
    if found_in_history:
        return found_in_history

    return "unassociated_commit"

def fetch_branches():
    """Fetches list of all branch names in the repo for the comparison fallback."""
    url = f"{BASE_URL}/branches"
    r = requests.get(url, headers=HEADERS)
    if r.status_code == 200:
        return [b["name"] for b in r.json()]
    return []

def fetch_all_commits():
    # Cache branches once to use in the fallback logic
    all_repo_branches = fetch_branches()
    page = 1

    while True:
        url = f"{BASE_URL}/commits"
        params = {"per_page": 100, "page": page}
        r = requests.get(url, headers=HEADERS, params=params)
        
        if r.status_code != 200 or not r.json():
            break

        for c in r.json():
            sha = c["sha"]
            branch = resolve_branch(sha, all_repo_branches)

            record = {
                "event_type": "commit",
                "commit_sha": sha,
                "commit_timestamp": c["commit"]["author"]["date"],
                "repo_name": REPO_NAME,
                "author": c["commit"]["author"]["name"],
                "author_email": c["commit"]["author"]["email"],
                "branch": branch, # Now returns branch, fork:branch, or unassociated
                "ingested_at": datetime.now(timezone.utc).isoformat()
            }
            print(record)
        page += 1

if __name__ == "__main__":
    fetch_all_commits()
'''
I have replaced the inefficient branch-looping logic with the Pull Request and Branches-where-head endpoints. This approach is much more accurate for identifying where a commit originated.

'''



import os
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
import time

load_dotenv()

# Configuration
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
REPO_OWNER = "hrishi-york"
REPO_NAME = "receiver_repo"

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28"
}

BASE_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}"

def resolve_branch(sha):
    """
    Attempts to find the branch name for a given commit SHA.
    1. Checks for Pull Requests associated with the commit (best for merged features).
    2. Checks if the commit is currently the 'head' (tip) of any branch.
    """
    # Step 1: Check Pull Requests (Most reliable for historical commits)
    pulls_url = f"{BASE_URL}/commits/{sha}/pulls"
    try:
        r_pulls = requests.get(pulls_url, headers=HEADERS)
        if r_pulls.status_code == 200:
            pulls = r_pulls.json()
            if pulls:
                # 'head' 'ref' is the source branch name from the PR
                return pulls[0].get("head", {}).get("ref", "unknown")
    except Exception as e:
        print(f"Error fetching PRs for {sha}: {e}")

    # Step 2: Check if this commit is the head of any branch (Direct pushes)
    head_url = f"{BASE_URL}/commits/{sha}/branches-where-head"
    try:
        r_head = requests.get(head_url, headers=HEADERS)
        if r_head.status_code == 200:
            branches = r_head.json()
            if branches:
                return branches[0].get("name", "unknown")
    except Exception as e:
        print(f"Error fetching head branch for {sha}: {e}")

    # Step 3: Default fallback
    return "main"

def fetch_all_commits():
    """
    Iterates through all commits in the repository and resolves their branch.
    """
    page = 1
    
    print(f"Starting ingestion for {REPO_OWNER}/{REPO_NAME}...\n")

    while True:
        url = f"{BASE_URL}/commits"
        params = {"per_page": 100, "page": page}

        r = requests.get(url, headers=HEADERS, params=params)
        
        if r.status_code != 200:
            print(f"Failed to fetch commits: {r.status_code} - {r.text}")
            break

        commits = r.json()
        if not commits:
            break

        for c in commits:
            sha = c["sha"]
            
            # Resolve the branch name using the new logic
            branch = resolve_branch(sha)

            record = {
                "event_type": "commit",
                "commit_sha": sha,
                "commit_timestamp": c["commit"]["author"]["date"],
                "repo_name": REPO_NAME,
                "author": c["commit"]["author"]["name"].strip("“”\"'"),
                "author_email": c["commit"]["author"]["email"],
                "branch": branch,
                "ingested_at": datetime.now(timezone.utc).isoformat()
            }

            print(record)
            
            # Optional: Sleep briefly to avoid hitting secondary rate limits 
            # if the repo has thousands of commits.
            # time.sleep(0.1)

        page += 1

if __name__ == "__main__":
    if not ACCESS_TOKEN:
        print("Error: ACCESS_TOKEN not found in environment variables.")
    else:
        fetch_all_commits()


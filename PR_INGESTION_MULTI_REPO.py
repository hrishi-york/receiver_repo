import os
import json
import requests
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

# -------------------------------------------------
# Configuration
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
GRAPHQL_URL = "https://api.github.com/graphql"

PER_PAGE = 100
TIMEOUT = 10

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/vnd.github+json",
}

session = requests.Session()
session.headers.update(HEADERS)

# -------------------------------------------------
# REST + GraphQL helpers
# -------------------------------------------------


def github_get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    r = session.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def github_graphql(query: str, variables: dict) -> dict:
    r = session.post(
        GRAPHQL_URL,
        json={"query": query, "variables": variables},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.json()


# -------------------------------------------------
# Storage helpers
# -------------------------------------------------


def load_existing_records() -> List[Dict[str, Any]]:
    if not os.path.exists(OUTPUT_FILE):
        return []
    with open(OUTPUT_FILE, "r") as f:
        return json.load(f)


def build_checkpoint(records: List[Dict[str, Any]]) -> Dict[str, str]:
    checkpoint = {}
    for r in records:
        if "repo_name" in r and r.get("updated_at"):
            repo = r["repo_name"]
            if repo not in checkpoint or r["updated_at"] > checkpoint[repo]:
                checkpoint[repo] = r["updated_at"]
    return checkpoint


def existing_pr_ids(records: List[Dict[str, Any]]) -> set:
    return {r["pr_id"] for r in records if "pr_id" in r}


# -------------------------------------------------
# GraphQL query
# -------------------------------------------------

PR_GRAPHQL_QUERY = """
query ($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      id
      number
      createdAt
      updatedAt
      mergedAt

      headRefName
      baseRefName
      baseRefOid

      commits(first: 100) {
        nodes {
          commit {
            oid
            messageHeadline
            authoredDate
            committedDate
            parents(first: 5) {
              totalCount
            }
          }
        }
      }

      mergeCommit {
        oid
        parents(first: 5) {
          totalCount
        }
      }
    }
  }
}
"""


# -------------------------------------------------
# Merge type detection
# -------------------------------------------------


def infer_merge_type_graphql(pr_data: Dict[str, Any]) -> str:
    pr = pr_data["data"]["repository"]["pullRequest"]

    commits = pr["commits"]["nodes"]
    merge_commit = pr.get("mergeCommit")

    # Merge commit → multiple parents
    if merge_commit and merge_commit["parents"]["totalCount"] >= 2:
        return "merge"

    # Squash → single commit with PR reference
    if len(commits) == 1:
        title = commits[0]["commit"]["messageHeadline"].lower()
        if "pr #" in title or "pull request" in title or "#" in title:
            return "squash"

    # Rebase → multiple commits with differing timestamps
    commit_times = {
        (c["commit"]["authoredDate"], c["commit"]["committedDate"]) for c in commits
    }

    if len(commits) > 1 and len(commit_times) > 1:
        return "rebase"

    return "unknown"


# -------------------------------------------------
# REST PR fetch
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
# Main execution
# -------------------------------------------------

if __name__ == "__main__":

    existing = load_existing_records()
    checkpoint = build_checkpoint(existing)
    seen_pr_ids = existing_pr_ids(existing)

    new_events: List[Dict[str, Any]] = []

    for repo in REPOSITORIES:
        print(f"Fetching PRs for {repo}")

        since_ts = checkpoint.get(repo)
        prs = fetch_pull_requests(repo, since_ts)

        for pr in prs:
            if not pr.get("merged_at"):
                continue

            if pr["id"] in seen_pr_ids:
                continue

            gql_data = github_graphql(
                PR_GRAPHQL_QUERY,
                {
                    "owner": REPO_OWNER,
                    "repo": repo,
                    "number": pr["number"],
                },
            )

            pr_node = gql_data["data"]["repository"]["pullRequest"]
            commits = pr_node["commits"]["nodes"]
            merge_commit = pr_node.get("mergeCommit")

            record = {
                "pr_id": pr_node["id"],
                "pr_number": pr_node["number"],
                "source_branch": pr_node["headRefName"],
                "source_sha": commits[-1]["commit"]["oid"] if commits else None,
                "target_branch": pr_node["baseRefName"],
                "base_sha": pr_node["baseRefOid"],
                "merge_type": infer_merge_type_graphql(gql_data),
                "merge_commit_sha": (merge_commit["oid"] if merge_commit else None),
                "repo_owner": REPO_OWNER,
                "repo_name": repo,
                "created_at": pr_node["createdAt"],
                "updated_at": pr_node["updatedAt"],
                "merged_at": pr_node["mergedAt"],
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }

            new_events.append(record)
            seen_pr_ids.add(pr["id"])

    if new_events:
        all_records = existing + new_events
        with open(OUTPUT_FILE, "w") as f:
            json.dump(all_records, f, indent=2)

        print(f"New PRs ingested: {len(new_events)}")
        print(f"Total PRs stored: {len(all_records)}")
    else:
        print("No new PRs found")

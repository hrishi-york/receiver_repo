import os
import requests
from dotenv import load_dotenv
load_dotenv()

# --- Configuration ---
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
REPO_OWNER = "hrishi-york"
REPO_NAME = "receiver_repo"

def fetch_repo_events():
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/events"
    
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        events = response.json()
        for event in events[:5]:  # Displaying the 5 most recent events
            print(f"ID: {event['id']}")
            print(f"Type: {event['type']}")
            print(f"Actor: {event['actor']['login']}")
            print(f"Created At: {event['created_at']}")
            print("-" * 20)
    else:
        print(f"Error: {response.status_code} - {response.text}")

if __name__ == "__main__":
    fetch_repo_events()
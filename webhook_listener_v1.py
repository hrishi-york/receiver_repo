import hmac
import hashlib
import json
from datetime import datetime
from flask import Flask, request, abort, jsonify

app = Flask(__name__)

GITHUB_SECRET = b"webshook_url@8989"
EVENT_LOG_FILE = "github_commit_events.ndjson"


def verify_signature(req):
    signature = req.headers.get("X-Hub-Signature-256")
    if not signature:
        abort(401)

    digest = hmac.new(
        GITHUB_SECRET,
        msg=req.data,
        digestmod=hashlib.sha256
    ).hexdigest()

    expected = f"sha256={digest}"

    if not hmac.compare_digest(expected, signature):
        abort(401)


def persist_event(record):
    with open(EVENT_LOG_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")


@app.route("/webhook", methods=["POST"])
def webhook():
    verify_signature(request)

    event_type = request.headers.get("X-GitHub-Event")
    payload = request.get_json()

    if event_type != "push":
        return jsonify({"status": "ignored", "event": event_type}), 200

    repo_name = payload["repository"]["full_name"]
    branch = payload["ref"].replace("refs/heads/", "")

    stored = 0

    for commit in payload.get("commits", []):
        record = {
            "event_type": event_type,
            "commit_sha": commit["id"],
            "commit_timestamp": commit["timestamp"],
            "repo_name": repo_name,
            "author": commit["author"]["name"],
            "branch": branch,
            "ingested_at": datetime.utcnow().isoformat()
        }

        persist_event(record)
        stored += 1

    return jsonify({
        "status": "received",
        "stored_events": stored
    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)


import os
import hmac
import hashlib
from datetime import datetime
from flask import Flask, request, abort, jsonify
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()


app = Flask(__name__)

GITHUB_SECRET = b"webshook_url@8989"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Supabase environment variables not set")

supabase = create_client(
    SUPABASE_URL,
    SUPABASE_SERVICE_ROLE_KEY
)


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


@app.route("/webhook", methods=["POST"])
def webhook():
    verify_signature(request)

    event_type = request.headers.get("X-GitHub-Event")
    payload = request.get_json()

    if event_type != "push":
        return jsonify({"status": "ignored", "event": event_type}), 200

    repo_name = payload["repository"]["full_name"]
    branch = payload["ref"].replace("refs/heads/", "")

    rows = []

    for commit in payload.get("commits", []):
        rows.append({
            "event_type": event_type,
            "commit_sha": commit["id"],
            "commit_timestamp": commit["timestamp"],
            "repo_name": repo_name,
            "author": commit["author"]["name"],
            "branch": branch,
            "ingested_at": datetime.utcnow().isoformat()
        })

    if not rows:
        return jsonify({"status": "no_commits"}), 200

    try:
        supabase.table("commits").insert(rows).execute()
    except Exception as e:
        error_msg = str(e).lower()

        if "duplicate key" in error_msg or "unique constraint" in error_msg:
            # Idempotency working as intended
            pass
        else:
            # Log real failures
            print("Supabase insert failed:", e)

    return jsonify({
        "status": "received",
        "insert_attempted": len(rows)
    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)

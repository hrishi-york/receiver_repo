import hmac
import hashlib
from flask import Flask, request, abort, jsonify

app = Flask(__name__)

GITHUB_SECRET = b"webshook_url@8989"

EVENT_STORE = []

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

    event = request.headers.get("X-GitHub-Event")
    payload = request.get_json()

    if event != "push":
        return jsonify({"status": "ignored", "event": event}), 200

    repo_name = payload["repository"]["full_name"]

    ref = payload["ref"]
    branch = ref.split("/")[-1]

    for commit in payload["commits"]:
        record = {
            "commit_sha": commit["id"],
            "commit_timestamp": commit["timestamp"],
            "repo_name": repo_name,
            "author": commit["author"]["name"],
            "branch": branch
        }

        EVENT_STORE.append(record)

    return jsonify({
        "status": "received",
        "stored_events": len(EVENT_STORE)
    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)

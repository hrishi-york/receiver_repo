import hmac
import hashlib
from flask import Flask, request, abort, jsonify

app = Flask(__name__)

GITHUB_SECRET = b"webshook_url@8989"


def verify_signature(payload, signature):
    mac = hmac.new(GITHUB_SECRET, msg=payload, digestmod=hashlib.sha256)
    expected = "sha256=" + mac.hexdigest()
    return hmac.compare_digest(expected, signature)


@app.route("/webhook", methods=["POST"])
def github_webhook():
    signature = request.headers.get("X-Hub-Signature-256")
    if not signature:
        abort(401)

    payload = request.data
    if not verify_signature(payload, signature):
        abort(401)

    event_type = request.headers.get("X-GitHub-Event")
    data = request.json

    if event_type == "deployment":
        deployment = data["deployment"]

        event = {
            "event_type": "deployment",
            "deployment_id": deployment["id"],
            "environment": deployment["environment"],
            "ref": deployment["ref"],
            "sha": deployment["sha"],
            "creator": deployment["creator"]["login"],
            "created_at": deployment["created_at"],
            "repo": data["repository"]["full_name"]
        }

        return jsonify(event), 200

    if event_type == "deployment_status":
        status = data["deployment_status"]

        event = {
            "event_type": "deployment_status",
            "deployment_id": data["deployment"]["id"],
            "state": status["state"],
            "environment": status["environment"],
            "description": status["description"],
            "log_url": status["log_url"],
            "updated_at": status["updated_at"],
            "repo": data["repository"]["full_name"]
        }

        return jsonify(event), 200

    return jsonify({"ignored_event": event_type}), 200


if __name__ == "__main__":
    app.run(port=5000)

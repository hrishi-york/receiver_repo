import hmac
import hashlib
from flask import Flask, request, abort, jsonify

app = Flask(__name__)

GITHUB_SECRET = b"webshook_url@8989"

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
    delivery_id = request.headers.get("X-GitHub-Delivery")
    payload = request.get_json()

    print("Event:", event)
    print("Delivery ID:", delivery_id)

    if event == "push":
        repo = payload["repository"]["full_name"]
        commits = len(payload["commits"])
        print(f"Push to {repo} with {commits} commits")

    return jsonify({"status": "received"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)

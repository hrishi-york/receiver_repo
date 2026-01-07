from flask import Flask, request, jsonify
from datetime import datetime

app = Flask(__name__)

EVENT_STORE = []

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    if request.method == "GET":
        return jsonify({
            "status": "alive",
            "message": "Webhook endpoint is running"
        }), 200

    event = request.headers.get("X-GitHub-Event", "unknown")
    payload = request.get_json(silent=True) or {}

    record = {
        "event": event,
        "repo": payload.get("repository", {}).get("full_name"),
        "action": payload.get("action"),
        "timestamp": datetime.utcnow().isoformat()
    }

    EVENT_STORE.append(record)

    return jsonify({
        "stored": True,
        "event": event
    }), 200

@app.route("/events", methods=["GET"])
def events():
    return jsonify(EVENT_STORE), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000)

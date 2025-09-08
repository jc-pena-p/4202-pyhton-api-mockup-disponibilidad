from flask import Flask, jsonify
import random
import time

app = Flask(__name__)

service_is_failing = False


@app.route("/consulta", methods=["GET"])
def consulta():
    global service_is_failing

    # Add random delay between 0-1 seconds
    delay_time = random.random()
    time.sleep(delay_time)

    # Randomly fail 10% of the time
    if random.random() < 0.1:
        service_is_failing = True
        return jsonify({"error": "Service unavailable"}), 503

    # Success response 90% of the time
    service_is_failing = False
    # Generate 10-100 random items
    num_items = random.randint(10, 100)
    items = []
    for i in range(num_items):
        items.append(
            {"id": i + 1, "name": f"Product {i + 1}", "count": random.randint(0, 1000)}
        )

    return jsonify({"items": items, "count": len(items)})


@app.route("/health", methods=["GET"])
def health():
    if service_is_failing:
        return jsonify({"status": "error"}), 503
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(debug=True)

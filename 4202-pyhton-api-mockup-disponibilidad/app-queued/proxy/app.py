import pika
import os, json, uuid, datetime, time
from flask import Flask, jsonify

AMQP_URL   = os.getenv("AMQP_URL", "amqp://guest:guest@mq:5672/%2F")
EXCHANGE   = os.getenv("EXCHANGE", "inventory.ex")
RK_QUERY   = os.getenv("RK_QUERY", "inventory.query")
RPC_TIMEOUT = float(os.getenv("RPC_TIMEOUT", "1.8"))

LOG_PATH   = os.getenv("LOG_PATH", "/var/log/consulta/proxy.jsonl")
CACHE_FILE = os.getenv("CACHE_FILE", "/var/cache/consulta/last_good.json")
PORT       = int(os.getenv("PORT", "80"))

app = Flask(__name__)

def jlog(event: dict):
    try:
        event["ts"] = datetime.datetime.utcnow().isoformat() + "Z"
        with open(LOG_PATH, "a") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
    except Exception:
        pass

def cache_load_raw():
    try:
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"error": "timeout"}

def normalize(obj, from_cache: bool):
    """Siempre devolver un dict + flag from_cache, envolviendo listas."""
    if isinstance(obj, dict):
        out = dict(obj)
    else:
        out = {"response": obj}
    out["from_cache"] = from_cache
    return out

@app.route("/consulta", methods=["GET"])
def consulta():
    corr_id = str(uuid.uuid4())
    t0 = time.perf_counter()

    params = pika.URLParameters(AMQP_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)

    result = channel.queue_declare(queue="", exclusive=True, durable=False, auto_delete=True)
    callback_q = result.method.queue

    response_holder = {"body": None}

    def on_response(ch, method, props, body):
        if props.correlation_id == corr_id:
            response_holder["body"] = body
            ch.basic_ack(method.delivery_tag)

    channel.basic_consume(queue=callback_q, on_message_callback=on_response, auto_ack=False)

    headers = {"x-attempt": 0} 
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=RK_QUERY,
        properties=pika.BasicProperties(
            reply_to=callback_q,
            correlation_id=corr_id,
            headers=headers,
            delivery_mode=2 
        ),
        body=b"{}"
    )

    deadline = time.time() + RPC_TIMEOUT
    try:
        while time.time() < deadline and response_holder["body"] is None:
            connection.process_data_events(time_limit=0.05)

        if response_holder["body"] is not None:
            decoded = json.loads(response_holder["body"].decode("utf-8", "ignore"))
            data = normalize(decoded, from_cache=False)
            jlog({
                "type": "request", "path": "/consulta", "outcome": "ok",
                "corr_id": corr_id, "from_cache": False,
                "duration_ms": round((time.perf_counter()-t0)*1000, 2)
            })
            return jsonify(data), 200

        cached_raw = cache_load_raw()       
        cached = normalize(cached_raw, True)
        jlog({
            "type": "request", "path": "/consulta", "outcome": "timeout_fallback",
            "corr_id": corr_id, "from_cache": True,
            "duration_ms": round((time.perf_counter()-t0)*1000, 2)
        })
        return jsonify(cached), 200

    finally:
        try:
            connection.close()
        except Exception:
            pass

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)

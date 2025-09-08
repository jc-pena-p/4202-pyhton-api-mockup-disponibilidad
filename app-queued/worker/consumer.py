import os, json, time, datetime
import pika, requests

AMQP_URL   = os.getenv("AMQP_URL", "amqp://guest:guest@mq:5672/%2F")
EXCHANGE   = os.getenv("EXCHANGE", "inventory.ex")

QUEUE_MAIN = os.getenv("QUEUE_MAIN", "inventory.q")
RK_QUERY   = os.getenv("RK_QUERY", "inventory.query")

QUEUE_RETRY = os.getenv("QUEUE_RETRY_2S", "inventory.retry.2s")
RK_RETRY    = os.getenv("RK_RETRY", "inventory.retry")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))
UPSTREAM_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "1.5"))
UPSTREAM_URL = os.getenv("UPSTREAM_URL", "https://maestria.codezor.dev/consulta")

LOG_PATH   = os.getenv("LOG_PATH", "/var/log/consulta/worker.jsonl")
CACHE_FILE = os.getenv("CACHE_FILE", "/var/cache/consulta/last_good.json")

def jlog(event: dict):
    try:
        event["ts"] = datetime.datetime.utcnow().isoformat() + "Z"
        with open(LOG_PATH, "a") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
    except Exception:
        pass

def cache_save(data):
    try:
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        with open(CACHE_FILE, "w") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass

def call_upstream(timeout: float):
    r = requests.get(UPSTREAM_URL, timeout=timeout)
    r.raise_for_status()
    return r.json()

def normalize(obj, from_cache: bool):
    if isinstance(obj, dict):
        out = dict(obj)
    else:
        out = {"response": obj}
    out["from_cache"] = from_cache
    return out

def setup_topology(channel: pika.adapters.blocking_connection.BlockingChannel):
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)

    channel.queue_declare(queue=QUEUE_MAIN, durable=True, arguments={})
    channel.queue_bind(queue=QUEUE_MAIN, exchange=EXCHANGE, routing_key=RK_QUERY)

    channel.queue_declare(
        queue=QUEUE_RETRY,
        durable=True,
        arguments={
            "x-message-ttl": 2000,
            "x-dead-letter-exchange": EXCHANGE,
            "x-dead-letter-routing-key": RK_QUERY,
        }
    )
    channel.queue_bind(queue=QUEUE_RETRY, exchange=EXCHANGE, routing_key=RK_RETRY)

def reply_and_ack(ch, method, props, data):
    if props.reply_to:
        ch.basic_publish(
            exchange="",
            routing_key=props.reply_to,
            properties=pika.BasicProperties(
                correlation_id=props.correlation_id,
                delivery_mode=1
            ),
            body=json.dumps(data).encode("utf-8")
        )
    ch.basic_ack(delivery_tag=method.delivery_tag)

def send_to_retry(ch, method, props, body, attempt_next: int):
    headers = dict(props.headers or {})
    headers["x-attempt"] = attempt_next
    ch.basic_publish(
        exchange=EXCHANGE,
        routing_key=RK_RETRY,
        properties=pika.BasicProperties(
            reply_to=props.reply_to,
            correlation_id=props.correlation_id,
            headers=headers,
            delivery_mode=2
        ),
        body=body
    )
    ch.basic_ack(delivery_tag=method.delivery_tag)

def process_message(ch, method, props, body):
    attempt = int((props.headers or {}).get("x-attempt", 0))
    corr_id = props.correlation_id

    t0 = time.perf_counter()
    try:
        upstream = call_upstream(timeout=UPSTREAM_TIMEOUT)
        cache_save(upstream)

        data = normalize(upstream, from_cache=False)

        jlog({
            "type": "attempt", "corr_id": corr_id, "attempt": attempt,
            "outcome": "ok", "duration_ms": round((time.perf_counter()-t0)*1000, 2)
        })
        reply_and_ack(ch, method, props, data)
        return

    except requests.Timeout:
        jlog({
            "type": "attempt", "corr_id": corr_id, "attempt": attempt,
            "outcome": "upstream_timeout"
        })
    except Exception as e:
        jlog({
            "type": "attempt", "corr_id": corr_id, "attempt": attempt,
            "outcome": "error", "error": repr(e)
        })

    if attempt < MAX_RETRIES:
        send_to_retry(ch, method, props, body, attempt_next=attempt+1)
    else:
        try:
            with open(CACHE_FILE, "r") as f:
                cached_raw = json.load(f)
        except Exception:
            cached_raw = {"error": "timeout"}

        cached = normalize(cached_raw, from_cache=True)

        jlog({
            "type": "attempt", "corr_id": corr_id, "attempt": attempt,
            "outcome": "fallback_cache"
        })
        reply_and_ack(ch, method, props, cached)

def main():
    for i in range(30):
        try:
            params = pika.URLParameters(AMQP_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            setup_topology(channel)
            jlog({"type": "boot", "message": "worker conectado a RabbitMQ"})
            break
        except Exception:
            print("[worker] RabbitMQ no disponible, reintento {}/30...".format(i+1), flush=True)
            time.sleep(1.0)
    else:
        print("[worker] No pudo conectar a RabbitMQ", flush=True)
        return

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_MAIN, on_message_callback=process_message, auto_ack=False)
    print("[worker] escuchando", QUEUE_MAIN, "con fallback de caché…", flush=True)
    try:
        channel.start_consuming()
    finally:
        try:
            connection.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()

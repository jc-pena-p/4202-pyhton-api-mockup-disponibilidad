FROM alpine:3.19

# Dependencias mínimas para el script
RUN apk add --no-cache bash curl jq

WORKDIR /app
COPY monitor.sh /app/monitor.sh
COPY runner.sh  /app/runner.sh

RUN chmod +x /app/*.sh

# Variables por defecto (puedes sobreescribir en docker-compose)
ENV OUT_DIR=/var/log/consulta \
    STATE_FILE=/var/cache/consulta/monitor_state.json \
    UPSTREAM_HEALTH_URL=https://maestria.codezor.dev/health \
    FAIL_THRESHOLD=3 \
    FAIL_WINDOW=5 \
    INTERVAL_SECONDS=1

# Arranca el runner en bucle, no usamos crond
CMD ["/usr/bin/env","bash","/app/runner.sh"]

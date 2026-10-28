#!/usr/bin/env bash
# Automatic smoke test. Containers are ephemeral on GitHub Actions.
set -Eeuo pipefail
cd "$(dirname "$0")/.."
mkdir -p artifacts/smoke
OUT="$PWD/artifacts/smoke"

capture() {
  local name="$1"; shift
  echo "===== $name $(date -u +%FT%TZ) =====" | tee "$OUT/$name.log"
  "$@" 2>&1 | tee -a "$OUT/$name.log"
}
collect() {
  local code=$?
  trap - EXIT
  docker compose -f kafka/docker-compose.yml ps -a > "$OUT/kafka-final-ps.log" 2>&1 || true
  docker compose -f nats/docker-compose.yml ps -a > "$OUT/nats-final-ps.log" 2>&1 || true
  docker compose -f kafka/docker-compose.yml logs --no-color --timestamps > "$OUT/kafka-container.log" 2>&1 || true
  docker compose -f nats/docker-compose.yml logs --no-color --timestamps > "$OUT/nats-container.log" 2>&1 || true
  docker compose -f kafka/docker-compose.yml down --remove-orphans >/dev/null 2>&1 || true
  docker compose -f nats/docker-compose.yml down --remove-orphans >/dev/null 2>&1 || true
  echo "smoke_exit_code=$code" > "$OUT/exit-status.txt"
  exit "$code"
}
trap collect EXIT

capture kafka_start docker compose -f kafka/docker-compose.yml up -d
capture nats_start docker compose -f nats/docker-compose.yml up -d

for node in kafka1 kafka2 kafka3 nats1 nats2 nats3 cliente cliente-nats; do
  [[ "$(docker inspect -f '{{.State.Running}}' "$node")" == true ]] || { echo "Not running: $node"; exit 1; }
done
for sys in kafka nats; do
  if [[ "$sys" == kafka ]]; then cli=cliente; else cli=cliente-nats; fi
  ready=false
  for attempt in $(seq 1 35); do
    if docker exec "$cli" /app/sdtool setup --sys "$sys" > "$OUT/$sys-setup-attempt.log" 2>&1; then
      ready=true
      break
    fi
    sleep 4
  done
  cat "$OUT/$sys-setup-attempt.log"
  [[ "$ready" == true ]] || { echo "$sys never became ready"; exit 1; }
  capture "$sys-topology" docker exec "$cli" /app/sdtool setup --sys "$sys"
  if [[ "$sys" == kafka ]]; then
    capture kafka-topic docker exec cliente sh -c '/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --describe --topic probe'
    capture kafka-quorum docker exec cliente sh -c '/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server "$BS" describe --status'
  else
    capture nats-stream docker exec cliente-nats nats --server nats://172.33.0.51:4222 stream info PROBE
  fi
  capture "$sys-publish" docker exec "$cli" /app/sdtool probe --sys "$sys" --count 12 --interval 30ms --file /tmp/smoke.csv
  capture "$sys-consume-verify" docker exec "$cli" /app/sdtool verify --sys "$sys" --file /tmp/smoke.csv --idle 2s
  docker cp "$cli:/tmp/smoke.csv" "$OUT/$sys-acked.csv"
  grep -q 'lidas=12  únicas=12  confirmadas ao produtor=12' "$OUT/$sys-consume-verify.log"
  grep -q 'PERDIDAS (confirmadas mas ausentes): 0 ' "$OUT/$sys-consume-verify.log"
  grep -q 'DUPLICADAS: 0' "$OUT/$sys-consume-verify.log"
  grep -q 'FORA DE ORDEM: 0' "$OUT/$sys-consume-verify.log"
  grep -q 'FANTASMAS (erro no produtor, mas gravadas): 0' "$OUT/$sys-consume-verify.log"
done
printf 'Kafka and NATS baseline publication/consumption verified at %s\n' "$(date -u +%FT%TZ)" | tee "$OUT/smoke-result.txt"

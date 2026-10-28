#!/usr/bin/env bash
# E3: consumer visibility (NOT follower replication propagation).
set -Eeuo pipefail
cd "$(dirname "$0")/.."
mkdir -p artifacts/e3-visibility
out="$PWD/artifacts/e3-visibility"
finish() {
  code=$?
  trap - EXIT
  docker compose -f kafka/docker-compose.yml logs --no-color --timestamps > "$out/kafka_containers.log" 2>&1 || true
  docker compose -f nats/docker-compose.yml logs --no-color --timestamps > "$out/nats_containers.log" 2>&1 || true
  docker compose -f kafka/docker-compose.yml down --remove-orphans >/dev/null 2>&1 || true
  docker compose -f nats/docker-compose.yml down --remove-orphans >/dev/null 2>&1 || true
  echo "exit=$code" > "$out/status.txt"
  exit "$code"
}
trap finish EXIT
(
 cd go
 mkdir -p bin
 CGO_ENABLED=0 GOOS=linux go build -mod=readonly -o bin/sdtool .
 CGO_ENABLED=0 GOOS=linux go build -mod=readonly -o bin/visibility ../methodology/visibility.go
)
for system in kafka nats; do
  docker compose -f "$system/docker-compose.yml" up -d > "$out/$system.up.log" 2>&1
  if [[ "$system" == kafka ]]; then client=cliente; else client=cliente-nats; fi
  ready=0
  for attempt in $(seq 1 50); do
    if docker exec "$client" /app/sdtool setup --sys "$system" > "$out/$system.setup.$attempt.log" 2>&1; then
      ready=1
      break
    fi
    sleep 2
  done
  test "$ready" = 1
  if [[ "$system" == kafka ]]; then
    for acks in all 1; do
      docker exec "$client" /app/visibility --sys kafka --acks "$acks" --runs 5 --out "/tmp/e3visibility_$acks.csv" \
       > "$out/kafka_$acks.log" 2>&1
      docker cp "$client:/tmp/e3visibility_$acks.csv" "$out/kafka_$acks.csv"
    done
  else
    docker exec "$client" /app/visibility --sys nats --runs 5 --out /tmp/e3visibility_nats.csv \
      > "$out/nats.log" 2>&1
    docker cp "$client:/tmp/e3visibility_nats.csv" "$out/nats.csv"
  fi
  docker compose -f "$system/docker-compose.yml" down --remove-orphans >/dev/null
done
python3 - "$out" <<'PY'
import csv, statistics, pathlib, sys
p=pathlib.Path(sys.argv[1])
with (p/"visibility_summary.md").open("w") as f:
 f.write("# E3: ACK versus observação pelo consumidor (NÃO propagação entre réplicas)\n\n")
 f.write("Tempo: relógio monotônico no mesmo processo Go. Warmup não contado. "+
         "A leitura ocorre pela API normal do líder/stream; seguidores não são "+
         "observados diretamente. Não é tempo de replica fetch ou commit interno.\n\n")
 f.write("| Sistema/modo | n | ACK média ± DP (ms) | callback média ± DP (ms) |\n|---|---:|---:|---:|\n")
 for name in ("kafka_all","kafka_1","nats"):
  rows=list(csv.DictReader((p/(name+".csv")).open()))
  assert len(rows)==5, (name,len(rows))
  assert all(x["producer_status"]=="acknowledged" and x["read_status"]=="observed" for x in rows)
  ack=[float(x["ack_ms"]) for x in rows]
  seen=[float(x["read_visibility_ms"]) for x in rows]
  f.write(f"| {name} | 5 | {statistics.mean(ack):.3f} ± {statistics.stdev(ack):.3f} | ")
  f.write(f"{statistics.mean(seen):.3f} ± {statistics.stdev(seen):.3f} |\n")
print("PASS: 15 separate ACK and first consumer callback observations")
PY

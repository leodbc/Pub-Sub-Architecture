#!/usr/bin/env bash
set -Eeuo pipefail
mkdir -p artifacts/m2-e3-follower artifacts/m2-e3-bench
out="$PWD/artifacts/m2-e3-follower"; perf="$PWD/artifacts/m2-e3-bench"
compose=(docker compose -p m2e3 -f kafka/docker-compose.yml -f audit/m2_e3/rack.yml)
trap '"${compose[@]}" logs --no-color >"$out/compose_final.log" 2>&1 || true; "${compose[@]}" down -v --remove-orphans >"$out/shutdown.log" 2>&1 || true' EXIT
{ date -u; uname -a; go version; docker version; docker compose version; git rev-parse HEAD; } >"$out/host.log" 2>&1
(cd go && mkdir -p bin && go build -mod=readonly -trimpath -o bin/m2_e3 ../audit/m2_e3/follower.go)
"${compose[@]}" up -d --wait >"$out/start.log" 2>&1 || { "${compose[@]}" ps >> "$out/start.log"; exit 1; }
for i in $(seq 1 80); do
 if docker exec cliente sh -c '/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server "$BS" describe --status' >"$out/quorum.log" 2>&1; then break; fi
 sleep 3
done
cat "$out/quorum.log"
for node in kafka1 kafka2 kafka3; do
 docker exec "$node" sh -c 'grep -E "broker.rack|replica.selector.class" /opt/kafka/config/server.properties || true' >"$out/${node}_config_hint.log" 2>&1 || true
 docker exec "$node" sh -c 'ls -la /tmp/kafka-logs' >"$out/${node}_logdir.log" 2>&1 || true
done
for trial in 1 2 3 4 5; do
 topic="m2_e3_follower_${GITHUB_RUN_ID:-local}_${trial}"
 docker exec cliente sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server \"\$BS\" --create --topic $topic --partitions 1 --replication-factor 3" >"$out/create_${trial}.log" 2>&1
 sleep 4
 docker exec cliente sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server \"\$BS\" --describe --topic $topic" >"$out/describe_before_${trial}.log" 2>&1
 docker exec cliente "/app/m2_e3" -topic "$topic" -trial "$trial" >"$out/measure_${trial}.jsonl" 2>"$out/measure_${trial}.stderr" || echo "E3_FOLLOWER_NOT_CONFIRMED trial=$trial" >>"$out/warnings.log"
 docker exec cliente sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server \"\$BS\" --describe --topic $topic" >"$out/describe_after_${trial}.log" 2>&1
 done
# Example command is 50,000 records of 200 bytes; retain all original output separately.
for mode in 1 all; do
 for trial in 1 2 3 4 5; do
  topic="m2_e3_perf_${GITHUB_RUN_ID:-local}_${mode}_${trial}"
  docker exec cliente sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server \"\$BS\" --create --topic $topic --partitions 3 --replication-factor 3" >"$perf/create_${mode}_${trial}.log" 2>&1
  docker exec cliente sh -c "/opt/kafka/bin/kafka-producer-perf-test.sh --topic $topic --num-records 50000 --record-size 200 --throughput -1 --producer-props bootstrap.servers=\"\$BS\" acks=$mode" >"$perf/perf_${mode}_${trial}.log" 2>&1 || echo "PERF_FAILED $mode $trial" >>"$perf/warnings.log"
 done
done
python3 audit/m2_e3/validate.py artifacts/m2-e3-follower artifacts/m2-e3-bench

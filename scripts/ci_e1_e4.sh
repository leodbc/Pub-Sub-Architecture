#!/usr/bin/env bash
# E1-E4 runner: preserves the previously authored collector and statistical analyzer.
set -Eeuo pipefail
cd "$(dirname "$0")/.."
mkdir -p artifacts/e1-e4
OUT="$PWD/artifacts/e1-e4"
finalize() {
  local result=$?
  trap - EXIT
  docker compose -f kafka/docker-compose.yml ps -a > "$OUT/kafka-final-ps.log" 2>&1 || true
  docker compose -f nats/docker-compose.yml ps -a > "$OUT/nats-final-ps.log" 2>&1 || true
  docker compose -f kafka/docker-compose.yml logs --no-color --timestamps > "$OUT/kafka-container.log" 2>&1 || true
  docker compose -f nats/docker-compose.yml logs --no-color --timestamps > "$OUT/nats-container.log" 2>&1 || true
  docker compose -f kafka/docker-compose.yml down --remove-orphans >/dev/null 2>&1 || true
  docker compose -f nats/docker-compose.yml down --remove-orphans >/dev/null 2>&1 || true
  echo "e1e4_exit_code=$result" > "$OUT/exit-status.txt"
  exit "$result"
}
trap finalize EXIT
bash scripts/coletar_e1_e4.sh "$PWD" "$OUT" --permitir-reset
# Complement E4: Kafka has no total order across 3 partitions.
# Switch from constant key (existing collector) to 90 distinct keys and inspect
# offsets, partition distribution and within-partition order independently.
docker exec cliente /app/sdtool reset --sys kafka > "$OUT/e4_kafka_multi_reset.log" 2>&1
for i in $(seq 1 50); do
  if ! docker exec cliente sh -c '/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --describe --topic probe' >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec cliente /app/sdtool setup --sys kafka --partitions 3 --replicas 3 > "$OUT/e4_kafka_multi_setup.log" 2>&1
docker exec cliente sh -c '/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --describe --topic probe' > "$OUT/e4_kafka_multi_topology.log" 2>&1
docker exec cliente /app/sdtool probe --sys kafka --key-mode id --count 90 --interval 10ms --file /tmp/e4_multi.csv > "$OUT/e4_kafka_multi_probe.log" 2>&1
docker exec cliente /app/sdtool verify --sys kafka --file /tmp/e4_multi.csv --idle 2s > "$OUT/e4_kafka_multi_verify.log" 2>&1
docker cp cliente:/tmp/e4_multi.csv "$OUT/e4_kafka_multi_acked.csv"
grep -q 'lidas=90  únicas=90  confirmadas ao produtor=90' "$OUT/e4_kafka_multi_verify.log"
grep -q 'PARTIÇÕES OBSERVADAS: 3' "$OUT/e4_kafka_multi_verify.log"
grep -q 'FORA DE ORDEM POR PARTIÇÃO: 0' "$OUT/e4_kafka_multi_verify.log"
grep -q 'PERDIDAS (confirmadas mas ausentes): 0 ' "$OUT/e4_kafka_multi_verify.log"
grep -q 'DUPLICADAS: 0' "$OUT/e4_kafka_multi_verify.log"

python3 scripts/analisar_coletas.py "$OUT"
python3 - "$OUT" <<'PY'
import pathlib, re, sys
p=pathlib.Path(sys.argv[1])
for file in ("e3_kafka_acks_all.log","e3_kafka_acks_1.log","e3_nats_rf3.log"):
    data=(p/file).read_text()
    matches=re.findall(r"run\s+(\d+):\s+latência média=([0-9.]+)\s+ms\s+vazão=([0-9.]+)\s+msg/s\s+erros=(\d+)",data)
    assert [int(x[0]) for x in matches] == [1,2,3,4,5], (file, matches)
    assert all(int(x[3]) == 0 for x in matches), f"{file}: errors recorded"
    assert "RESULTADO (5 execuções):" in data, file
for system in ("kafka","nats"):
    for exp,num in (("e2",10),("e4",100)):
        data=(p/f"{exp}_{system}_verify.log").read_text()
        assert f"lidas={num}  únicas={num}  confirmadas ao produtor={num}" in data, (exp,system)
        for marker in ["PERDIDAS (confirmadas mas ausentes): 0 ", "DUPLICADAS: 0", "FORA DE ORDEM: 0", "FANTASMAS (erro no produtor, mas gravadas): 0"]:
            assert marker in data, (exp, system, marker)
print("E1-E4 source-backed checks passed; E2 and E4 apply to one partition and a single producer only.")
PY

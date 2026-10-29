#!/usr/bin/env bash
# Ensaio isolado do seminário M2; não substitui evidências científicas E1–E9.
set -Eeuo pipefail
cd "$(dirname "$0")/.."
OUT="$PWD/artifacts/seminario-demo"
mkdir -p "$OUT" go/bin
kcompose() { docker compose -p m2seminarioci-k -f kafka/docker-compose.yml -f audit/m2_e3/rack.yml "$@"; }
ncompose() { docker compose -p m2seminarioci-n -f nats/docker-compose.yml "$@"; }
STOPPED=""
cleanup() {
  local rc=$?
  trap - EXIT
  set +e
  if [[ -n "$STOPPED" ]]; then docker start "$STOPPED" >>"$OUT/cleanup.log" 2>&1; fi
  kcompose ps -a >"$OUT/kafka_final_ps.txt" 2>&1
  ncompose ps -a >"$OUT/nats_final_ps.txt" 2>&1
  kcompose logs --no-color >"$OUT/kafka_compose.log" 2>&1
  ncompose logs --no-color >"$OUT/nats_compose.log" 2>&1
  printf 'EXIT_CODE=%s\n' "$rc" | tee "$OUT/exit_code.txt"
  exit "$rc"
}
trap cleanup EXIT
section() { printf '\n========== %s ==========\n' "$*"; }
fail() { echo "::error::$*" >&2; exit 1; }
wait_for() { local limit="$1"; shift; local s=$SECONDS; while (( SECONDS-s < limit )); do if "$@" >/dev/null 2>&1;then return 0;fi;sleep 2;done;return 1; }
kexec() { docker exec cliente sh -c '/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" '"$1"; }
kdesc() { kexec '--describe --topic '"$1"; }
kready() { kexec '--list'; }
nready() { docker exec cliente-nats nats stream ls; }
isr3() {
  local d isr
  d="$(kdesc "$1" 2>/dev/null)" || return 1
  isr="$(printf '%s\n' "$d" | sed -nE 's/.*Isr: *([0-9,]+).*/\1/p' | head -n1)"
  [[ "$isr" =~ ^[123],[123],[123]$ ]] || return 1
  for n in 1 2 3;do [[ ",$isr," == *",$n,"* ]] || return 1;done
}
leader() { kdesc probe | sed -nE 's/.*Leader: *([0-9]+).*/\1/p' | head -n1; }
kstatus() { kdesc probe; }
check_zero() {
  local f="$1"
  grep -Eq '^PERDIDAS \(confirmadas mas ausentes\): 0 ' "$f" || fail "Mensagens confirmadas ausentes em $f"
  grep -Eq '^DUPLICADAS: 0$' "$f" || fail "Duplicatas em $f"
}
section "Origem e ferramentas"
git rev-parse HEAD | tee "$OUT/commit.txt"
git merge-base --is-ancestor 24a98f93c5d8cacec3b9793049788a107a1e5c43 HEAD || fail 'PR #5 não é ancestral'
go version | tee "$OUT/go_version.txt"
docker version >"$OUT/docker_version.txt"
docker compose version | tee "$OUT/compose_version.txt"
for f in go/main.go go/kafka.go go/nats.go audit/m2_fase4/e4_30.go audit/m2_e3/follower.go kafka/docker-compose.yml nats/docker-compose.yml audit/m2_e3/rack.yml; do
  [[ "$(git rev-parse "HEAD:$f")" == "$(git rev-parse "24a98f93c5d8cacec3b9793049788a107a1e5c43:$f")" ]] || fail "Fonte histórica modificada: $f"
done
(cd go && CGO_ENABLED=0 GOOS=linux go build -mod=readonly -trimpath -o bin/sdtool . && CGO_ENABLED=0 GOOS=linux go build -mod=readonly -trimpath -o bin/m2e4 ../audit/m2_fase4/e4_30.go && CGO_ENABLED=0 GOOS=linux go build -mod=readonly -trimpath -o bin/m2e3 ../audit/m2_e3/follower.go)
section 'Iniciar Kafka KRaft com 3 brokers, overlay follower e NATS JetStream com 3 nós'
kcompose up -d >"$OUT/kafka_start.txt" 2>&1
wait_for 120 kready || fail 'Kafka não ficou pronto'
ncompose up -d >"$OUT/nats_start.txt" 2>&1
wait_for 90 nready || fail 'NATS não ficou pronto'
section 'E1: tópicos e stream RF3'
docker exec cliente /app/sdtool setup --sys kafka --partitions 1 --replicas 3 | tee "$OUT/kafka_setup.txt"
kexec '--create --topic pedidos --partitions 3 --replication-factor 3 --config min.insync.replicas=2' | tee "$OUT/pedidos_create.txt"
docker exec cliente-nats /app/sdtool setup --sys nats --replicas 3 | tee "$OUT/nats_setup.txt"
wait_for 90 isr3 probe || fail 'Kafka probe sem ISR3'
kdesc pedidos | tee "$OUT/pedidos_describe.txt"
kstatus | tee "$OUT/probe_describe_before.txt"
docker exec cliente sh -c '/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server "$BS" describe --status' | tee "$OUT/kraft_quorum.txt"
docker exec cliente-nats nats stream info PROBE | tee "$OUT/nats_stream_info.txt"
[[ "$(grep -c 'Partition: [0-9]' "$OUT/pedidos_describe.txt")" == 3 ]] || fail 'pedidos sem 3 partições'
section 'E2: publicação, consumo e offsets Kafka'
docker exec cliente /app/sdtool probe --sys kafka --count 12 --interval 50ms --start-id 101001 --file /tmp/m2_demo_kafka.csv | tee "$OUT/e2_produce.txt"
docker exec cliente /app/sdtool verify --sys kafka --file /tmp/m2_demo_kafka.csv --idle 1s | tee "$OUT/e2_verify.txt"
check_zero "$OUT/e2_verify.txt"
section 'E4: 30 sem chave + 30 com mesma chave em pedidos'
docker exec cliente /app/m2e4 --topic pedidos | tee "$OUT/e4_full.txt" | awk '
  /^PRODUCE/ && ++p <= 3 { print }
  /^E4_SUMMARY|^E4_OK/ { print }
'
grep -q '^E4_OK' "$OUT/e4_full.txt" || fail 'E4 60/60 não confirmado'
section 'E3: UMA observação didática, não benchmark nem nova série científica'
topic="m2_demo_follower_$GITHUB_RUN_ID"
kexec "--create --topic $topic --partitions 1 --replication-factor 3" >"$OUT/e3_create.txt"
wait_for 60 isr3 "$topic" || fail 'E3 sem ISR3 antes da demonstração'
if docker exec cliente /app/m2e3 -topic "$topic" -trial 1 >"$OUT/e3_measurement.jsonl" 2>"$OUT/e3.stderr" && grep -q '"follower_fetch_confirmed":true' "$OUT/e3_measurement.jsonl"; then
  echo 'E3_DEMO_FOLLOWER_CONFIRMED' | tee "$OUT/e3_result.txt"
else
  echo 'E3_DEMO_FOLLOWER_NOT_PROVEN — consultar as cinco medições históricas do PR #5' | tee "$OUT/e3_result.txt"
fi
section 'E5/E7: interromper SOMENTE o líder atual de probe, observar eleição e recuperar'
before="$(leader)"
[[ "$before" =~ ^[123]$ ]] || fail "Líder inválido: $before"
STOPPED="kafka$before"
[[ "$(docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' "$STOPPED")" == 'm2seminarioci-k' ]] || fail 'Contêiner fora do laboratório'
printf 'Líder anterior: %s\n' "$before" | tee "$OUT/e5_e7.txt"
docker stop --time 5 "$STOPPED" >>"$OUT/e5_e7.txt" 2>&1
after=''
s=$SECONDS
while (( SECONDS-s < 65 ));do
  after="$(leader 2>/dev/null || true)"
  if [[ "$after" =~ ^[123]$ && "$after" != "$before" ]];then break;fi
  sleep 2
done
[[ "$after" =~ ^[123]$ && "$after" != "$before" ]] || fail 'Não observou novo líder no prazo'
printf 'Novo líder: %s\n' "$after" | tee -a "$OUT/e5_e7.txt"
kstatus | tee "$OUT/e5_probe_after_failover.txt"
docker start "$STOPPED" >>"$OUT/e5_e7.txt"
wait_for 90 isr3 probe || fail 'Não recuperou ISR3'
STOPPED=''
kstatus | tee "$OUT/e7_probe_after_recovery.txt"
docker exec cliente /app/sdtool verify --sys kafka --file /tmp/m2_demo_kafka.csv --idle 1s | tee "$OUT/e7_verify.txt"
check_zero "$OUT/e7_verify.txt"
section 'E9: JetStream RF3, produção e consumo'
docker exec cliente-nats /app/sdtool probe --sys nats --count 12 --interval 50ms --start-id 201001 --file /tmp/m2_demo_nats.csv | tee "$OUT/e9_produce.txt"
docker exec cliente-nats /app/sdtool verify --sys nats --file /tmp/m2_demo_nats.csv --idle 1s | tee "$OUT/e9_verify.txt"
check_zero "$OUT/e9_verify.txt"
section 'Resultados: CI real e isolado, não substituir evidências históricas'
{
  echo '## Ensaio operacional M2 — Kafka e NATS'
  echo "- Commit de execução: $(git rev-parse HEAD)"
  echo '- E1: topologias dos dois clusters: executado'
  echo '- E2/E4: publicação, consumo e ordenação: executados'
  echo "- E3 didático: $(cat "$OUT/e3_result.txt")"
  echo '- E5/E7: falha de um líder e recuperação do ISR: executadas'
  echo '- E9: publicação/consumo JetStream: executados'
  echo '- Não é reexecução dos benchmarks científicos nem comprova sozinho tempo mínimo de apresentação ao vivo.'
} | tee "$OUT/resumo.md" >>"$GITHUB_STEP_SUMMARY"

#!/usr/bin/env bash
# Coleta E1-E4 para o repositório leodbc/Pub-Sub-Architecture.
# Não executado nem validado com Docker nesta sessão; revisar antes de usar.
set -Eeuo pipefail

usage() {
  cat <<'EOM'
Uso: bash coletar_e1_e4.sh CAMINHO_DO_REPO CAMINHO_DAS_EVIDENCIAS --permitir-reset

ATENÇÃO: --permitir-reset autoriza sdtool reset de tópico Kafka `probe`
e stream NATS `PROBE`. Isso APAGA os dados anteriores desses recursos.
O script não executa E5-E8 (injeção de falhas), que exigem controle
explícito dos líderes e acompanhamento humano das janelas de indisponibilidade.
EOM
}

if [[ $# -ne 3 || "$3" != "--permitir-reset" ]]; then usage; exit 2; fi
ROOT="$(cd "$1" && pwd)"
mkdir -p "$2"
OUT="$(cd "$2" && pwd)"
for f in README.md go/main.go go/go.mod kafka/docker-compose.yml nats/docker-compose.yml; do
  if [[ ! -f "$ROOT/$f" ]]; then echo "Arquivo ausente: $ROOT/$f" >&2; exit 2; fi
done
command -v go >/dev/null || { echo 'Go não encontrado'; exit 2; }
command -v docker >/dev/null || { echo 'Docker não encontrado'; exit 2; }

capture() {
  local label="$1"; shift
  printf '\n===== %s | %s =====\n' "$label" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee "$OUT/${label}.log"
  if "$@" 2>&1 | tee -a "$OUT/${label}.log"; then
    echo "OK: $label" | tee -a "$OUT/${label}.log"
  else
    echo "FALHOU: $label (ver log)" >&2
    exit 1
  fi
}

retry_setup() {
  local sys="$1" label="$2" cli attempt output
  if [[ "$sys" == kafka ]]; then cli=cliente; else cli=cliente-nats; fi
  for attempt in $(seq 1 45); do
    output="$OUT/$label-attempt-$attempt.log"
    printf '===== %s attempt %s | %s =====\n' "$label" "$attempt" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$output"
    if docker exec "$cli" /app/sdtool setup --sys "$sys" >> "$output" 2>&1; then
      cp "$output" "$OUT/$label.log"
      echo "OK: $label na tentativa $attempt"
      return 0
    fi
    sleep 2
  done
  echo "FALHOU: $label após 45 tentativas; logs preservados" >&2
  return 1
}

reset_setup() {
  local sys="$1" cli
  if [[ "$sys" == kafka ]]; then cli=cliente; else cli=cliente-nats; fi
  capture "${sys}_reset_$(date +%s)" docker exec "$cli" /app/sdtool reset --sys "$sys"
  # Espera a remoção real; Kafka pode levar tempo para concluir a exclusão.
  local deleted=0
  for _ in $(seq 1 45); do
    if [[ "$sys" == kafka ]]; then
      if ! docker exec cliente sh -c '/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --describe --topic probe' >/dev/null 2>&1; then deleted=1; break; fi
    else
      if ! docker exec cliente-nats nats stream info PROBE >/dev/null 2>&1; then deleted=1; break; fi
    fi
    sleep 1
  done
  if [[ $deleted != 1 ]]; then echo "Falha: recurso $sys não desapareceu após reset" >&2; exit 1; fi
  retry_setup "$sys" "${sys}_setup_$(date +%s)"
}

capture host_info bash -c 'date -u +%Y-%m-%dT%H:%M:%SZ; uname -a; go version; docker version; docker compose version; git -C "$1" rev-parse HEAD' bash "$ROOT"
# Apenas leitura no repositório de trabalho, mas 'tidy' criará/atualizará go.sum e go.mod.
capture build_sdtool bash -c 'cd "$1/go" && mkdir -p bin && go mod tidy && go version && CGO_ENABLED=0 GOOS=linux GOARCH="$(go env GOARCH)" go build -o bin/sdtool .' bash "$ROOT"
[[ -s "$ROOT/go/bin/sdtool" ]] || { echo 'Binário não produzido'; exit 1; }
capture kafka_compose_up docker compose -f "$ROOT/kafka/docker-compose.yml" up -d
capture nats_compose_up docker compose -f "$ROOT/nats/docker-compose.yml" up -d
capture kafka_exec_check docker exec cliente test -x /app/sdtool
capture nats_exec_check docker exec cliente-nats test -x /app/sdtool
capture kafka_ps docker compose -f "$ROOT/kafka/docker-compose.yml" ps
capture nats_ps docker compose -f "$ROOT/nats/docker-compose.yml" ps

# E1: estados e líderes; 'setup' não apaga tópicos/streams já existentes.
retry_setup kafka e1_kafka_setup
capture e1_kafka_topic docker exec cliente sh -c '/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --describe --topic probe'
capture e1_kafka_quorum docker exec cliente sh -c '/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server "$BS" describe --status'
retry_setup nats e1_nats_setup
capture e1_nats_stream docker exec cliente-nats nats stream info PROBE

# E2: após reset explícito, são esperadas 10 mensagens sem histórico contaminante.
for sys in kafka nats; do
  reset_setup "$sys"
  if [[ "$sys" == kafka ]]; then cli=cliente; else cli=cliente-nats; fi
  capture "e2_${sys}_probe" docker exec "$cli" /app/sdtool probe --sys "$sys" --count 10 --interval 100ms --file /tmp/e2.csv
  capture "e2_${sys}_verify" docker exec "$cli" /app/sdtool verify --sys "$sys" --file /tmp/e2.csv
  capture "e2_${sys}_acked_copy" docker cp "$cli:/tmp/e2.csv" "$OUT/e2_${sys}_acked.csv"
done

# E3: as três condições são tomadas separadamente, cada uma com cinco rodadas.
reset_setup kafka
capture e3_kafka_acks_all docker exec cliente /app/sdtool bench --sys kafka --acks all --runs 5
reset_setup kafka
capture e3_kafka_acks_1 docker exec cliente /app/sdtool bench --sys kafka --acks 1 --runs 5
reset_setup nats
capture e3_nats_rf3 docker exec cliente-nats /app/sdtool bench --sys nats --runs 5

# E4: fontes independentes, novo reset e 100 IDs por sistema.
for sys in kafka nats; do
  reset_setup "$sys"
  if [[ "$sys" == kafka ]]; then cli=cliente; else cli=cliente-nats; fi
  capture "e4_${sys}_probe" docker exec "$cli" /app/sdtool probe --sys "$sys" --count 100 --interval 10ms --file /tmp/e4.csv
  capture "e4_${sys}_verify" docker exec "$cli" /app/sdtool verify --sys "$sys" --file /tmp/e4.csv
  capture "e4_${sys}_acked_copy" docker cp "$cli:/tmp/e4.csv" "$OUT/e4_${sys}_acked.csv"
done

cat <<EOM

Coleta E1-E4 finalizada em: $OUT
Execute 'python3 analisar_coletas.py "$OUT"' para resumir E3.
E5-E8 NÃO foram executados por este script. Consulte o arquivo
'evidencias_experimentos.md' e faça as injeções controladas com cinco ciclos
para E5 e E7, guardando TODOS os logs, timestamps e saídas do verify.
EOM
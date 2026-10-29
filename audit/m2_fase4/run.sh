#!/usr/bin/env bash
set -euo pipefail
OUT="${M2_F4_OUT:-artifacts/m2-fase4-complemento}"
mkdir -p "$OUT" go/bin
{
  date -u +'%Y-%m-%dT%H:%M:%SZ'
  printf 'commit=%s\n' "${GITHUB_SHA:-local}"
  docker version
  docker compose version
  go version
} > "$OUT/host_e_contexto.log" 2>&1
export COMPOSE_PROJECT_NAME=m2f4_kafka
trap 'docker compose -f kafka/docker-compose.yml ps >"$OUT/docker_estado_final.log" 2>&1 || true; docker compose -f kafka/docker-compose.yml down --remove-orphans >"$OUT/encerramento.log" 2>&1 || true' EXIT
docker compose -f kafka/docker-compose.yml up -d > "$OUT/cluster_start.log" 2>&1
for i in $(seq 1 60); do
  if docker exec cliente sh -c '/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --list' >"$OUT/prontidao.log" 2>&1;then break;fi
  sleep 5
  if [ "$i" = 60 ]; then echo 'cluster não pronto' >&2;exit 1;fi
done
# E1, conforme comando oficial de Tema 4 p.31 (pedidos, 3 partições, RF3)
docker exec cliente sh -c '/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --create --if-not-exists --topic pedidos --partitions 3 --replication-factor 3' >"$OUT/e1_create_pedidos.log" 2>&1
for i in $(seq 1 40);do
  docker exec cliente sh -c '/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --describe --topic pedidos' >"$OUT/e1_describe_pedidos.log" 2>&1 || true
  if python3 audit/m2_fase4/validate_e1.py "$OUT/e1_describe_pedidos.log" >/dev/null 2>&1;then break;fi
  if [ "$i" = 40 ];then echo 'não se confirmou ISR=3 nas 3 partições' >&2;exit 1;fi
  sleep 3
done
python3 audit/m2_fase4/validate_e1.py "$OUT/e1_describe_pedidos.log" > "$OUT/e1_validacao.json"
docker exec cliente sh -c '/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server "$BS" describe --status' >"$OUT/e1_metadata_quorum.log" 2>&1
for x in kafka1 kafka2 kafka3;do docker inspect "$x" --format '{{json .NetworkSettings.Networks}}' >"$OUT/e1_${x}_redes.json";done
# E4, somente dois cenários literais do roteiro: 1-30 sem chave e 1-30 mesma chave.
docker exec cliente /app/m2_f4_e4 --topic pedidos > "$OUT/e4_30_sem_chave_e_mesma_chave.log" 2>&1
python3 audit/m2_fase4/validate_e4.py "$OUT/e4_30_sem_chave_e_mesma_chave.log" > "$OUT/e4_validacao.json"
sha256sum "$OUT"/*.log "$OUT"/*.json > "$OUT/sha256_arquivos.txt"
echo "E1_E4_COMPLEMENTO_OK; logs=$OUT"

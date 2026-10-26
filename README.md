# Trabalho 2: Pub/Sub, Ênfase D (Kafka x NATS JetStream) com ferramenta em Go

## 0. Pré-requisitos
Docker + Compose, Git, Go 1.23+ (no host; o binário roda dentro dos contêineres).

## 1. Compilar a ferramenta (uma vez)
```bash
cd go
go mod tidy
CGO_ENABLED=0 GOOS=linux go build -o bin/sdtool .   # binário estático, vai para os contêineres
```
(Mac com Apple Silicon: acrescente GOARCH=arm64.)

## 2. Kafka (baseline, E1 a E8)
```bash
cd kafka && docker compose up -d
docker exec cliente /app/sdtool setup --sys kafka
docker exec cliente sh -c '/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --describe --topic probe'
docker exec cliente sh -c '/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server $BS describe --status'
```
Atalho: `alias sdk='docker exec cliente /app/sdtool'`

## 3. NATS JetStream (sistema similar)
```bash
cd nats && docker compose up -d
docker exec cliente-nats /app/sdtool setup --sys nats
docker exec cliente-nats nats stream info PROBE      # Leader e Replicas
```
Atalho: `alias sdn='docker exec cliente-nats /app/sdtool'`

## 4. Experimentos (rode em CADA sistema e compare)

| Exp | Kafka | NATS |
|---|---|---|
| **E3** replicação | `sdk bench --sys kafka --acks all --runs 5` e `--acks 1`; `docker exec kafka1 ls /tmp/kafka-logs` (idem kafka2/3) | `sdn bench --sys nats --runs 5`; `docker exec nats2 ls -R /data/jetstream` |
| **E4** ordem | `sdk reset --sys kafka; sdk setup --sys kafka; sdk probe --sys kafka --count 100 --interval 10ms; sdk verify --sys kafka` | idem com `--sys nats` |
| **E5** falha/quórum | Terminal A: `sdk probe --sys kafka --count 600 --interval 100ms`. Terminal B: descobrir líder (`--describe`), `docker stop kafkaX`; esperar; parar o 2º nó; depois `docker start`. Anote cada janela "RECUPEROU". Depois `verify`. | Terminal A: `sdn probe --sys nats --count 600`. Terminal B: líder via `nats stream info PROBE`; `docker stop natsX`; parar o 2º nó. |
| **E6** partição | `docker network ls` (ver `<pasta>_interna`); `docker network disconnect <proj>_interna kafkaX` (líder); observar o probe; reconectar com `docker network connect --ip 172.30.0.4X <proj>_interna kafkaX` | `docker network disconnect <proj>_interna natsX` (líder do stream) |
| **E7** recuperação | reconectar/`docker start`; `--describe` até o Isr voltar; meça o tempo (5x) | `nats stream info PROBE` até a réplica voltar sem "Lag"; meça (5x) |
| **E8** coordenação | `kafka-metadata-quorum.sh ... describe --status` (LeaderId, LeaderEpoch); pare o controlador ativo e repita; `--describe` mostra o LeaderEpoch da partição | `nats stream cluster step-down PROBE`; líder do meta-grupo: `wget -qO- http://172.33.0.51:8222/jsz` (campo `meta_cluster`); pare o líder do meta-grupo |

Janelas de indisponibilidade: repita 5x e calcule média e desvio (exigência R7).

### Variações úteis (garantias de entrega)
- `--idem` liga produtor idempotente (Kafka) / `Nats-Msg-Id` (NATS).
- `--retry 3` reenvia após erro; compare duplicatas no `verify` com e sem `--idem`.
- `--acks 1` (Kafka): mate o líder durante o probe e veja se aparecem PERDIDAS.
- FANTASMAS = o produtor viu erro (timeout), mas a mensagem foi gravada (falha ambígua).

## 5. Tabela comparativa final (preencher com evidências)
| Aspecto | Kafka | NATS JetStream |
|---|---|---|
| Unidade de replicação | partição (líder + seguidores, ISR) | stream (grupo Raft próprio) |
| Consenso (metadados) | KRaft (Raft) | meta-grupo Raft |
| Consenso (dados) | ISR + high watermark | Raft por stream, commit por maioria |
| Garantia de ordem | por partição | total dentro do stream |
| Tolerância (3 nós) | ISR: f+1; min.insync define | maioria: 1 falha |
| Sob partição (minoria) | líder isolado sai do ISR | minoria não confirma escrita |
| Comportamento sob falha | medido: ___ ms | medido: ___ ms |

## 6. Cuidados
- `docker compose down` apaga dados: use `stop/start` entre experimentos.
- Limpe entre rodadas: `sdtool reset` + `sdtool setup`.
- Registre a previsão ANTES de cada experimento.

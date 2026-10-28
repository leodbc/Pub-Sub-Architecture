# Trabalho 2: Pub/Sub, Ênfase D (Kafka x NATS JetStream) com ferramenta em Go

## 0. Pré-requisitos
Docker + Compose, Git, Go **1.26.2** (definido em `go/go.mod`; o binário roda dentro dos contêineres).

## 1. Compilar a ferramenta (uma vez)
```bash
cd go
go mod tidy
CGO_ENABLED=0 GOOS=linux go build -o bin/sdtool .   # binário estático, vai para os contêineres
```
(Mac com Apple Silicon: acrescente GOARCH=arm64.)

## 2. Kafka (baseline, E1 a E8)
O ambiente usa `apache/kafka-native:4.4.0` (GraalVM 25) nos três brokers e `apache/kafka:4.4.0` no cliente. A versão 4.1.0 apresentou `SIGSEGV` intermitente em startup após falha induzida; veja `docs/ci-incidente-2026-10-28.md` e o bug oficial KAFKA-20314. A versão nova exige repetir a bateria completa para manter a comparabilidade.
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
| **E8** coordenação | `kafka-metadata-quorum.sh ... describe --status` (LeaderId, LeaderEpoch); pare o controlador ativo e repita; `--describe` mostra líder/ISR, NÃO a época; use a API Metadata via `fase1_normativo --mode epochs` para observar a época da partição | `nats stream cluster step-down PROBE`; líder do meta-grupo: `wget -qO- http://172.33.0.51:8222/jsz` (campo `meta_cluster`); pare o líder do meta-grupo |

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

## 7. GitHub Actions e evidências verificáveis (M2, Ênfase D)

O Pull Request da branch de experimentos usa o workflow
[`.github/workflows/m2-experiments.yml`](.github/workflows/m2-experiments.yml):
ao abrir/atualizar um Pull Request, o GitHub provisiona um runner **Ubuntu 24.04 com Docker**, executando as etapas dependentes na ordem abaixo.

1. **smoke** — compila o `sdtool` no Go definido em `go.mod`; sobe clusters Kafka KRaft e NATS JetStream de três nós; verifica 12 produções e consumos em cada sistema.
2. **e1e4** — executa o coletor de `scripts/coletar_e1_e4.sh` e `scripts/analisar_coletas.py`; verifica E2/E4 e cinco execuções de E3 por configuração, sem erros; salva métricas e estatísticas amostrais.
3. **e5e7** — após E1–E4, provoca **cinco falhas de líder por sistema**, testa operações com dois e um nó, restaura os brokers e mede o retorno das réplicas ao ISR/quórum, além de gerar `sdtool verify` e CSV.
4. **e6e8** — após E1–E4, isola o líder da rede interna, mantendo a rede de clientes; restaura a rede e, em cinco repetições, observa eleição de coordenador e retorno à saúde.
5. **e9** — só executa se E1–E8 tiverem sido aprovados no Actions. Reúne artefatos dos experimentos e produz comparação Kafka × NATS **exigindo cinco observações reais em cada métrica temporal**, sem estimativas para dados ausentes.

Os artefatos `m2-smoke-*`, `m2-e1e4-*`, `m2-e5e7-*`, `m2-e6e8-*` e `m2-e9-*`
ficam disponíveis na página de **Actions → Execução → Artifacts** por 30 dias.
Logs brutos, tabelas CSV, descrições de cluster, resultados `verify` e resumos são
salvos inclusive em jobs com falha. Uma execução cancelada ou reprovada **não comprova**
a conclusão de seu experimento. Os resultados confirmados aparecem também na
conversa do Pull Request.

**Limites de validade acadêmica:** E2 requer o diagrama de sequência próprio e a
demonstração do caminho interno da operação. O E4 também foi ampliado para 90 mensagens distribuídas por três partições:
zero inversões por partição, com inversões na leitura global; ver
`e4_kafka_multi_verify.log`. O isolamento rigoroso de E6 posterior criou
um cliente minoritário em bridge Docker `--internal`, distinta do cliente
majoritário, corrigindo a limitação do experimento histórico. A partição de rede E6 *histórica* isolava apenas o canal interno, podendo
permitir redirecionamento pela metadata do Kafka; a extensão rigorosa posterior
separou os clientes em redes distintas. A limitação histórica permanece registrada. Essas limitações devem constar no relatório, junto à análise CAP/PACELC.
O CI não substitui o relatório de 15–25 páginas, o seminário e as respostas Q1–Q14/S1–S4.

Em um terminal local para reproduzir apenas a compilação travada em checksum:

```bash
cd go
go mod download
CGO_ENABLED=0 GOOS=linux go build -mod=readonly -o bin/sdtool .
```

A aplicação usa endereços e portas internos de laboratório sem autenticação;
não conecte esses clusters a redes públicas ou de produção.

## 8. Provas normativas finais e preservação (Fase 1)

O PDF oficial do Trabalho M2, Tema 4 (páginas 31–33), exige evidências complementares.
O workflow [m2-fase1-final.yml](.github/workflows/m2-fase1-final.yml) executa SOMENTE os testes novos em runner Ubuntu com Docker:

- **E2:** 12 chaves × 3 mensagens = 36 registros. As linhas PRODUCE e CONSUME mostram chave, partição e offset; o teste falha se houver divergência ou troca de partição para a mesma chave.
- **E5:** após derrubar dois controladores KRaft, tenta CreateTopics no único sobrevivente. Classifica timeout como ausência de resposta do cliente e verifica se o tópico aparece depois do restabelecimento; ambos os resultados são preservados. Não pressupõe que o tópico tenha sido criado durante a partição.
- **E7:** consumidor de grupo com autocommit desligado confirma offset de retomada 2, é interrompido sem LeaveGroup e um segundo processo do MESMO grupo recupera os registros offsets 2–5. O teste não é sinônimo de catch-up do ISR.
- **E8:** compara época de liderança da PARTIÇÃO, obtida por kadm.ListTopics (Metadata API), com época do CONTROLADOR, obtida por kafka-metadata-quorum describe --status. São contadores independentes.

**E3:** segundo a p. 31, o experimento específico exige réplica em três nós e comparação de vazão/latência acks=1 contra acks=all com cinco repetições. A instrumentação complementar diferencia ACK e visibilidade do callback; não inventa o instante da propagação até cada follower.

**Retenção:** artifacts GitHub Actions expiram após 30 dias (27/11/2026 para as execuções originais), antes da entrega 28/11. Um job condicionado à aprovação dos novos testes baixa ZIPs originais especificados, valida SHA-256 ANCORADOS no script, examina padrões de credenciais e publica uma release com ZIPs e manifesto por arquivo. O tag é `m2-fase1-evidencias-2026-10-28`. O pacote da [release publicada](https://github.com/leodbc/Pub-Sub-Architecture/releases/tag/m2-fase1-evidencias-2026-10-28) foi verificado no run #40775447916; não sobrescrever assets nem apagar evidências prévias.

A documentação canônica com observações, limites científicos e matriz R1–R10 está em [docs/evidencias_experimentos.md](docs/evidencias_experimentos.md). O relatório final e seminário permanecem tarefas futuras.

# Roteiro assistido de E5–E8 — executar no host com Docker

**Estado:** instruções preparadas, **não executadas**. Ler `evidencias_experimentos.md` antes. Os comandos abaixo param e isolam nós: usar **apenas o laboratório** do trabalho. Não executar em infraestrutura de produção. Nunca declarar uma rodada concluída sem logs e timestamps.

## 0. Inicialização e registro de evidências

Na raiz do clone, em um terminal **Bash**, depois de executar E1–E4 ou de verificar os dois clusters:

```bash
set -o pipefail
ROOT="$PWD"
OUT="$ROOT/coleta_falhas_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT"
# Ações administrativas sempre no host; o sdtool é /app/sdtool dentro dos contêineres.
logcmd() { local nome="$1"; shift; { date -u '+%Y-%m-%dT%H:%M:%SZ'; printf 'COMMAND: '; printf '%q ' "$@"; echo; "$@"; } 2>&1 | tee "$OUT/$nome.log"; }
nowms() { python3 -c 'import time; print(time.time_ns()//1000000)'; }
logcmd baseline_ps docker ps --format '{{.Names}} {{.Status}}'
```

**Importante:** `logcmd` não mantém `pipefail` em shells que não o tenham ativado; inicie com `set -o pipefail`. Copie também `git rev-parse HEAD`, `docker version`, `go version` e os arquivos CSV originais após cada rodada. Não use o mesmo `/tmp/acked.csv` para repetições diferentes.

## E5 — Falha de um nó e depois dois nós: 5 rodadas em cada sistema

Para **cada sistema**, execute cinco rodadas independentes. Antes de cada rodada, restaure todos os nós, aguarde estabilidade e confirme ISR com 3 membros (Kafka) ou leader + duas réplicas ativas (NATS). Anote o líder **atual**, que pode mudar de uma rodada para outra.

### Kafka (repita r=1...5)

```bash
r=1 # ajustar 1..5 nos DOIS terminais utilizados para esta rodada
# 1) Confirmar líder da partição, ISR e líder KRaft:
logcmd "e5_kafka_r${r}_topologia_antes" docker exec cliente sh -c '/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --describe --topic probe'
logcmd "e5_kafka_r${r}_quorum_antes" docker exec cliente sh -c '/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server "$BS" describe --status'
# 2) Em uma janela separada, iniciar probe; NÃO reutilizar nome de arquivo:
docker exec cliente /app/sdtool probe --sys kafka --count 600 --interval 100ms --file "/tmp/e5_kafka_r${r}.csv" 2>&1 | tee "$OUT/e5_kafka_r${r}_probe.log"
# 3) Em outro terminal, durante o probe, registre instantes em:
date -u '+%Y-%m-%dT%H:%M:%SZ' | tee -a "$OUT/e5_kafka_r${r}_eventos.log"
docker stop kafkaX | tee -a "$OUT/e5_kafka_r${r}_eventos.log"   # X: LÍDER OBSERVADO
# Inspecione se houve novo líder e ACKs; só depois:
docker stop kafkaY | tee -a "$OUT/e5_kafka_r${r}_eventos.log"   # Y: segundo nó distinto
# Registrar ausência de quórum; restaurar os DOIS:
docker start kafkaX kafkaY | tee -a "$OUT/e5_kafka_r${r}_eventos.log"
# 4) Após terminar o probe e estabilizar:
docker exec cliente /app/sdtool verify --sys kafka --file "/tmp/e5_kafka_r${r}.csv" 2>&1 | tee "$OUT/e5_kafka_r${r}_verify.log"
docker cp "cliente:/tmp/e5_kafka_r${r}.csv" "$OUT/e5_kafka_r${r}_acked.csv"
```

Os blocos **2 e 3 devem ocorrer em terminais distintos**; não executar um após o outro na mesma sessão síncrona. O operador deve registrar precisamente os timestamps de `docker stop/start`, primeiro erro e primeiro ACK de recuperação. O probe pode terminar antes da restauração: invalide a rodada e repita com maior `--count` se isso acontecer.

### NATS (repita r=1...5)

Substitua `cliente` por `cliente-nats`, `kafka` por `nats`, `kafkaX` por `natsX`, `kafkaY` por `natsY` e colete `docker exec cliente-nats nats stream info PROBE` antes e após as falhas. Identifique `Leader` no status real do stream, **não** suponha que seja `nats1`. O caminho do CSV é `/tmp/e5_nats_r${r}.csv` e o nome do log é `e5_nats_r${r}_verify.log`.

**Métricas de E5:** duração da interrupção detectável após uma falha e após a segunda (ambas com timestamps e logs). Caso haja mudança de líder sem perda observável de ACK, relatar precisamente isso; não inferir que a duração foi zero se a resolução de medição não permite.

## E6 — Partição de rede interna

1. Identifique líder atual por `kafka-topics --describe` ou `nats stream info PROBE`.
2. Identifique o nome real da rede interna com `docker network ls`, tipicamente `kafka_interna` ou `nats_interna`; confirme seu subnet/IP com `docker network inspect`.
3. Inicie o `probe` em um terminal e deixe gravando em arquivo CSV e log específicos de E6.
4. No outro terminal, execute a desconexão e depois restaure, guardando timestamps:

```bash
# EXEMPLO Kafka com kafka2 como líder CONFERIDO — adaptar à observação:
docker network disconnect kafka_interna kafka2
# Registre os estados do controlador, ISR e mensagens durante a partição.
docker network connect --ip 172.30.0.42 kafka_interna kafka2
# EXEMPLO NATS com nats2 como líder CONFERIDO — adaptar à observação:
docker network disconnect nats_interna nats2
docker network connect --ip 172.32.0.52 nats_interna nats2
```

**Não use simultaneamente os exemplos Kafka e NATS se apenas um sistema estiver sob teste.** Para Kafka, mantenha a rede de clientes `172.31...`; NATS `172.33...`. A remoção da rede interna pode **não** equivaler ao isolamento total do nó, pois ainda há conectividade pela rede de clientes; registrar as adjacências remanescentes e interpretar o CAP com essa limitação.

5. Após restabelecimento, execute `verify` com o mesmo `--file` do probe, grave logs integrais, confira cluster e dados.

## E7 — Catch-up: cinco ciclos de recuperação **adicionais** por sistema

Para cada sistema e r=1...5: com cluster saudável, descubra qual nó pode ser interrompido preservando quorum; escolha um **seguidor/réplica** quando possível. Gere algumas centenas de mensagens enquanto ele está ausente (sem comprometer a maioria). Tome `T0` no instante imediatamente após solicitar a restauração; consulte status em intervalos registrados até o **retorno ao ISR** (Kafka) ou ao status de réplica ativa com `Lag=0` (NATS). Tome `T1` na primeira leitura que comprovar isso.

```bash
# Temporização de relógio do host em milissegundos (Python padrão):
nowms
# Kafka: inspecionar repetidamente (com timestamps) e salvar saídas:
date -u '+%Y-%m-%dT%H:%M:%SZ'
docker exec cliente sh -c '/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --describe --topic probe'
# NATS: idem:
date -u '+%Y-%m-%dT%H:%M:%SZ'
docker exec cliente-nats nats stream info PROBE
```

Uma leitura a cada 200–500 ms é um bom ponto inicial, mas registrar intervalo escolhido, desvios e eventuais consultas com erro. **Tempo de catch-up = T1 − T0**, com precisão limitada pelo período de consulta; não substituir pelo texto `RECUPEROU` do probe. Armazenar logs de cinco ciclos e resultados em `medicoes_falhas.csv`. Enquanto o seguidor está ausente, continuar escrevendo uma quantidade comparável de mensagens por rodada para que exista atraso a recuperar; sem backlog, o tempo medido pode refletir só a reentrada no grupo.

## E8 — Eleição de coordenadores

Kafka (apenas depois de E7 e com todas as réplicas saudáveis):

```bash
docker exec cliente sh -c '/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server "$BS" describe --status'
# Localize LeaderId, pare APENAS o controlador correspondente e consulte novamente.
docker stop kafkaX
sleep 5
docker exec cliente sh -c '/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server "$BS" describe --status'
docker start kafkaX
```

Compare `LeaderId` e `LeaderEpoch` do **quórum de metadados**, observando que líder da partição Kafka é outra entidade.

NATS: primeiro capture `nats stream info PROBE`; para a eleição **do stream**, use `docker exec cliente-nats nats stream cluster step-down PROBE` e capture novo `Leader`. Para o **meta-grupo**, consulte `/jsz` pelo monitor HTTP habilitado na porta 8222 dentro da rede `clientes`, identificando `meta_cluster` e líder atual. Então pare somente esse nó, consulte `/jsz` em um nó ainda ativo e restaure. Se `wget` não estiver disponível, use uma ferramenta HTTP já instalada ou um contêiner auxiliar autorizado na mesma rede.

## Como preencher a planilha de medições para o analisador

Crie `medicoes_falhas.csv` na pasta dos logs com **cabeçalho**:

```csv
experimento,sistema,cenario,repeticao,valor_ms,evidencia
```

Adicione uma linha por medida real, com cenário `1_no` / `2_nos` para E5 e `catchup` para E7. Repetições de 1 a 5, `valor_ms` numérico com ponto decimal e `evidencia` apontando para arquivo de log com timestamps. **Não use valores hipotéticos**. No fim:

```bash
python3 analisar_coletas.py "$OUT"
```

O analisador só publica média e DP quando existem **cinco linhas válidas** por cenário. Os dados brutos, CSVs e saídas de `verify` precisam acompanhar o relatório final.
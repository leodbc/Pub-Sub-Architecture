# Evidências experimentais — Kafka KRaft × NATS JetStream

**Trabalho M2 — Sistemas Distribuídos / UNIVALI**  
**Tema 4:** mensageria Publish–Subscribe  
**Ênfase D:** sistema similar (NATS JetStream)  
**Data de preparação:** 28/10/2026  
**Repositório auditado:** [leodbc/Pub-Sub-Architecture](https://github.com/leodbc/Pub-Sub-Architecture), ramo `main`  
**Estado:** **PROTOCOLO PREPARADO — EXPERIMENTOS NÃO EXECUTADOS NESTA SESSÃO**.

> **Integridade das evidências.** Esta sessão teve acesso aos arquivos do GitHub, mas **não** ao Docker Engine dos computadores do grupo. Não foi possível observar os processos Kafka/NATS, executar o `sdtool` nos contêineres, coletar logs de `verify` nem cronometrar eventos. Toda célula `ND` significa *não disponível/não medido* — nunca zero. Previsões abaixo são hipóteses **a testar**, não resultados. O caminho `/workspace/scratch/` criado nesta sessão pertence ao ambiente isolado do assistente, não ao computador do aluno.

## 1. Rastreabilidade, ambiente e bloqueios

| Item | Evidência documental / status |
|---|---|
| README | [README.md](https://github.com/leodbc/Pub-Sub-Architecture/blob/main/README.md), blob `5fbf69dcc16e6fdddc74fe79a5084b2443d00106` |
| Fonte da ferramenta | `go/main.go`, `go/backend.go`, `go/kafka.go`, `go/nats.go` lidos no GitHub |
| Go do projeto | `go/go.mod` declara `go 1.26.2`; README declara Go `1.23+` (**inconsistência a resolver**) |
| Executável `sdtool` | **Não verificado no host/contêiner**. Ausente do rastreamento do repositório (sem `go/bin/sdtool` em `main`). |
| Teste de compilador local ao assistente | Go `1.23.2`: a análise de um `go.mod` com a diretiva idêntica retornou `go.mod requires go >= 1.26.2` com `GOTOOLCHAIN=local`. **Não é tentativa de compilar o projeto completo.** |
| Docker local do assistente | Sem executável `docker` e sem `/var/run/docker.sock`; nenhum cluster iniciado aqui. |
| Kafka | `apache/kafka-native:4.1.0`, três nós `kafka1..3`, broker+controller, RF planejado 3, `min.insync.replicas=2`, redes `172.30.0.0/24` (interna) e `172.31.0.0/24` (clientes). |
| NATS | `nats:2.10-alpine`, três nós `nats1..3`, JetStream habilitado e RF planejado 3; redes `172.32.0.0/24` (interna) e `172.33.0.0/24` (clientes). |
| Clientes | `cliente` (Kafka) e `cliente-nats` (NATS); `../go/bin` montado como `/app:ro` em ambos. Executável esperado: **`/app/sdtool`**, não `/go/bin/sdtool` dentro dos contêineres. |
| Capturas brutas | PENDENTE: registrar versão do host, `docker version`, `docker compose version`, `docker compose ps`, `docker inspect`, timestamps e `sdtool`/commit. |

### Pré-voo obrigatório no computador que tem Docker

Executar na raiz do repositório clonado (sem supor que o binário já existe):

```bash
go version
cat go/go.mod
cd go && go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o bin/sdtool . && cd ..
ls -lh go/bin/sdtool
(cd kafka && docker compose up -d)
(cd nats && docker compose up -d)
docker exec cliente test -x /app/sdtool && echo 'Kafka: sdtool executável'
docker exec cliente-nats test -x /app/sdtool && echo 'NATS: sdtool executável'
docker compose -f kafka/docker-compose.yml ps
docker compose -f nats/docker-compose.yml ps
```

Se `go mod tidy` falhar, corrigir a versão da toolchain em ambiente conectado, **sem rebaixar `go.mod` automaticamente**; verificar primeiro as versões mínimas das dependências. Não avançar para injeção de falhas sem cluster saudável. **Não usar `docker compose down` entre testes**, pois o projeto não declara volumes persistentes nomeados para os dados.

## 2. Protocolo experimental por experimento

### E1 — Inicialização e topologia

**Previsão — Kafka:** três processos combinados broker/controller; controlador KRaft eleito; tópico `probe` com RF=3, uma partição, líder e ISR com 3 membros quando estabilizado.  
**Previsão — NATS:** três servidores na malha; `PROBE` com 3 réplicas e líder de stream; meta-grupo Raft eleito.

**Procedimento:** compilar; subir Compose; `docker exec cliente /app/sdtool setup --sys kafka`; `docker exec cliente-nats /app/sdtool setup --sys nats`; registrar:

```bash
docker exec cliente sh -c '/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --describe --topic probe'
docker exec cliente sh -c '/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server "$BS" describe --status'
docker exec cliente-nats nats stream info PROBE
docker ps --format '{{.Names}} {{.Status}}'
```

**Observado:** ND. **Evidências:** `e1_kafka_*.log` e `e1_nats_*.log`, PENDENTES.  
**Análise:** confrontar líderes, réplicas e ISR informados com a previsão; distinguir controlador KRaft de líder da partição e meta-líder NATS de líder do stream.

### E2 — Caminho de escrita/leitura

**Previsão — Kafka:** produtor encaminha mensagem ao líder da partição; seguidores replicam; registros tornam-se elegíveis à leitura segundo high watermark e condições de replicação. **NATS:** publish alcança líder de stream, é replicado pelo grupo Raft e confirmado por quórum; consumidor ordenado lê a sequência.

**Procedimento:** após `reset` + `setup` em ambiente isolado, executar `probe --count 10 --interval 100ms --file /tmp/e2.csv` e `verify --file /tmp/e2.csv` em cada cliente, capturar posições `p0@offset` / `seqN`, status de réplicas, e saídas das ferramentas administrativas. **Atenção:** a saída de `verify`, isoladamente, não comprova o mecanismo de commit/high watermark; correlacionar com metadados e documentação.

**Observado:** ND. **Evidências:** `e2_*_probe.log`, `e2_*_verify.log`, `e2_*_topologia.log`, PENDENTES.  
**Análise:** descrever o caminho inferido, distinguindo o mecanismo documentado dos sinais diretamente observáveis.

### E3 — Replicação, latência e vazão (R7)

**Previsão — Kafka:** `acks=all` costuma apresentar latência maior, com maior proteção a perdas de escritas confirmadas, do que `acks=1` sob condições equivalentes; medir, não presumir a diferença. **NATS:** publish do JetStream com `Replicas=3` é confirmado segundo quórum; **não possui parâmetro `acks=1` equivalente**.

```bash
docker exec cliente /app/sdtool bench --sys kafka --acks all --runs 5
docker exec cliente /app/sdtool bench --sys kafka --acks 1 --runs 5
docker exec cliente-nats /app/sdtool bench --sys nats --runs 5
```

**Observado:** ND. **Evidências:** arquivos de `bench` com as cinco linhas `run 1` ... `run 5` para cada configuração, PENDENTES. **Análise:** média e desvio padrão *amostral* por configuração; comparar latência e vazão apenas com mensagens, tamanho, infraestrutura e concorrência constantes; registrar erros por repetição.

### E4 — Consistência e ordem

**Previsão — Kafka:** com uma partição e chave fixa, os registros visíveis a um consumidor mantêm ordem de offset; o teste **não demonstra** ordenação global entre várias partições. **NATS:** uma sequência de stream ordenada é observável ao consumidor ordenado.

```bash
# Em ambiente experimental, após reset/setup, por sistema:
docker exec cliente /app/sdtool probe --sys kafka --count 100 --interval 10ms --file /tmp/e4.csv
docker exec cliente /app/sdtool verify --sys kafka --file /tmp/e4.csv
docker exec cliente-nats /app/sdtool probe --sys nats --count 100 --interval 10ms --file /tmp/e4.csv
docker exec cliente-nats /app/sdtool verify --sys nats --file /tmp/e4.csv
```

**Observado:** ND. **Evidências:** duas saídas integrais de `verify` + CSV de ACKs, PENDENTES.  
**Análise:** reportar `PERDIDAS`, `DUPLICADAS`, `FORA DE ORDEM`, `FANTASMAS` **conforme saída real**; investigar divergências. Kafka: observar que os IDs do payload não são offsets.

### E5 — Falha e quórum (R7)

**Previsão — Kafka:** com ISR=3, `acks=all` e `min.insync.replicas=2`, a perda de um nó poderá manter escritas após eleição/ajuste do ISR; com dois nós parados, espera-se indisponibilidade para novas escritas confirmadas, sobretudo com perda de quórum KRaft. **NATS:** RF=3 tolera uma falha, mas duas falhas eliminam a maioria e impedem commit do stream.

**Procedimento:** por sistema e **em cada uma das 5 repetições independentes**, identificar líder atualizado, iniciar `probe` com arquivo CSV exclusivo, registrar timestamp e saída antes/depois de parar o primeiro nó, depois o segundo, e restaurá-los com `docker start`. Registrar **separadamente** tempo até recuperar ACKs e duração da indisponibilidade durante perda de um ou dois nós. Após restabelecer quórum e estabilizar, executar `verify` usando o mesmo `--file` do probe e armazenar sua saída integral. Não confundir interrupção causada por detecção/eleição com indisponibilidade sustentada por ausência de quórum.

**Observado:** ND. **Evidências:** 5 logs de `probe`, 5 logs de `verify` e timestamps/logs de `docker stop/start` **por sistema**, PENDENTES.  
**Análise:** distinguir mensagens não confirmadas de perdas de mensagens antes confirmadas; tratar timeout como resultado ambíguo, não prova de perda.

### E6 — Partição de rede

**Previsão:** a partição minoritária perde capacidade de confirmar gravações para o log replicado sob requisitos de consistência/commit; possíveis janelas de reeleição e reconexão. O teorema CAP aplica-se sob partição efetiva; não é um rótulo único e permanente para todo o produto.

**Procedimento:** localizar rede real com `docker network ls` e líder atual; isolar **a rede interna** (Kafka geralmente `kafka_interna`, NATS geralmente `nats_interna`) preservando a rede de clientes. Antes de desconectar, registrar `docker network inspect`; executar probe em paralelo. Reatar com **IP interno correto**: Kafka `172.30.0.4X`, NATS `172.32.0.5X`, de acordo com o nó. Verificar novamente topologia, quórum, dados e `verify`.

**Observado:** ND. **Evidências:** log da desconexão/reconexão e probe/verify, PENDENTES.  
**Análise:** verificar se houve isolamento realmente relevante ao plano de dados e/ou controle. Nunca inferir consistência forte apenas de ausência de duplicatas.

### E7 — Recuperação e catch-up (R7)

**Previsão — Kafka:** o nó restaurado sincroniza e retorna ao ISR quando alcançar estado adequado. **NATS:** réplica reassocia ao grupo do stream e reduz `Lag` até sincronizar.

**Procedimento:** fazer **5 ciclos independentes para cada sistema**, com carga e estados iniciais comparáveis. Registrar `T0` do `docker start` ou `docker network connect` e `T1` da **primeira observação confirmada de retorno da réplica ao ISR** (Kafka) / `Lag` zerado e réplica ativa (NATS). Relatar resolução e frequência da amostragem; evitar confundir `RECUPEROU` do produtor com `catch-up` da réplica. Arquivar todas as saídas intermediárias de `kafka-topics --describe` e `nats stream info`.

**Observado:** ND. **Evidências:** 5 séries temporais por sistema, PENDENTES.  
**Análise:** comparar latência de recuperação (ms) e explicar efeitos de quantidade de dados, polling e timeout.

### E8 — Coordenação e reeleição

**Previsão — Kafka:** parada do controlador ativo leva à eleição de novo líder de metadados quando resta maioria; verificar alteração de `LeaderId` e época *do controlador*. **NATS:** parada do líder de meta-grupo leva à reeleição com maioria; liderança do meta-grupo e do stream podem estar em nós diferentes.

**Procedimento — Kafka:** `kafka-metadata-quorum.sh --bootstrap-server "$BS" describe --status` antes/depois de parar o controlador identificado; registrar `LeaderId`/`LeaderEpoch`, restaurar. Consultar `kafka-topics --describe` para líder da partição, **sem tratar epoch da partição como sinônimo do epoch do controlador**. **NATS:** `nats stream cluster step-down PROBE` testa eleição de líder de stream; para *meta-grupo*, descobrir líder no endpoint de monitoração `/jsz` e então interromper o nó correspondente; consultar `/jsz` antes/depois e restaurar.

**Observado:** ND. **Evidências:** metadados com timestamp antes/depois e logs de stop/start, PENDENTES.  
**Análise:** explicar distinção controle versus dados e quórum necessário para eleição.

### E9 — Ênfase D: comparação Kafka × NATS

**Previsão comparativa:** Kafka replica partições com ISR/high watermark e mantém metadados via KRaft; NATS usa grupos Raft para metadados e streams. Ambos exigem maioria para seus respectivos grupos de consenso, mas o protocolo de ACK e o limiar de confirmação dos dados do Kafka não devem ser descritos como idênticos ao Raft do stream NATS.

**Procedimento:** consolidar E1–E8 em matriz de arquitetura, ACK, ordenação, quorum, perdas, duplicatas, latência/vazão, falhas, partição, recuperação, eleição e limitações; anexar saídas reais e valores estatísticos. **Observado:** ND. **Análise:** PENDENTE até execução real.

## 3. R7 — Tabelas de cinco repetições (TODOS os valores são ND até coleta)

### E3 — latência média por rodada (ms) e vazão (mensagens/s)

| Repetição | Kafka `acks=all` lat. / vazão | Kafka `acks=1` lat. / vazão | NATS RF3 lat. / vazão |
|---:|---:|---:|---:|
| 1 | ND / ND | ND / ND | ND / ND |
| 2 | ND / ND | ND / ND | ND / ND |
| 3 | ND / ND | ND / ND | ND / ND |
| 4 | ND / ND | ND / ND | ND / ND |
| 5 | ND / ND | ND / ND | ND / ND |
| **Média** | **ND / ND** | **ND / ND** | **ND / ND** |
| **DP amostral** | **ND / ND** | **ND / ND** | **ND / ND** |

### E5 — indisponibilidade (ms), duas situações por sistema

| Repetição | Kafka: após 1ª falha | Kafka: após 2ª falha | NATS: após 1ª falha | NATS: após 2ª falha |
|---:|---:|---:|---:|---:|
| 1 | ND | ND | ND | ND |
| 2 | ND | ND | ND | ND |
| 3 | ND | ND | ND | ND |
| 4 | ND | ND | ND | ND |
| 5 | ND | ND | ND | ND |
| **Média** | **ND** | **ND** | **ND** | **ND** |
| **DP amostral** | **ND** | **ND** | **ND** | **ND** |

> Se a primeira falha não provocar indisponibilidade detectável, registrar **`0 ms (medido)`** somente quando houver probe contínuo e evidência suficiente. Se a janela não for detectável ou o teste tiver falhado, registrar `não detectada`/`inválida`, não usar zero sem justificativa. Para a segunda falha, medir duração até a primeira confirmação após recuperação; se não recuperar dentro da janela, registrar como dado censurado.

### E7 — retorno da réplica ao ISR / catch-up (ms)

| Repetição | Kafka | NATS |
|---:|---:|---:|
| 1 | ND | ND |
| 2 | ND | ND |
| 3 | ND | ND |
| 4 | ND | ND |
| 5 | ND | ND |
| **Média** | **ND** | **ND** |
| **DP amostral** | **ND** | **ND** |

**Cálculo:** para valores medidos `x₁...x₅`, `média = Σxᵢ/5`; `desvio padrão amostral = √[Σ(xᵢ−média)²/4]`. Se uma rodada falhar ou não for observável, documentar e repetir para obter **cinco rodadas válidas**, sem apagar as falhas do histórico.

## 4. Registro de logs reais — `sdtool verify` (não preenchido)

Os blocos abaixo **não contêm nenhuma saída inventada**. Colar a saída completa real capturada com `tee`, preservando o nome do arquivo, comando, timestamp, versão e SHA do código. A presença de `PERDIDAS: 0` etc. só poderá ser registrada se a ferramenta realmente informar esses valores.

#### Kafka E4 — `docker exec cliente /app/sdtool verify --sys kafka --file /tmp/e4.csv`

```text
[AINDA NÃO EXECUTADO / SEM LOG]
```

#### NATS E4 — `docker exec cliente-nats /app/sdtool verify --sys nats --file /tmp/e4.csv`

```text
[AINDA NÃO EXECUTADO / SEM LOG]
```

#### Kafka E5 — 5 verificações

```text
[AINDA NÃO EXECUTADAS / SEM LOGS; anexar e5_kafka_r1_verify.log ... e5_kafka_r5_verify.log]
```

#### NATS E5 — 5 verificações

```text
[AINDA NÃO EXECUTADAS / SEM LOGS; anexar e5_nats_r1_verify.log ... e5_nats_r5_verify.log]
```

#### Kafka/NATS E6 e E7

```text
[AINDA NÃO EXECUTADOS / SEM LOGS; anexar saída integral do probe e verify de cada rodada]
```

## 5. Resumo das evidências e conclusão provisória

| E | Previsão registrada | Execução real | Evidência de observação |
|---|---|---|---|
| E1 | Sim, por sistema | Não | ND |
| E2 | Sim, por sistema | Não | ND |
| E3 | Sim; 5 repetições planejadas | Não | ND |
| E4 | Sim, por sistema | Não | ND |
| E5 | Sim; 5 repetições planejadas | Não | ND |
| E6 | Sim, por sistema | Não | ND |
| E7 | Sim; 5 repetições planejadas | Não | ND |
| E8 | Sim, por sistema | Não | ND |
| E9 | Sim; depende de E1–E8 | Não | ND |

### Limitações metodológicas identificadas na implementação

1. `go.mod` não lista dependências diretas, e o repositório não traz `go.sum`. O `go mod tidy` irá resolver versões atuais no momento da execução; registrar o arquivo gerado, as versões resolvidas e o commit, ou fixar dependências antes da campanha.
2. `bench` usa cinco *rodadas* e já apresenta média/desvio amostral, mas **não** mede cinco ciclos de falha ou sincronização. Para E5 e E7, coletar cinco eventos independentes.
3. O `verify` compara os IDs únicos observados com o arquivo de ACKs daquela execução. Ele não prova *exactly-once* ponta-a-ponta, nem substitui auditoria de offsets e períodos sob partição.
4. Em `bench`, o payload repete IDs (`i=0..n−1`) a cada rodada; portanto, evitar rodar `verify` sobre o tópico que contém resultados do `bench`. Recriar tópico/stream antes dos experimentos de integridade.
5. O código Kafka fixa a chave `"k"`; a configuração inicial fixa **uma partição**. Assim, E4 comprova apenas uma única ordem local; não prova, por si, o contraste interpartições de S1.
6. `docker stop/start` preserva os dados dos contêineres existentes, mas `docker compose down` seguido de `up` pode recriá-los, perdendo dados sem volumes nomeados. Realizar ações destrutivas só após coletar evidências.
7. `min.insync.replicas=2` **não** significa que `acks=all` espera sempre exatamente duas réplicas. Havendo ISR=3, Kafka espera as réplicas integrantes do ISR; o mínimo define o limite inferior para escritas `acks=all`.
8. `RECUPEROU` do `probe` marca retomada de ACK observada; não é um cronômetro confiável de retorno ao ISR ou redução de lag a zero. Capturar indicadores específicos para E7.
9. NATS e Kafka têm diferentes modelos de ACK. O benchmark deve comparar as garantias reais oferecidas por cada configuração, sem chamar `acks=1` e `RF=3` do NATS de modos equivalentes.
10. `sdtool reset` do Kafka pode emitir aviso e prosseguir com código de saída zero; confirmar a efetiva exclusão e recriação do tópico antes de registrar um experimento independente.

### Referências técnicas de apoio

- [README do trabalho](https://github.com/leodbc/Pub-Sub-Architecture/blob/main/README.md) e arquivos `go/*.go`, `kafka/docker-compose.yml`, `nats/docker-compose.yml` (repositório do aluno).
- [Kafka — configuração `min.insync.replicas` (API 4.1)](https://kafka.apache.org/41/javadoc/constant-values.html).
- [NATS — JetStream Clustering](https://docs.nats.io/running-a-nats-service/configuration/clustering/jetstream_clustering).
- [Go — seleção de toolchains](https://go.dev/doc/toolchain).

**Próximo estado possível:** realizar a coleta na máquina que possui Docker, preencher dados e incorporar as saídas originais do `verify`. **Não utilizar este protocolo como relatório de resultados executados.**
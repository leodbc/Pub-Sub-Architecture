# Resumo estatístico das coletas

Desvio padrão **amostral** (n-1). Não imputa valores ausentes.

## E3 — extração dos logs reais do `sdtool bench`

| Sistema/modo | Repetição | Latência média (ms) | Vazão (msg/s) | Erros | Evidência |
|---|---:|---:|---:|---:|---|
| Kafka acks=all | 1 | ND | ND | ND | `e3_kafka_acks_all.log` ausente/incompleto |
| Kafka acks=all | 2 | ND | ND | ND | `e3_kafka_acks_all.log` ausente/incompleto |
| Kafka acks=all | 3 | ND | ND | ND | `e3_kafka_acks_all.log` ausente/incompleto |
| Kafka acks=all | 4 | ND | ND | ND | `e3_kafka_acks_all.log` ausente/incompleto |
| Kafka acks=all | 5 | ND | ND | ND | `e3_kafka_acks_all.log` ausente/incompleto |
| **Kafka acks=all: média ± DP** | **5** | **ND (exigem-se 5 medições válidas)** | **ND (exigem-se 5 medições válidas)** | — | — |
| Kafka acks=1 | 1 | ND | ND | ND | `e3_kafka_acks_1.log` ausente/incompleto |
| Kafka acks=1 | 2 | ND | ND | ND | `e3_kafka_acks_1.log` ausente/incompleto |
| Kafka acks=1 | 3 | ND | ND | ND | `e3_kafka_acks_1.log` ausente/incompleto |
| Kafka acks=1 | 4 | ND | ND | ND | `e3_kafka_acks_1.log` ausente/incompleto |
| Kafka acks=1 | 5 | ND | ND | ND | `e3_kafka_acks_1.log` ausente/incompleto |
| **Kafka acks=1: média ± DP** | **5** | **ND (exigem-se 5 medições válidas)** | **ND (exigem-se 5 medições válidas)** | — | — |
| NATS RF3 | 1 | ND | ND | ND | `e3_nats_rf3.log` ausente/incompleto |
| NATS RF3 | 2 | ND | ND | ND | `e3_nats_rf3.log` ausente/incompleto |
| NATS RF3 | 3 | ND | ND | ND | `e3_nats_rf3.log` ausente/incompleto |
| NATS RF3 | 4 | ND | ND | ND | `e3_nats_rf3.log` ausente/incompleto |
| NATS RF3 | 5 | ND | ND | ND | `e3_nats_rf3.log` ausente/incompleto |
| **NATS RF3: média ± DP** | **5** | **ND (exigem-se 5 medições válidas)** | **ND (exigem-se 5 medições válidas)** | — | — |

## E5/E7 — medições manuais com evidência de timestamp

Opcional: criar `medicoes_falhas.csv` na pasta de coletas, com colunas
`experimento,sistema,cenario,repeticao,valor_ms,evidencia`.
Para E5 use cenários `1_no` e `2_nos`; para E7 use `catchup`.
Cada linha deve remeter a um log com a observação do instante inicial e final.

| Experimento | Sistema | Cenário | n válidas | Medições (ms) | Média ± DP amostral (ms) |
|---|---|---|---:|---|---|
| E5 | kafka | 1_no | 0 | r1=ND, r2=ND, r3=ND, r4=ND, r5=ND | ND (exigem-se 5 medições válidas) |
| E5 | kafka | 2_nos | 0 | r1=ND, r2=ND, r3=ND, r4=ND, r5=ND | ND (exigem-se 5 medições válidas) |
| E5 | nats | 1_no | 0 | r1=ND, r2=ND, r3=ND, r4=ND, r5=ND | ND (exigem-se 5 medições válidas) |
| E5 | nats | 2_nos | 0 | r1=ND, r2=ND, r3=ND, r4=ND, r5=ND | ND (exigem-se 5 medições válidas) |
| E7 | kafka | catchup | 0 | r1=ND, r2=ND, r3=ND, r4=ND, r5=ND | ND (exigem-se 5 medições válidas) |
| E7 | nats | catchup | 0 | r1=ND, r2=ND, r3=ND, r4=ND, r5=ND | ND (exigem-se 5 medições válidas) |

**Regra:** n deve ser 5 para apresentar média e DP. Uma medição ausente ou censurada não deve ser substituída por zero.

## Verificação de integridade — arquivos brutos presentes

- E2: **nenhuma saída verify disponível**
- E4: **nenhuma saída verify disponível**
- E5: **nenhuma saída verify disponível**
- E6: **nenhuma saída verify disponível**
- E7: **nenhuma saída verify disponível**
- E8: **nenhuma saída verify disponível**
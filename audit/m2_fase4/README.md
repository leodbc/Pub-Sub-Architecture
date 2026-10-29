# Fase 4 — E1/E4 complementares, sem repetir E1–E9 histórico

Fonte normativa: PDF oficial do Trabalho M2 da UNIVALI, Tema 4, p. 31–32. Branch baseada no commit `20904c871facdb818e0c483bbdd7dc2c4751aade` do PR #3.

- GitHub Actions **[run #40795085275](https://github.com/leodbc/Pub-Sub-Architecture/actions/runs/40795085275)**, status **success**, commit do instrumento `a7fa581dc0d9681ce3fc7330b6b1c1134bcb9482`.
- Artifact original [#12863137924](https://github.com/leodbc/Pub-Sub-Architecture/actions/runs/40795085275/artifacts/12863137924); SHA-256 do ZIP original: `5f4292fb945b28c4713da354d59e89a9d62f22258c1ebd197a4e1358eeac1c67` (recalculado sobre ZIP baixado).
- Cópias íntegras textuais em `evidence/run-40795085275/`; preservar o artifact/release da Fase 1 sem mudanças.

## E1 — tópico normativo `pedidos`

Tópico **3 partições/RF3**, `min.insync.replicas=2`. Em `e1_describe_pedidos.log`:

| Partição | Líder | Réplicas | ISR |
|---:|---:|---|---|
| 0 | 2 | 2,3,1 | 2,3,1 |
| 1 | 3 | 3,1,2 | 3,1,2 |
| 2 | 1 | 1,2,3 | 1,2,3 |

Metagrupo KRaft: `e1_metadata_quorum.log`, `LeaderId=3`, `LeaderEpoch=1`, votantes 1,2,3. `e1_validacao.json` documenta a classificação automatizada. Cada líder é um snapshot, não um papel fixo. O tópico histórico `probe` tinha **uma partição** e não era equivalente ao `pedidos` do roteiro.

## E4 — dois cenários normativos (sequenciais, um produtor)

O arquivo `e4_30_sem_chave_e_mesma_chave.log` contém 30 linhas `PRODUCE` + 30 linhas `CONSUME` para **cada** cenário. `e4_validacao.json` cruza número, chave, partição e offset nos dois sentidos.

| Cenário | Partição real nesta rodada | Offsets | Resultado |
|---|---:|---|---|
| 1–30 sem chave | 0 | 0–29 | 30/30 produzidas/lidas, ordem local observada |
| 1–30 mesma chave | 2 | 0–29 | 30/30 produzidas/lidas, chave estável e ordem local observada |

A primeira amostra não espalhou mensagens por partições: não demonstrou reordenação global, nem a garante. Para o contraste entre partições, manter **separadamente** o histórico de 90 mensagens com diferentes chaves (71 inversões na concatenação global, nenhuma inversão local).

## Escopo, limitações e reprodutibilidade

`run.sh` opera apenas E1/E4 em Kafka 4.4.0 em três contêineres separados, compila programa Go em `go/bin` (diretório ignorado), usa `kafka-topics.sh` e `kafka-metadata-quorum.sh`, faz 60 produções e 60 leituras e arquiva as saídas e SHA-256. A saída é de um runner Ubuntu; não representa requisitos de produção, carga concorrente nem consenso interno. Não executa E3, E5, E6, E7, E8 ou E9.

Não houve novas medições de 50.000 mensagens de 200 bytes nem de propagação individual por follower: **E3 permanece pendente de interpretação normativa humana**. `sha256_arquivos.txt` registra os hashes produzidos pelo runner antes do empacotamento; use o ZIP original como fonte da integridade e os JSONs validados para consultas.

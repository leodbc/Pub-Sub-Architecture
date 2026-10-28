#!/usr/bin/env python3
"""E9 Tema 4 Ênfase D: compare only CI-produced E3-E8 logs, never substitute guesses."""
import csv
import re
import statistics
from pathlib import Path

root=Path("artifacts/collected")
target=Path("artifacts/e9")
target.mkdir(parents=True,exist_ok=True)

def require_one(pattern):
    matches=list(root.rglob(pattern))
    if len(matches)!=1:raise RuntimeError(f"expected one {pattern}, found {len(matches)}")
    return matches[0]

bench={
    "Kafka acks=all":"e3_kafka_acks_all.log",
    "Kafka acks=1":"e3_kafka_acks_1.log",
    "NATS JetStream RF3":"e3_nats_rf3.log"
}
rows=[]
for name,file in bench.items():
    p=require_one(file)
    raw=p.read_text()
    matches=re.findall(r"run\s+(\d+):\s+latência média=([\d.]+)\s+ms\s+vazão=([\d.]+)\s+msg/s\s+erros=(\d+)",raw)
    if [int(x[0]) for x in matches]!=[1,2,3,4,5] or any(int(x[3]) for x in matches):
        raise RuntimeError(f"Invalid 5-run sample for {name}")
    lats=[float(x[1]) for x in matches]
    throughput=[float(x[2]) for x in matches]
    rows.append((name,statistics.mean(lats),statistics.stdev(lats),
                 statistics.mean(throughput),statistics.stdev(throughput),str(p)))

grouped={}
for expfile in ("medicoes_falhas.csv","medicoes_e6_e8.csv"):
    p=require_one(expfile)
    with p.open(newline="",encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key=(row["experimento"],row["sistema"])
            grouped.setdefault(key,[]).append(float(row["valor_ms"]))
for exp in ("E5","E6","E7","E8"):
    for sys in ("kafka","nats"):
        vals=grouped.get((exp,sys),[])
        if len(vals)!=5:
            raise RuntimeError(f"{exp}/{sys}: expected five observed measurements, got {len(vals)}")
for sys in ("kafka","nats"):
    for exp,expected in (("e2",10),("e4",100)):
        data=require_one(f"{exp}_{sys}_verify.log").read_text()
        if f"lidas={expected}  únicas={expected}  confirmadas ao produtor={expected}" not in data:
            raise RuntimeError(f"{exp}/{sys}: evidence mismatch")
    for rep in range(1,6):
        outcome=require_one(f"e5_{sys}_r{rep}_quorum.txt").read_text()
        if "one_node_acks_all_confirmed=False" not in outcome:
            raise RuntimeError(f"E5/{sys} repetition {rep}: quorum loss not demonstrated")

report=[
"# E9 — Comparação experimental Kafka KRaft × NATS JetStream",
"",
"Proveniência: artefatos da mesma execução GitHub Actions do Pull Request.",
"Todos os tempos são observações feitas em runners GitHub Ubuntu com Docker.",
"Desvio padrão: amostral (n−1), n=5 para cada caso; não são garantias de desempenho.",
"",
"## E3 — latência de publicação e vazão",
"",
"| Configuração | Latência média ± DP (ms) | Vazão média ± DP (msg/s) | Arquivo bruto |",
"|---|---:|---:|---|"
]
for name,lat,ls,tps,ts,proof in rows:
    report.append(f"| {name} | {lat:.3f} ± {ls:.3f} | {tps:.1f} ± {ts:.1f} | {proof} |")
report += ["","## E5–E8 — falhas, partição e coordenação",
"",
"| Experimento | Sistema | n | Média ± DP amostral (ms) | Definição operacional |",
"|---|---|---:|---:|---|"]
scenarios={
 "E5":"Parada do líder até a primeira publicação confirmada após eleição com 2 nós",
 "E6":"Desconexão interna do líder até reentrada e saúde das 3 réplicas",
 "E7":"Reinício dos nós até ISR/replicação integralmente saudável",
 "E8":"Parada do coordenador ativo até eleição comprovada de outro"
}
for exp in ("E5","E6","E7","E8"):
    for sys in ("kafka","nats"):
        vals=grouped[(exp,sys)]
        report.append(f"| {exp} | {sys} | 5 | {statistics.mean(vals):.1f} ± {statistics.stdev(vals):.1f} | {scenarios[exp]} |")
report += ["",
"## Modelo de replicação e semântica comparada",
"",
"| Propriedade | Kafka KRaft | NATS JetStream |",
"|---|---|---|",
"| Unidade de replicação | Partição (ISR e high watermark) | Stream replicado (grupo Raft do stream) |",
"| Coordenação de metadados | Quórum KRaft | Meta-grupo Raft |",
"| Ordenação observada por E4 | Uma única partição com chave fixa | Sequência no stream PROBE |",
"| Quórum de escrita | acks=all e min.insync.replicas=2 | Commit majoritário com 3 réplicas |",
"| Perda de maioria em E5 | Arquivos e5_kafka_r*_quorum.txt | Arquivos e5_nats_r*_quorum.txt |",
"",
"## Limitações e ameaças à validade",
"",
"- E2: trace lógico da operação ainda depende de diagrama/explicação; sdtool não observa cada salto interno.",
"- E4: prova de ordem local apenas na configuração testada (um produtor e uma partição). Não demonstra múltiplas partições, concorrência ou exatamente-uma-vez.",
"- E6: um cliente que recebe apenas o bootstrap da minoria ainda pode ser redirecionado ao líder da maioria. Isso NÃO substitui isolamento completo do cliente; o script registra esse limite.",
"- E5/E7/E8: a métrica observada inclui intervalos de polling e sobrecarga do Docker/CLI; não equivale à latência interna do consenso.",
"- E3: o benchmark é sequencial, 200 mensagens por rodada, sujeito a efeito de aquecimento e a diferenças entre APIs; não compara workloads idênticos de produção.",
"- A saída de sdtool verify mede IDs no laboratório, não prova consistência linearizável nem exactly-once ponta a ponta.",
"",
"## Rastreabilidade",
"",
"- Logs, CSVs, saídas verify e resumos originais permanecem nos artifacts m2-e1e4, m2-e5e7 e m2-e6e8.",
"- Os dados aqui apresentados são derivados exclusivamente desses arquivos; se faltarem cinco repetições, a execução E9 falha.",
"- O relatório acadêmico de 15–25 páginas, desenhos próprios, Q1–Q14, S1–S4 e apresentação não são gerados ou aprovados automaticamente aqui.",
]
(target/"comparacao_e9.md").write_text("\n".join(report)+"\n",encoding="utf-8")
print("E9 evidence-based comparison complete (E3–E8, 5 measurements per system)")

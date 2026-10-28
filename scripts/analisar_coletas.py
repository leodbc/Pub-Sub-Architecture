#!/usr/bin/env python3
"""Análise sem inventar medidas: extrai E3 do sdtool e E5/E7 do CSV manual.

Uso: python3 analisar_coletas.py PASTA_DOS_LOGS
Gera PASTA_DOS_LOGS/resumo_estatistico.md.
"""
from __future__ import annotations
import csv
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

if len(sys.argv) != 2:
    raise SystemExit('Uso: python3 analisar_coletas.py PASTA_DOS_LOGS')
root = Path(sys.argv[1]).resolve()
if not root.is_dir():
    raise SystemExit(f'Pasta inexistente: {root}')

PATTERN = re.compile(r'run\s+(\d+):\s+latência média=([0-9.]+)\s+ms\s+vazão=([0-9.]+)\s+msg/s\s+erros=(\d+)')
BENCH = {
    'Kafka acks=all': 'e3_kafka_acks_all.log',
    'Kafka acks=1': 'e3_kafka_acks_1.log',
    'NATS RF3': 'e3_nats_rf3.log',
}

def stats(nums: list[float]) -> str:
    if len(nums) != 5:
        return 'ND (exigem-se 5 medições válidas)'
    return f'{statistics.mean(nums):.3f} ± {statistics.stdev(nums):.3f}'

lines = [
    '# Resumo estatístico das coletas',
    '',
    'Desvio padrão **amostral** (n-1). Não imputa valores ausentes.',
    '',
    '## E3 — extração dos logs reais do `sdtool bench`',
    '',
    '| Sistema/modo | Repetição | Latência média (ms) | Vazão (msg/s) | Erros | Evidência |',
    '|---|---:|---:|---:|---:|---|',
]

for label, filename in BENCH.items():
    p = root / filename
    found = PATTERN.findall(p.read_text(encoding='utf-8', errors='replace')) if p.is_file() else []
    rows: dict[int, tuple[float,float,int]] = {}
    for run, lat, tps, errs in found:
        rows[int(run)] = (float(lat),float(tps),int(errs))
    lats = []
    tpses = []
    for i in range(1,6):
        if i in rows:
            lat,tps,errs=rows[i]
            lines.append(f'| {label} | {i} | {lat:.2f} | {tps:.2f} | {errs} | `{filename}` |')
            if errs == 0:
                lats.append(lat)
                tpses.append(tps)
        else:
            lines.append(f'| {label} | {i} | ND | ND | ND | `{filename}` ausente/incompleto |')
    lines.append(f'| **{label}: média ± DP** | **5** | **{stats(lats)}** | **{stats(tpses)}** | — | — |')
    if rows and any(errs for _,_,errs in rows.values()):
        lines.append(f'\n> **Atenção {label}:** há rodadas com erros; a estatística acima só é considerada completa com cinco rodadas válidas sem erros. Preservar os logs de falha.\n')

lines += [
    '',
    '## E5/E7 — medições manuais com evidência de timestamp',
    '',
    'Opcional: criar `medicoes_falhas.csv` na pasta de coletas, com colunas',
    '`experimento,sistema,cenario,repeticao,valor_ms,evidencia`.',
    'Para E5 use cenários `1_no` e `2_nos`; para E7 use `catchup`.',
    'Cada linha deve remeter a um log com a observação do instante inicial e final.',
    '',
    '| Experimento | Sistema | Cenário | n válidas | Medições (ms) | Média ± DP amostral (ms) |',
    '|---|---|---|---:|---|---|',
]

expected = [('E5','kafka','1_no'),('E5','kafka','2_nos'),('E5','nats','1_no'),('E5','nats','2_nos'),('E7','kafka','catchup'),('E7','nats','catchup')]
values = defaultdict(dict)
p = root/'medicoes_falhas.csv'
if p.is_file():
    with p.open(newline='',encoding='utf-8-sig') as f:
        for index,row in enumerate(csv.DictReader(f),start=2):
            try:
                key=(row['experimento'].upper().strip(),row['sistema'].lower().strip(),row['cenario'].lower().strip())
                rep=int(row['repeticao'])
                val=float(row['valor_ms'])
                proof=row['evidencia'].strip()
                if key not in expected or not 1<=rep<=5 or val<0 or not proof:
                    raise ValueError('cenário, repetição, tempo ou evidência inválidos')
                if rep in values[key]:
                    raise ValueError('repetição duplicada')
                values[key][rep]=(val,proof)
            except (ValueError,KeyError) as e:
                raise SystemExit(f'CSV inválido na linha {index}: {e}') from e

for key in expected:
    d=values[key]
    arr=[d[i][0] for i in range(1,6) if i in d]
    vals=', '.join(f'r{i}={d[i][0]:.3f} [{d[i][1]}]' if i in d else f'r{i}=ND' for i in range(1,6))
    lines.append(f'| {key[0]} | {key[1]} | {key[2]} | {len(arr)} | {vals} | {stats(arr)} |')
lines.extend(['', '**Regra:** n deve ser 5 para apresentar média e DP. Uma medição ausente ou censurada não deve ser substituída por zero.', '', '## Verificação de integridade — arquivos brutos presentes', ''])
for exp in ('e2','e4','e5','e6','e7','e8'):
    matches=sorted(x.name for x in root.glob(f'{exp}*verify.log'))
    lines.append(f'- {exp.upper()}: ' + (', '.join(f'`{x}`' for x in matches) if matches else '**nenhuma saída verify disponível**'))
output = root/'resumo_estatistico.md'
output.write_text('\n'.join(lines)+'\n',encoding='utf-8')
print(f'Gerado: {output}')
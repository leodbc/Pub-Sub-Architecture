#!/usr/bin/env python3
"""Re-evaluate run 40732373570 without changing its raw evidence."""
import csv,re,collections
from pathlib import Path
root=Path("artifacts/historical")
out=Path("artifacts/methodology-audit");out.mkdir(parents=True,exist_ok=True)
original="https://github.com/leodbc/Pub-Sub-Architecture/actions/runs/40732373570"
e5=root/"e5";e8=root/"e6"
def get(directory,filename):
 f=directory/filename
 if not f.is_file():raise RuntimeError("Historical evidence missing: "+str(f))
 return f.read_text(encoding="utf-8",errors="replace")
def rc(text):
 m=re.search(r"\bcode=(\d+)",text)
 return int(m.group(1)) if m else None
def write_status(text):
 m=re.search(r"confirmadas=(\d+)\s+falhas=(\d+)",text)
 if m and int(m.group(1))==1 and int(m.group(2))==0:return "confirmed"
 if rc(text)==124 or "deadline exceeded" in text or "timeout" in text.lower():return "timeout_unconfirmed"
 if rc(text) not in (0,None):return "api_error"
 return "unconfirmed"
def read_status(text):
 if rc(text)==124:return "timeout"
 if rc(text)!=0:return "api_error"
 m=re.search(r"lidas=(\d+)\s+únicas=(\d+)\s+confirmadas ao produtor=(\d+)",text)
 if not m:return "invalid_summary"
 return "api_ok_data" if int(m.group(1)) else "api_ok_empty"
rows=[]
for sys in ("kafka","nats"):
 for rep in range(1,6):
  name=f"e5_{sys}_r{rep}"
  def append(active,operation,mode,status,filename):
   rows.append(dict(system=sys,repeat=rep,nodes_active=active,
       operation=operation,mode=mode,status=status,source=filename,
       artifact="m2-e5e7-40732373570-1"))
  baseline=name+"_baseline.log"
  assert write_status(get(e5,baseline))=="confirmed",baseline
  append(3,"write","kafka_acks_all" if sys=="kafka" else "nats_puback","confirmed",baseline)
  attempts=sorted(e5.glob(name+"_failover_attempt*.log"))
  if not attempts:raise RuntimeError("No 2-node writes for "+name)
  successful=[p for p in attempts if write_status(p.read_text())=="confirmed"]
  append(2,"write","kafka_acks_all" if sys=="kafka" else "nats_puback",
         "confirmed" if successful else "unconfirmed",str(successful[-1].name if successful else attempts[-1].name))
  for active,stage in ((2,"read_2_nodes"),(1,"read_1_node")):
   filename=name+"_"+stage+".log"
   append(active,"read","historical_verify",read_status(get(e5,filename)),filename)
  filename=name+"_one_node_acks_all.log"
  append(1,"write","kafka_acks_all" if sys=="kafka" else "nats_puback",
         write_status(get(e5,filename)),filename)
  if sys=="kafka":
   filename=name+"_one_node_acks1.log"
   append(1,"write","kafka_acks_1",write_status(get(e5,filename)),filename)
# Keep all exact outcomes and names of the original artifacts.
with (out/"e5_5x_matrix.csv").open("w",newline="") as file:
 writer=csv.DictWriter(file,fieldnames=rows[0].keys());writer.writeheader();writer.writerows(rows)
with (out/"e5_matriz.md").open("w") as file:
 file.write("# E5 — matriz auditada: 3/2/1 nós ativos\n\n")
 file.write("Proveniência integral: ["+original+"]("+original+"), artifact "+
           "m2-e5e7-40732373570-1. As 5 repetições originais NÃO foram alteradas.\n\n")
 file.write("Sucesso do comando verify é somente sucesso de API; arquivo de ACK "+
           "das leituras intermediárias pode corresponder à última tentativa "+
           "não confirmada. Portanto NÃO é prova de leitura de um registro "+
           "previamente confirmado. Timeout ≠ perda de dados.\n\n")
 file.write("| Sistema | Nós ativos | Operação | Modo | Resultado em r1–r5 | Arquivos fonte |\n")
 file.write("|---|---:|---|---|---|---|\n")
 groups=collections.defaultdict(list)
 for item in rows:groups[(item["system"],item["nodes_active"],item["operation"],item["mode"])].append(item)
 for k,rr in sorted(groups.items()):
  file.write("| "+" | ".join(map(str,k))+" | "+
      ", ".join(f"r{x['repeat']}={x['status']}" for x in rr)+" | "+
      ", ".join(x["source"] for x in rr)+" |\n")
 file.write("\n**Não demonstrado no legado:** 3 nós × leitura com arquivo de ACK verificado, "+
            "e garantia de entrega a consumidor específico ao perder maioria. "+
            "E6 isolado acrescentará leitura com referência de um ACK pré-partição.\n")
# NATS exposes meta-leader ID at /jsz. It does not expose its Raft term directly.
with (out/"e8_metagrupo.md").open("w") as file:
 file.write("# E8 — observações diretas de coordenação (run #40732373570)\n\n")
 file.write("| Sistema | Repetição | Líder anterior | Novo líder | Época observada | Fonte |\n")
 file.write("|---|---:|---|---|---|---|\n")
 for sys in ("kafka","nats"):
  for rep in range(1,6):
   name=f"e8_{sys}_r{rep}_elected.txt"
   txt=get(e8,name)
   a=re.search(r"previous=(\S+)\s+epoch=(\S+)",txt)
   b=re.search(r"new=(\S+)\s+epoch=(\S+)",txt)
   if not a or not b or a.group(1)==b.group(1):raise RuntimeError(name+": no proved new leader")
   if sys=="nats":
    assert a.group(2)=="None" and b.group(2)=="None",name
    epoch="não exposta pela API /jsz (não observada)"
   else:
    assert int(b.group(2))>int(a.group(2)),name
    epoch=a.group(2)+" → "+b.group(2)
   file.write(f"| {sys} | {rep} | {a.group(1)} | {b.group(1)} | {epoch} | {name} |\n")
 file.write("\nO meta-grupo NATS foi reeleito (mudança de identidade observada); "+
  "o termo Raft interno não foi exposto nos dados coletados. "+
  "Um campo ausente NÃO deve ser convertido para zero, estimado ou "+
  "assimilado ao LeaderEpoch do KRaft.\n")
print("PASS: E5 5-repetition source matrix and E8 NATS identity, no fabricated term")

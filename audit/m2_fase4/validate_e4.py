#!/usr/bin/env python3
"""Valida 60 produções e 60 leituras com partições/offsets rastreáveis."""
import sys,re,json,collections
s=open(sys.argv[1],encoding='utf-8').read()
patterns={'PRODUCE':{},'CONSUME':{}}
for line in s.splitlines():
 m=re.search(r'^(PRODUCE|CONSUME) scenario=(sem_chave|mesma_chave) number=(\d+) partition=(\d+) offset=(\d+) key=([^ ]+) value=(\S+)',line)
 if not m:continue
 kind,scenario,idx,part,off,key,value=m.groups()
 idx=int(idx);part=int(part);off=int(off)
 assert 1<=idx<=30 and value==f'M2F4_{scenario}_{idx:02d}',line
 assert idx not in patterns[kind].setdefault(scenario,{}),line
 patterns[kind][scenario][idx]={'partition':part,'offset':off,'key':key}
res={}
for sc in ['sem_chave','mesma_chave']:
 sent=patterns['PRODUCE'].get(sc,{});read=patterns['CONSUME'].get(sc,{})
 assert set(sent)==set(range(1,31)) and sent==read, (sc,sent,read)
 assert all(x['key']==('<nil>' if sc=='sem_chave' else 'mesma-chave-e4') for x in sent.values()),sc
 parts=collections.defaultdict(list)
 for idx,entry in sent.items():parts[entry['partition']].append((entry['offset'],idx))
 for values in parts.values():
  indices=[i for off,i in sorted(values)]
  assert indices==sorted(indices),values
 if sc=='mesma_chave':assert len(parts)==1,parts
 res[sc]={'produced':30,'consumed':30,'partitions':sorted(parts),'per_partition_order':True,'per_partition_offsets':{str(p):sorted(a) for p,a in parts.items()}}
assert 'E4_OK two_scenarios=2' in s
print(json.dumps({'status':'validated','cases':res,'source':sys.argv[1]},ensure_ascii=False,indent=2))

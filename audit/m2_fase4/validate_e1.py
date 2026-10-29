#!/usr/bin/env python3
"""Checagem estrita dos 3 líderes, 3 RF e 3 ISRs, sem fabricar observações."""
import re,sys,json
s=open(sys.argv[1],encoding='utf-8').read()
rows=[]
for line in s.splitlines():
 if not re.search(r'\bPartition:\s*[012]\b',line): continue
 get=lambda name:re.search(r'\b'+name+r':\s*([^\t ]+)',line,re.I)
 x={k:get(k).group(1) if get(k) else None for k in ['Topic','Partition','Leader','Replicas','Isr']}
 if x['Topic']!='pedidos': continue
 rows.append(x)
assert len(rows)==3,rows
assert {int(x['Partition']) for x in rows}=={0,1,2},rows
for x in rows:
 replicas=x['Replicas'].split(',');isr=x['Isr'].split(',')
 assert len(replicas)==3 and len(set(replicas))==3 and len(isr)==3, x
 assert set(isr)==set(replicas),x
 assert x['Leader'] in replicas and x['Leader']!='-1',x
assert 'PartitionCount: 3' in s and 'ReplicationFactor: 3' in s,s
print(json.dumps({'topic':'pedidos','partitions':rows,'rf':3,'all_in_sync':True,'source':sys.argv[1]},ensure_ascii=False,indent=2))

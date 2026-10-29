#!/usr/bin/env python3
"""Process original E3 logs (preserve originals); fail transparently for incomplete series."""
import glob,hashlib,json,pathlib,re,statistics,sys
F=pathlib.Path(sys.argv[1]); B=pathlib.Path(sys.argv[2])
measure=[]; warning=[]
for p in sorted(F.glob('measure_*.jsonl')):
    lines=p.read_text(errors='replace').splitlines()
    rows=[json.loads(line.split(' ',1)[1]) for line in lines if line.startswith('MEASUREMENT ')]
    if len(rows)!=1: warning.append(f'{p.name}: observed {len(rows)} measurement rows');continue
    row=rows[0]
    if int(row['producer_offset']) != int(row['offset']) or row['key'] not in row['record_id']:
        warning.append(f'{p.name}: identifier/offset mismatch');continue
    if row.get('batch_serving_broker') != row.get('selected_follower'):
        warning.append(f'{p.name}: follower not confirmed by batch hook')
    measure.append(row)
def stats(rows,field):
    vals=[float(x[field]) for x in rows]
    return {'n':len(vals),'individual_ms':vals,'mean_ms':statistics.mean(vals) if vals else None,'sample_sd_ms':statistics.stdev(vals) if len(vals)>1 else None}
results={'follower_read':{'observations':measure,'n':len(measure),'follower_confirmed':sum(1 for x in measure if x.get('follower_fetch_confirmed') is True),
    'send_to_ack':stats(measure,'ack_ms'),'send_to_callback':stats(measure,'send_to_callback_ms'),
    'ack_to_callback':stats(measure,'ack_to_callback_ms'),
    'interpretation':'End-to-end durations include client scheduling/polling; not direct measurement of follower log-apply time.'},
    'producer_benchmark':{},'warnings':warning,
    'method':'Historical 200-message runs remain independent; 50,000x200-byte runs are separate and use kafka-producer-perf-test.'}
for mode in ('1','all'):
    rows=[]
    for f in sorted(B.glob(f'perf_{mode}_*.log')):
        t=f.read_text(errors='replace')
        matches=re.findall(r'([\d,.]+) records sent,\s*([\d.]+) records/sec.*?([\d.]+) ms avg latency',t,re.S)
        if len(matches)!=1:
            warning.append(f'{f.name}: performance summary missing or ambiguous');continue
        count,throughput,latency=matches[0]
        if int(count.replace(',',''))!=50000:
            warning.append(f'{f.name}: wrong record count {count}');continue
        rows.append({'file':f.name,'records':50000,'records_per_sec':float(throughput),'producer_avg_latency_ms':float(latency)})
    results['producer_benchmark'][mode]={'individual':rows,'n':len(rows),
        'throughput_records_sec':{'individual':[r['records_per_sec'] for r in rows], 'mean':statistics.mean(r['records_per_sec'] for r in rows) if rows else None, 'sample_sd':statistics.stdev([r['records_per_sec'] for r in rows]) if len(rows)>1 else None},
        'avg_latency_ms':{'individual':[r['producer_avg_latency_ms'] for r in rows], 'mean':statistics.mean(r['producer_avg_latency_ms'] for r in rows) if rows else None, 'sample_sd':statistics.stdev([r['producer_avg_latency_ms'] for r in rows]) if len(rows)>1 else None}}
for p,d in [(F,'follower'),(B,'benchmark')]:
    for f in sorted(p.rglob('*')):
        if f.is_file() and f.name not in ('sha256_arquivos.txt','summary.json'):
            pass
    lines=[f'{hashlib.sha256(f.read_bytes()).hexdigest()}  {f.relative_to(p)}' for f in sorted(p.rglob('*')) if f.is_file() and f.name not in ('sha256_arquivos.txt','summary.json')]
    (p/'sha256_arquivos.txt').write_text('\n'.join(lines)+'\n')
(F/'summary.json').write_text(json.dumps(results,ensure_ascii=False,indent=2)+'\n')
print(json.dumps({'follower_n':results['follower_read']['n'],'follower_confirmed':results['follower_read']['follower_confirmed'],'benchmark_n':{m:results['producer_benchmark'][m]['n'] for m in ('1','all')},'warnings':warning},ensure_ascii=False))
if len(measure)!=5 or any(results['producer_benchmark'][m]['n']!=5 for m in ('1','all')):
    sys.exit('Incomplete series: inspect raw logs; do not publish invented results')

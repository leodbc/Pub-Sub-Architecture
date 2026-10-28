#!/usr/bin/env python3
"""E6 partitions and E8 coordinator elections, repeated five times in disposable Docker."""
import csv
import json
import re
import statistics
import subprocess
import time
import urllib.request
from pathlib import Path

ROOT=Path(__file__).resolve().parent.parent
OUT=ROOT/"artifacts"/"e6-e8"
OUT.mkdir(parents=True,exist_ok=True)
nodes={"kafka":["kafka1","kafka2","kafka3"],"nats":["nats1","nats2","nats3"]}
clients={"kafka":"cliente","nats":"cliente-nats"}
records=[]
seq=15000

def execute(label,args,timeout=25,optional=False):
    st=time.monotonic()
    try:
        p=subprocess.run(args,cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=timeout)
        code,out=p.returncode,p.stdout
    except subprocess.TimeoutExpired as exc:code,out=124,str(exc)
    rec=f"utc={time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())} cmd={args!r} code={code} elapsed_ms={(time.monotonic()-st)*1000:.2f}\n{out}\n"
    (OUT/(label+".log")).write_text(rec,encoding="utf-8")
    print(label,code,flush=True)
    if code and not optional:raise RuntimeError(rec[-1600:])
    return code,out

def sd(sys,label,*args,timeout=30,optional=False):
    return execute(label,["docker","exec",clients[sys],"/app/sdtool",*args,"--sys",sys],
                   timeout=timeout,optional=optional)

def stream_info(label):
    for ip in (51,52,53):
        code,out=execute(label,["docker","exec","cliente-nats","nats","--server",
                      f"nats://172.33.0.{ip}:4222","stream","info","PROBE"],timeout=8,optional=True)
        if not code:return out
    raise RuntimeError("NATS stream info unreachable "+label)

def kafka_topic(label):
    return execute(label,["docker","exec","cliente","sh","-c",
           '/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --describe --topic probe'],timeout=12)[1]

def kafka_quorum(label):
    return execute(label,["docker","exec","cliente","sh","-c",
           '/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server "$BS" describe --status'],timeout=12)[1]

def meta_json(label):
    for ip in (51,52,53):
        try:
            with urllib.request.urlopen(f"http://172.33.0.{ip}:8222/jsz",timeout=3) as res:
                data=json.loads(res.read())
            (OUT/(label+f"_node_{ip}.json")).write_text(json.dumps(data,indent=2),encoding="utf-8")
            if data.get("meta_cluster",{}).get("leader"):return data
        except Exception as exc:
            (OUT/(label+"_error.log")).write_text(str(exc),encoding="utf-8")
    raise RuntimeError("NATS /jsz: all sampled nodes lack a confirmed meta_cluster.leader")

def leader(sys,label):
    if sys=="kafka":
        m=re.search(r"Partition:\s*0\b.*?Leader:\s*(\d+)",kafka_topic(label))
        if not m:raise RuntimeError("Kafka leader parse failed")
        return "kafka"+m.group(1)
    m=re.search(r"^\s*Leader:\s*(nats\d+)\s*$",stream_info(label),re.M)
    if not m:raise RuntimeError("NATS stream leader parse failed")
    return m.group(1)

def healthy(sys,label):
    if sys=="kafka":
        m=re.search(r"Partition:\s*0\b.*?Isr:\s*([\d,]+)",kafka_topic(label))
        return bool(m and set(m.group(1).split(","))=={"1","2","3"})
    out=stream_info(label)
    return bool(re.search(r"^\s*Leader:\s*nats\d+\s*$",out,re.M) and
                len(re.findall(r"^\s*Replica:\s+nats\d+,\s*current",out,re.M))==2)

def wait(sys,label,limit=125):
    start=time.monotonic()
    path=OUT/(label+"_poll.log")
    while time.monotonic()-start<limit:
        try:
            if healthy(sys,label+"_last"):
                with path.open("a") as f:f.write(f"{time.time()} HEALTHY\n")
                return (time.monotonic()-start)*1000
        except Exception as exc:
            with path.open("a") as f:f.write(f"{time.time()} {exc}\n")
        time.sleep(1)
    for node in nodes[sys]:
        execute(label+"_docker_state_"+node,["docker","inspect","--format",
            "{{json .State}}",node],timeout=10,optional=True)
    raise RuntimeError(label+" failed to recover replication health; inspect Docker state and native logs")

def probe(sys,label,addrs=None):
    global seq
    seq+=1
    command=["docker","exec",clients[sys],"/app/sdtool","probe","--sys",sys,
             "--count","1","--start-id",str(seq),"--interval","1ms",
             "--timeout","2s","--file","/tmp/partition.csv"]
    if addrs:command.extend(["--addrs",addrs])
    code,out=execute(label,command,timeout=13,optional=True)
    execute(label+"_acked",["docker","cp",clients[sys]+":/tmp/partition.csv",
             str(OUT/(label+".csv"))],timeout=7,optional=True)
    return code==0 and bool(re.search(r"confirmadas=1\s+falhas=0",out))

def record(exp,sys,rep,metric,val,evidence):
    records.append(dict(experimento=exp,sistema=sys,repeticao=rep,
                        cenario=metric,valor_ms=round(val,3),evidencia=evidence))
    export()

def export():
    with (OUT/"medicoes_e6_e8.csv").open("w",newline="") as f:
        w=csv.DictWriter(f,fieldnames=("experimento","sistema","repeticao","cenario","valor_ms","evidencia"))
        w.writeheader();w.writerows(records)
    with (OUT/"resumo_e6_e8.md").open("w") as f:
        f.write("# E6/E8 medições reais\n\n")
        f.write("E6: duração da desconexão interna até saúde total após reconexão; "
                "não equivale a janela exata de indisponibilidade do cliente. "
                "E8: tempo de eleição após parada do coordenador. "
                "DP amostral, n=5.\n\n")
        f.write("| Exp | Sistema | n | Média ms | DP ms | Valores ms |\n|---|---|---:|---:|---:|---|\n")
        for exp in ("E6","E8"):
            for sys in nodes:
                vals=[r["valor_ms"] for r in records if r["experimento"]==exp and r["sistema"]==sys]
                if len(vals)==5:f.write(f"| {exp} | {sys} | 5 | {statistics.mean(vals):.3f} | {statistics.stdev(vals):.3f} | {vals} |\n")
                else:f.write(f"| {exp} | {sys} | {len(vals)} | ND | ND | {vals} |\n")

def setup(sys):
    execute(sys+"_up",["docker","compose","-f",sys+"/docker-compose.yml","up","-d"],timeout=130)
    for i in range(35):
        code,out=execute(sys+f"_setup_{i}",["docker","exec",clients[sys],"/app/sdtool",
                             "setup","--sys",sys],timeout=15,optional=True)
        if code==0 and "criado" in out:break
        time.sleep(3)
    else:raise RuntimeError(sys+": failed to setup")
    wait(sys,sys+"_ready")

def network_address(sys,node):
    if sys=="kafka":
        return "kafka_interna","172.30.0."+str(40+int(node[-1])),"172.31.0."+str(40+int(node[-1]))+":19092"
    return "nats_interna","172.32.0."+str(50+int(node[-1])),"nats://172.33.0."+str(50+int(node[-1]))+":4222"

def experiment_e6(sys,rep):
    name=f"e6_{sys}_r{rep}"
    old=leader(sys,name+"_before")
    network,internal,client=network_address(sys,old)
    disconnected=False
    t0=time.monotonic()
    try:
        execute(name+"_disconnect",["docker","network","disconnect",network,old],timeout=20)
        disconnected=True
        # Majority may elect a new leader. Single seed on minority DOES NOT guarantee
        # traffic confinement in Kafka: broker metadata can redirect to majority.
        majority=probe(sys,name+"_majority_client")
        minority=probe(sys,name+"_minority_seed",addrs=client)
        (OUT/(name+"_outcomes.txt")).write_text(
            f"old_leader={old}\nmajority_write_ack={majority}\nminority_seed_ack={minority}\n"
            "CAVEAT: a single bootstrap seed is not a true client-side network partition.\n")
        try:leader(sys,name+"_during")
        except Exception as exc:(OUT/(name+"_leader_error.log")).write_text(str(exc))
    finally:
        if disconnected:
            execute(name+"_reconnect",["docker","network","connect","--ip",internal,network,old],timeout=25)
    time_to_health=(time.monotonic()-t0)*1000+wait(sys,name+"_restored")
    record("E6",sys,rep,"partition_to_healthy",time_to_health,name+"_outcomes.txt")

def coordinator(sys,label):
    if sys=="kafka":
        out=kafka_quorum(label)
        a=re.search(r"LeaderId:\s*(\d+)",out)
        b=re.search(r"LeaderEpoch:\s*(\d+)",out)
        if not a or not b:raise RuntimeError("KRaft quorum output lacks leader/epoch")
        return "kafka"+a.group(1),int(b.group(1))
    for retry in range(15):
        try:
            data=meta_json(label+f"_attempt_{retry}")
            leader_name=data.get("meta_cluster",{}).get("leader")
            if leader_name:
                return leader_name,None
        except RuntimeError:
            pass
        time.sleep(.8)
    raise RuntimeError("NATS meta leader unavailable on all 3 nodes after 15 observations")

def experiment_e8(sys,rep):
    name=f"e8_{sys}_r{rep}"
    old,epoch=coordinator(sys,name+"_before")
    stopped=False
    t0=time.monotonic()
    try:
        execute(name+"_stop",["docker","stop","-t","10",old],timeout=22)
        stopped=True
        seen=None
        for i in range(70):
            try:
                new,ep=coordinator(sys,name+f"_observe_{i}")
                if new!=old and (epoch is None or (ep is not None and ep>epoch)):
                    seen=(new,ep)
                    break
            except Exception as exc:
                (OUT/(name+f"_error_{i}.log")).write_text(str(exc))
            time.sleep(.8)
        if not seen:raise RuntimeError(name+": no verified coordinator change")
        duration=(time.monotonic()-t0)*1000
        (OUT/(name+"_elected.txt")).write_text(f"previous={old} epoch={epoch}\nnew={seen[0]} epoch={seen[1]}\n")
        record("E8",sys,rep,"election",duration,name+"_elected.txt")
    finally:
        if stopped:execute(name+"_restart",["docker","start",old],timeout=30)
    wait(sys,name+"_full_recovery")

try:
    for sys in nodes:
        setup(sys)
        for rep in range(1,6):experiment_e6(sys,rep)
        for rep in range(1,6):experiment_e8(sys,rep)
    export()
    if len(records)!=20:raise RuntimeError(f"R7 incomplete: {len(records)}/20")
    print("E6/E8 complete, 20 real timed cycles",flush=True)
finally:
    for sys in nodes:
        execute(sys+"_docker_logs",["docker","compose","-f",sys+"/docker-compose.yml",
                            "logs","--no-color","--timestamps"],timeout=35,optional=True)
        execute(sys+"_down",["docker","compose","-f",sys+"/docker-compose.yml",
                            "down","--remove-orphans"],timeout=40,optional=True)

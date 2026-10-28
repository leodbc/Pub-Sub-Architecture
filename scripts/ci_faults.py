#!/usr/bin/env python3
"""Five real failover and catch-up cycles per system, with raw timestamps."""
import csv
import re
import statistics
import subprocess
import time
from pathlib import Path

ROOT=Path(__file__).resolve().parent.parent
OUT=ROOT/"artifacts"/"e5-e7"
OUT.mkdir(parents=True,exist_ok=True)
nodes={"kafka":["kafka1","kafka2","kafka3"],"nats":["nats1","nats2","nats3"]}
clients={"kafka":"cliente","nats":"cliente-nats"}
results=[]
next_id=100

def run(label,args,timeout=35,allow_failure=False):
    stamp=time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())
    t=time.monotonic()
    try:
        proc=subprocess.run(args,cwd=ROOT,text=True,stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,timeout=timeout)
        code,out=proc.returncode,proc.stdout
    except subprocess.TimeoutExpired as exc:
        code,out=124,str(exc)
    record=f"{stamp} CMD={args!r}\ncode={code} elapsed_ms={(time.monotonic()-t)*1000:.1f}\n{out}\n"
    (OUT/(label+".log")).write_text(record,encoding="utf-8")
    print(label,code,flush=True)
    if code and not allow_failure:
        raise RuntimeError(label+": "+record[-1800:])
    return code,out

def sd(sys,label,*args,timeout=25):
    return run(label,["docker","exec",clients[sys],"/app/sdtool",*args,"--sys",sys],timeout=timeout)

def info(sys,label):
    if sys=="kafka":
        return run(label,["docker","exec","cliente","sh","-c",
                          '/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --describe --topic probe'],timeout=12)[1]
    for ip in (51,52,53):
        code,out=run(label,["docker","exec","cliente-nats","nats","--server",
                     f"nats://172.33.0.{ip}:4222","stream","info","PROBE"],
                     timeout=9,allow_failure=True)
        if not code:return out
    raise RuntimeError(sys+" NATS info unavailable")

def leader(sys,label):
    out=info(sys,label)
    pattern=(r"\bPartition:\s*0\b.*?\bLeader:\s*(\d+)" if sys=="kafka"
             else r"^\s*Leader:\s*(nats\d+)\s*$")
    mat=re.search(pattern,out,re.M)
    if not mat:raise RuntimeError("Cannot parse leader: "+out)
    return ("kafka"+mat.group(1)) if sys=="kafka" else mat.group(1)

def healthy(sys,out):
    if sys=="kafka":
        m=re.search(r"\bPartition:\s*0\b.*?\bIsr:\s*([\d,]+)",out)
        return bool(m and set(m.group(1).split(","))=={"1","2","3"})
    return bool(re.search(r"^\s*Leader:\s*nats\d+\s*$",out,re.M)
        and len(re.findall(r"^\s*Replica:\s+nats\d+,\s*current",out,re.M))==2)

def await_healthy(sys,label,limit=120):
    start=time.monotonic()
    observations=[]
    while time.monotonic()-start<limit:
        try:
            value=info(sys,label+"_poll")
            observations.append(f"{time.time():.3f} {value[-1000:]}")
            if healthy(sys,value):
                (OUT/(label+"_history.log")).write_text("\n".join(observations),encoding="utf-8")
                return (time.monotonic()-start)*1000
        except Exception as exc:
            observations.append(f"{time.time():.3f} {exc}")
        time.sleep(.8)
    (OUT/(label+"_history.log")).write_text("\n".join(observations),encoding="utf-8")
    raise RuntimeError(label+": could not confirm 3 replicas healthy")

def reset_setup(sys,label):
    sd(sys,label+"_reset","reset",timeout=45)
    for attempt in range(45):
        code,out=run(label+f"_setup{attempt}",
                  ["docker","exec",clients[sys],"/app/sdtool","setup","--sys",sys],
                  timeout=20,allow_failure=True)
        if code==0 and "criado" in out:
            await_healthy(sys,label+"_ready")
            return
        time.sleep(2)
    raise RuntimeError(label+": cannot recreate topic/stream")

def probe(sys,label,acks="all",aggregate=None):
    global next_id
    next_id+=1
    args=["docker","exec",clients[sys],"/app/sdtool","probe","--sys",sys,
          "--count","1","--start-id",str(next_id),"--timeout","2s",
          "--interval","1ms","--file","/tmp/fault.csv"]
    if sys=="kafka":args+=["--acks",acks]
    code,out=run(label,args,timeout=12,allow_failure=True)
    _,_=run(label+"_copy",["docker","cp",clients[sys]+":/tmp/fault.csv",
                          str(OUT/(label+".csv"))],timeout=8,allow_failure=True)
    file=OUT/(label+".csv")
    if aggregate is not None and file.exists():
        aggregate.extend(file.read_text(encoding="utf-8").splitlines(keepends=True)[1:])
    return code==0 and bool(re.search(r"confirmadas=1\s+falhas=0",out))

def flush():
    with (OUT/"medicoes_falhas.csv").open("w",newline="",encoding="utf-8") as f:
        writer=csv.DictWriter(f,fieldnames=("experimento","sistema","cenario","repeticao","valor_ms","evidencia"))
        writer.writeheader()
        writer.writerows(results)
    with (OUT/"resumo_falhas.md").open("w",encoding="utf-8") as f:
        f.write("# E5/E7: medições reais de failover e catch-up\n\n")
        f.write("Failover é o tempo da parada do líder à primeira confirmação de escrita. "
                "Catch-up é o tempo de restart até todas as 3 réplicas saudáveis. "
                "Desvio padrão amostral, apenas quando n=5.\n\n")
        f.write("| Experimento | Sistema | n | Valores (ms) | Média (ms) | DP (ms) |\n")
        f.write("|---|---|---:|---|---:|---:|\n")
        for exp,scenario in (("E5","failover"),("E7","catchup")):
            for sys in nodes:
                xs=[r["valor_ms"] for r in results if r["experimento"]==exp and r["sistema"]==sys]
                f.write(f"| {exp} | {sys} | {len(xs)} | {xs} | ")
                f.write(f"{statistics.mean(xs):.3f} | {statistics.stdev(xs):.3f} |\n" if len(xs)==5 else "ND | ND |\n")

def measure(sys,exp,scenario,rep,value,proof):
    results.append(dict(experimento=exp,sistema=sys,cenario=scenario,
                        repeticao=rep,valor_ms=round(value,3),evidencia=proof))
    flush()

try:
    for sys in nodes:
        run(sys+"_compose_up",["docker","compose","-f",sys+"/docker-compose.yml","up","-d"],timeout=130)
        for attempt in range(35):
            code,out=run(sys+f"_initial_setup{attempt}",["docker","exec",clients[sys],"/app/sdtool",
                         "setup","--sys",sys],timeout=15,allow_failure=True)
            if code==0 and "criado" in out:break
            time.sleep(3)
        else:raise RuntimeError(sys+": never initialized")
        await_healthy(sys,sys+"_initial_ready")
        for rep in range(1,6):
            name=f"e5_{sys}_r{rep}"
            reset_setup(sys,name)
            aggregate=[]
            assert probe(sys,name+"_baseline",aggregate=aggregate),"baseline failed "+name
            first=leader(sys,name+"_leader")
            second=next(n for n in nodes[sys] if n!=first)
            stopped=[]
            try:
                t0=time.monotonic()
                run(name+"_stop_leader",["docker","stop","-t","2",first],timeout=12)
                stopped.append(first)
                confirmed=False
                for attempt in range(25):
                    if probe(sys,name+f"_failover_attempt{attempt}",aggregate=aggregate):
                        confirmed=True
                        break
                    time.sleep(.5)
                if not confirmed:raise RuntimeError(name+": no confirmed acks=all with 2 nodes")
                measure(sys,"E5","failover",rep,(time.monotonic()-t0)*1000,name+"_stop_leader.log")
                run(name+"_read_2_nodes",["docker","exec",clients[sys],"/app/sdtool","verify","--sys",sys,"--file","/tmp/fault.csv","--idle","1s"],timeout=22,allow_failure=True)
                run(name+"_stop_second",["docker","stop","-t","2",second],timeout=12)
                stopped.append(second)
                quorum_write=probe(sys,name+"_one_node_acks_all",aggregate=aggregate)
                (OUT/(name+"_quorum.txt")).write_text(f"one_node_acks_all_confirmed={quorum_write}\n",encoding="utf-8")
                run(name+"_read_1_node",["docker","exec",clients[sys],"/app/sdtool","verify","--sys",sys,"--file","/tmp/fault.csv","--idle","1s"],timeout=22,allow_failure=True)
                if quorum_write:raise RuntimeError(name+": unexpected confirmed acks=all with one live node")
                if sys=="kafka":probe(sys,name+"_one_node_acks1",acks="1",aggregate=aggregate)
                t1=time.monotonic()
                for n in reversed(stopped):
                    run(name+"_restart_"+n,["docker","start",n],timeout=25)
                stopped.clear()
                catchup=(time.monotonic()-t1)*1000+await_healthy(sys,name+"_catchup")
                measure(sys,"E7","catchup",rep,catchup,name+"_catchup_history.log")
                source=OUT/(name+"_acked.csv")
                source.write_text("id,ok,lat_ms,t_ms,pos\n"+"".join(aggregate),encoding="utf-8")
                run(name+"_copy_acked",["docker","cp",str(source),clients[sys]+":/tmp/merged.csv"])
                sd(sys,name+"_verify","verify","--file","/tmp/merged.csv","--idle","2s",timeout=55)
            finally:
                for n in stopped:
                    run(name+"_emergency_restart_"+n,["docker","start",n],timeout=30,allow_failure=True)
    flush()
    if len(results)!=20:raise RuntimeError("Incomplete R7: expected 20 measured cycles")
    print("E5/E7: 20 recorded real measurements",flush=True)
finally:
    for sys in nodes:
        run(sys+"_container_logs",["docker","compose","-f",sys+"/docker-compose.yml",
                                  "logs","--no-color","--timestamps"],timeout=30,allow_failure=True)
        run(sys+"_down",["docker","compose","-f",sys+"/docker-compose.yml",
                       "down","--remove-orphans"],timeout=30,allow_failure=True)

#!/usr/bin/env python3
"""Strictly isolated E6 clients, write/read both sides, separated time intervals."""
import csv,json,re,statistics,subprocess,time,sys,urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent; OUT=ROOT/"artifacts"/"methodology-e6";OUT.mkdir(parents=True,exist_ok=True)
systems={"kafka":("cliente",["kafka1","kafka2","kafka3"],"apache/kafka:4.4.0"),
         "nats":("cliente-nats",["nats1","nats2","nats3"],"natsio/nats-box:latest")}
metrics=[];operations=[]
SELECTED=(sys.argv[2] if len(sys.argv)==3 and sys.argv[1]=="--sys" else "both")
if SELECTED not in ("both","kafka","nats"):raise SystemExit("usage: ci_e6_isolated.py [--sys kafka|nats]")
def call(label,args,timeout=25,optional=False):
 start=time.monotonic()
 try:
  p=subprocess.run(args,cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=timeout)
  rc,out=p.returncode,p.stdout
 except subprocess.TimeoutExpired as e:rc,out=124,"TIMEOUT "+str(e)
 (OUT/(label+".log")).write_text("utc="+str(time.time())+"\nargs="+repr(args)+"\nrc="+str(rc)+"\nwall_ms="+str(round((time.monotonic()-start)*1000,3))+"\n"+out)
 print(label,rc,flush=True)
 if rc and not optional:raise RuntimeError(label+": "+out[-800:])
 return rc,out
def sd(sys,label,cmd,cli=None,**kw):
 args=["docker","exec",cli or systems[sys][0],"/app/sdtool",cmd,"--sys",sys]
 for key,value in kw.items():args+=["--"+key.replace("_","-"),str(value)]
 return call(label,args,timeout=17,optional=True)
def state(sys,label):
 if sys=="kafka":
  return call(label,["docker","exec","cliente","sh","-c",
   '/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --describe --topic probe'],timeout=12)[1]
 for host in (51,52,53):
  rc,out=call(label+f"_host{host}",["docker","exec","cliente-nats","nats","--server",
    f"nats://172.33.0.{host}:4222","stream","info","PROBE"],timeout=10,optional=True)
  if rc==0:return out
 raise RuntimeError(sys+": no NATS stream info")
def leader(sys,label):
 data=state(sys,label)
 m=re.search(r"Partition:\s*0\b.*?Leader:\s*(\d+)",data) if sys=="kafka" else re.search(r"^\s*Leader:\s*(nats\d+)",data,re.M)
 if not m:raise RuntimeError("Cannot parse leader: "+data[-600:])
 return "kafka"+m.group(1) if sys=="kafka" else m.group(1)
def healthy(sys,label):
 data=state(sys,label)
 if sys=="kafka":
  m=re.search(r"Partition:\s*0\b.*?Isr:\s*([\d,]+)",data)
  return bool(m and set(m.group(1).split(","))=={"1","2","3"})
 return bool(re.search(r"^\s*Leader:\s*nats\d+",data,re.M) and
             len(re.findall(r"^\s*Replica:\s+nats\d+,\s*current",data,re.M))==2)
def health_time(sys,label):
 start=time.monotonic();observations=[]
 while time.monotonic()-start<155:
  try:
   good=healthy(sys,label+"_sample")
   observations.append(f"{time.time():.6f} healthy={good}")
   if good:
    (OUT/(label+"_observations.log")).write_text("\n".join(observations)+"\n")
    return time.monotonic()
  except Exception as e:observations.append(f"{time.time():.6f} err={e}")
  time.sleep(.8)
 (OUT/(label+"_observations.log")).write_text("\n".join(observations)+"\n")
 raise RuntimeError(sys+" did not restore three replicas")
def nats_diagnostics(label):
 for node in ("nats1","nats2","nats3"):
  call(label+"_container_"+node,["docker","logs",node],timeout=15,optional=True)
 for i in (51,52,53):
  for endpoint in ("routez","jsz"):
   url=f"http://172.33.0.{i}:8222/{endpoint}"
   try:
    with urllib.request.urlopen(url,timeout=2) as f: body=f.read().decode("utf-8","replace")
   except Exception as e: body="HTTP monitor unavailable: "+str(e)
   (OUT/(label+f"_{i}_{endpoint}.json")).write_text(body)

def setup(sys):
 call(sys+"_start",["docker","compose","-f",sys+"/docker-compose.yml","up","-d"],timeout=120)
 for generation in range(2):
  for n in range(35):
   rc,out=sd(sys,sys+f"_setup_generation{generation}_attempt{n}","setup")
   if rc==0 and "criado" in out:
    health_time(sys,sys+"_baseline")
    return
   time.sleep(2)
  if sys!="nats":break
  nats_diagnostics(sys+"_startup_generation"+str(generation))
  if generation==0:
   # One bounded, fully logged restart BEFORE experimental measurements.
   call(sys+"_startup_recreate_down",["docker","compose","-f",
        "nats/docker-compose.yml","down","--remove-orphans"],timeout=40)
   call(sys+"_startup_recreate_up",["docker","compose","-f",
        "nats/docker-compose.yml","up","-d"],timeout=120)
 if sys=="nats":nats_diagnostics("nats_startup_final")
 raise RuntimeError(sys+" setup failed; consult routez/jsz and container logs")
def probe(sys,label,cli=None,addr=None,ident=1,filename="/tmp/e6.csv"):
 args={"count":1,"start_id":ident,"timeout":"2s","interval":"1ms","file":filename}
 if addr:args["addrs"]=addr
 if sys=="kafka":args["acks"]="all"
 rc,out=sd(sys,label,"probe",cli=cli,**args)
 if re.search(r"confirmadas=1\s+falhas=0",out):return "success",rc
 return ("timeout" if rc==124 or "deadline exceeded" in out or "timeout" in out.lower() else "api_error"),rc
def read(sys,label,filename,cli=None,addr=None):
 args={"file":filename,"idle":"1s","timeout":"2s"}
 if addr:args["addrs"]=addr
 rc,out=sd(sys,label,"verify",cli=cli,**args)
 if rc==124:return "timeout",rc
 if rc:return "api_error",rc
 m=re.search(r"lidas=(\d+)\s+únicas=(\d+)\s+confirmadas ao produtor=(\d+)",out)
 if not m:return "invalid_summary",rc
 return ("success_data" if int(m.group(1)) else "success_empty"),rc
def save():
 for filename,rows,cols in [
  ("e6_operations.csv",operations,("system","repeat","side","operation","status","exit","log")),
  ("e6_clocks.csv",metrics,("system","repeat","metric","ms","log"))]:
  with (OUT/filename).open("w",newline="") as f:
   w=csv.DictWriter(f,fieldnames=cols);w.writeheader();w.writerows(rows)
 with (OUT/"summary.md").open("w") as f:
  f.write("# E6 - partition measurements; NOT internal consensus time\n\n")
  f.write("partition_duration: disconnect completed to reconnection started; "+
   "first_majority_ack: disconnect completed to first confirmed client write; "+
   "reconnect_to_healthy: connect completed to first healthy 3-replica poll.\n\n")
  f.write("| System | Metric | n | Mean ms | Sample SD ms |\n|---|---|---:|---:|---:|\n")
  for sys in systems:
   for metric in ("partition_duration","first_majority_ack","reconnect_to_healthy"):
    xs=[x["ms"] for x in metrics if x["system"]==sys and x["metric"]==metric]
    f.write(f"| {sys} | {metric} | {len(xs)} | "+(f"{statistics.mean(xs):.3f} | {statistics.stdev(xs):.3f}" if len(xs)==5 else "ND | ND")+" |\n")
def result(sys,rep,side,operation,status,rc,label):
 operations.append(dict(system=sys,repeat=rep,side=side,operation=operation,status=status,exit=rc,log=label+".log"));save()
def metric(sys,rep,name,start,end,label):
 metrics.append(dict(system=sys,repeat=rep,metric=name,ms=round((end-start)*1000,3),log=label));save()
def ip(sys,node):
 return ("172.30.0."+str(40+int(node[-1]))) if sys=="kafka" else ("172.32.0."+str(50+int(node[-1])))
def external(sys,node):
 return (f"172.31.0.{40+int(node[-1])}:19092") if sys=="kafka" else (f"nats://172.33.0.{50+int(node[-1])}:4222")
def private(sys):
 return "172.45.100.10:19092" if sys=="kafka" else "nats://172.45.100.10:4222"
try:
 for sys,(normal,nodes,image) in systems.items():
  if SELECTED!="both" and sys!=SELECTED:continue
  setup(sys)
  try:
   for rep in range(1,6):
    name=f"e6isolated_{sys}_r{rep}";old=leader(sys,name+"_leader");other=next(n for n in nodes if n!=old)
    net=f"m2e6_{sys}_{rep}_internal";client=f"m2e6_{sys}_{rep}_client"
    net_created=False;connected=False;disconnected=False;client_created=False
    ident=8000000+(rep*1000)+(0 if sys=="kafka" else 100000)
    file=f"/tmp/e6base{rep}.csv"
    try:
     # Recovery status via StreamInfo/ISR alone is not active-write readiness.
     # Retried reference messages use unique IDs: any ambiguous ACK remains
     # ambiguous, and every attempt is preserved for the audit trail.
     baseline_ok=False
     for baseline_attempt in range(15):
      label=name+f"_baseline_attempt{baseline_attempt}"
      status,rc=probe(sys,label,ident=ident+baseline_attempt,filename=file)
      if status=="success":
       baseline_ok=True
       break
      time.sleep(1)
     if not baseline_ok:
      if sys=="nats":nats_diagnostics(name+"_baseline_unavailable")
      raise RuntimeError(name+" baseline never confirmed in 15 attempts; inspect logs")
     call(name+"_savebaseline",["docker","cp",normal+":"+file,str(OUT/(name+"_baseline.csv"))])
     call(name+"_create_private",["docker","network","create","--internal",
      "--subnet","172.45.100.0/24",net]);net_created=True
     call(name+"_connect_private",["docker","network","connect","--ip","172.45.100.10",net,old]);connected=True
     call(name+"_run_client",["docker","run","-d","--name",client,"--network",net,
      "--ip","172.45.100.20","-v",str(ROOT/"go/bin")+":/app:ro",
      "-v",str(OUT)+":/evidence:ro","--entrypoint","sleep",image,"infinity"]);client_created=True
     _,topo=call(name+"_network_proof",["docker","network","inspect",net])
     detail=json.loads(topo)[0]
     members={v["Name"] for v in detail.get("Containers",{}).values()}
     assert detail.get("Internal") is True and members=={old,client},(name,members)
     _,inspect=call(name+"_minority_interfaces",["docker","inspect",client])
     assert set(json.loads(inspect)[0]["NetworkSettings"]["Networks"])=={net}
     # Positive proof of NO alternative route: direct bootstrap to known majority
     # advertised address must not confirm a write from isolated client.
     status,rc=probe(sys,name+"_direct_majority",cli=client,addr=external(sys,other),ident=ident+30)
     result(sys,rep,"minority","direct_to_majority",status,rc,name+"_direct_majority")
     if status=="success":raise RuntimeError(name+": isolated client could reach majority")
     call(name+"_disconnect",["docker","network","disconnect",sys+"_interna",old],timeout=25)
     disconnected=True;t_disconnected=time.monotonic()
     majority_ack=None
     for attempt in range(18):
      l=name+f"_majority_write_{attempt}"
      status,rc=probe(sys,l,ident=ident+100+attempt)
      result(sys,rep,"majority","write",status,rc,l)
      if status=="success":
       majority_ack=time.monotonic();break
      time.sleep(.4)
     if majority_ack is None:raise RuntimeError(name+" majority cannot write")
     # A successful API response with zero read records is not treated as read evidence.
     for attempt in range(10):
      l=name+f"_majority_read_{attempt}"
      status,rc=read(sys,l,file)
      result(sys,rep,"majority","read",status,rc,l)
      if status=="success_data" and "PERDIDAS (confirmadas mas ausentes): 0 " in (OUT/(l+".log")).read_text():break
      time.sleep(.5)
     else:raise RuntimeError(name+" baseline missing in majority reads")
     l=name+"_minority_write"
     status,rc=probe(sys,l,cli=client,addr=private(sys),ident=ident+500)
     result(sys,rep,"minority","write_quorum",status,rc,l)
     if status=="success":raise RuntimeError(name+" minority confirmed a quorum write")
     l=name+"_minority_read"
     status,rc=read(sys,l,"/evidence/"+name+"_baseline.csv",cli=client,addr=private(sys))
     result(sys,rep,"minority","read",status,rc,l)
     t_before_reconnect=time.monotonic()
     call(name+"_reconnect",["docker","network","connect","--ip",ip(sys,old),sys+"_interna",old],timeout=30)
     disconnected=False;t_reconnected=time.monotonic()
     t_health=health_time(sys,name+"_healthy")
     metric(sys,rep,"partition_duration",t_disconnected,t_before_reconnect,name+"_disconnect.log")
     metric(sys,rep,"first_majority_ack",t_disconnected,majority_ack,name+"_majority_write_*.log")
     metric(sys,rep,"reconnect_to_healthy",t_reconnected,t_health,name+"_healthy_observations.log")
     (OUT/(name+"_timestamps.json")).write_text(json.dumps(dict(
       disconnected=t_disconnected,majority_ack=majority_ack,
       before_reconnect=t_before_reconnect,after_reconnect=t_reconnected,
       health_poll=t_health,clock="single runner monotonic; no consensus time"),indent=2))
    finally:
     if disconnected:call(name+"_emergency_reconnect",["docker","network","connect","--ip",ip(sys,old),sys+"_interna",old],timeout=30,optional=True)
     if client_created:call(name+"_remove_client",["docker","rm","-f",client],optional=True)
     if connected:call(name+"_remove_broker_private",["docker","network","disconnect","-f",net,old],optional=True)
     if net_created:call(name+"_remove_private_network",["docker","network","rm",net],optional=True)
  finally:
   call(sys+"_docker_logs",["docker","compose","-f",sys+"/docker-compose.yml","logs","--no-color","--timestamps"],timeout=35,optional=True)
   call(sys+"_down",["docker","compose","-f",sys+"/docker-compose.yml","down","--remove-orphans"],timeout=35,optional=True)
 save()
 for sys in systems:
  if SELECTED!="both" and sys!=SELECTED:continue
  for kind in ("partition_duration","first_majority_ack","reconnect_to_healthy"):
   assert sum(m["system"]==sys and m["metric"]==kind for m in metrics)==5
 print("PASS: selected systems had 5 repetitions and 3 independent E6 clock intervals")
finally:save()

#!/usr/bin/env python3
"""Only normative gaps E2/E5/E7/E8; preserves every raw command/result."""
import json,os,re,subprocess,time,csv
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
OUT=ROOT/"artifacts"/"fase1-normativa";OUT.mkdir(parents=True,exist_ok=True)
TOPIC="m2_e2_keys";G_TOPIC="m2_e7_group";GROUP="m2-e7-"+os.environ.get("GITHUB_RUN_ID","local")
def run(label,args,seconds=40,expected=True):
 now=time.time();start=time.monotonic()
 try:
  r=subprocess.run(args,cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=seconds)
  code,out=r.returncode,r.stdout
 except subprocess.TimeoutExpired as e: code,out=124,"EXECUTION TIMEOUT "+str(e)
 log=f"utc={now:.6f}\ncommand={args!r}\nexit={code}\nduration_ms={(time.monotonic()-start)*1000:.3f}\n{out}"
 (OUT/(label+".log")).write_text(log,encoding="utf-8")
 print(f"{label} exit={code}",flush=True)
 if expected and code!=0:raise RuntimeError(f"{label} exited {code}: {out[-1300:]}")
 return code,out
def cli(label,cmd,seconds=55,expected=True):
 return run(label,["docker","exec","cliente","/app/fase1_normativo",*cmd],seconds,expected)
def kafka_script(label,script,seconds=40,expected=True):
 return run(label,["docker","exec","cliente","sh","-c",script],seconds,expected)
def quorum(label):
 code,txt=kafka_script(label,'/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server "$BS" describe --status',25)
 match=re.search(r"LeaderEpoch:\s*(\d+)",txt)
 if not match:raise RuntimeError("No controller epoch in "+label)
 return int(match.group(1))
def epoch(label):
 _,txt=cli(label,["--mode","epochs","--topic",TOPIC],seconds=25)
 m=re.search(r"PARTITION_EPOCHS=(\[.*\])",txt)
 if not m:raise RuntimeError("Invalid partition metadata response")
 return json.loads(m.group(1))
def check_topic_ready():
 for n in range(45):
  code,txt=run(f"wait_probe_setup_{n}",["docker","exec","cliente","/app/sdtool","setup",
                 "--sys","kafka","--partitions","3","--replicas","3"],seconds=20,expected=False)
  if code==0 and "criado" in txt:return
  time.sleep(2)
 raise RuntimeError("Kafka cluster not ready after 45 attempts")
def group_offset(label):
 _,txt=kafka_script(label,
  f'/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$BS" --describe --group {GROUP}',seconds=32)
 lines=[l.split() for l in txt.splitlines() if G_TOPIC in l]
 vals=[]
 for fields in lines:
  if len(fields)>=4:
   try:
    at=fields.index(G_TOPIC)
    vals.append((int(fields[at+1]),int(fields[at+2])))
   except (ValueError,IndexError):pass
 if len(vals)!=1:raise RuntimeError("Expected exactly one committed partition, got "+str(vals))
 return vals[0]
def wait_isr(label):
 for attempt in range(80):
  rc,txt=kafka_script(f"{label}_{attempt}",
   '/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --describe --topic '+TOPIC,
   seconds=12,expected=False)
  if rc==0 and re.search(r"Partition:\s*0\b.*?Isr:\s*([\d,]+)",txt):
   m=re.search(r"Partition:\s*0\b.*?Isr:\s*([\d,]+)",txt)
   if set(m.group(1).split(","))=={"1","2","3"}:return
  time.sleep(.7)
 raise RuntimeError("3-node ISR not restored")
def main():
 run("host",["sh","-c","date -u; go version; docker version; git rev-parse HEAD"])
 run("compose_up",["docker","compose","-f","kafka/docker-compose.yml","up","-d"],seconds=140)
 check_topic_ready()
 # E2: per-key partition and offset, verified against consumer's actual records.
 _,e2=cli("e2_keys",["--mode","e2","--topic",TOPIC],seconds=125)
 if "E2_OK" not in e2:raise RuntimeError("E2 did not verify mapping")
 # E7: new single-partition replicated topic, real group, explicit commit, crash, resume.
 cli("e7_seed",["--mode","group-seed","--topic",G_TOPIC],seconds=100)
 cli("e7_first_interrupted",["--mode","group-first","--topic",G_TOPIC,"--group",GROUP],seconds=115)
 part,committed=group_offset("e7_offset_after_crash")
 if part!=0 or committed!=2:raise RuntimeError(f"Kafka committed offset mismatch {part} {committed}")
 _,resumed=cli("e7_resumed",["--mode","group-resume","--topic",G_TOPIC,"--group",GROUP],seconds=140)
 if "RESUME_OK" not in resumed:raise RuntimeError("Consumer failed to resume")
 (OUT/"e7_assertions.json").write_text(json.dumps({"group":GROUP,"topic":G_TOPIC,
   "partition":part,"committed_next_offset":committed,"first_resumed_offset":2,
   "last_consumed_offset":5,"first_process": "exited without Close()/LeaveGroup",
   "second_process":"new consumer with same group","verified":True},indent=2))
 # E8: the partition epoch is independent of KRaft controller quorum epoch.
 before_controller=quorum("e8_controller_before")
 before=epoch("e8_partition_before")
 row=next(p for p in before if p["partition"]==0)
 former=f'kafka{row["leader"]}'
 stopped=False
 try:
  run("e8_stop_partition_leader",["docker","stop","-t","10",former],seconds=25)
  stopped=True
  after=None;after_controller=None
  for n in range(65):
   try:
    p=epoch(f"e8_partition_after_attempt{n}")
    new=next(x for x in p if x["partition"]==0)
    if new["leader"]!=row["leader"] and new["leader_epoch"]>row["leader_epoch"]:
     after=p;after_controller=quorum("e8_controller_after");break
   except Exception as e:(OUT/(f"e8_poll_error_{n}.txt")).write_text(str(e))
   time.sleep(.8)
  if after is None:raise RuntimeError("LeaderEpoch of partition 0 did not increment")
  (OUT/"e8_epoch_comparison.json").write_text(json.dumps({
    "partition_0_before":row,"partition_0_after":next(x for x in after if x["partition"]==0),
    "controller_quorum_epoch_before":before_controller,
    "controller_quorum_epoch_after":after_controller,
    "interpretation":"independent domains; do not equate numeric values",
    "nats_meta_term":"not observed by historical /jsz"},indent=2))
 finally:
  if stopped:run("e8_restore",["docker","start",former],seconds=35,expected=False)
 wait_isr("e8_isr_recovered")
 # E5: metadata quorum unavailable with ONE surviving combined broker/controller.
 # Data may still exist, while cluster-level metadata CREATE is unavailable.
 to_stop=["kafka1","kafka2"];stopped=[]
 try:
  for node in to_stop:
   run("e5_stop_"+node,["docker","stop","-t","10",node],seconds=25)
   stopped.append(node)
  run("e5_docker_state",["docker","ps","--format","{{.Names}} {{.Status}}"])
  cmd='/opt/kafka/bin/kafka-topics.sh --bootstrap-server 172.31.0.43:19092 --create --if-not-exists --topic m2_e5_noquorum_'
  cmd+=os.environ.get("GITHUB_RUN_ID","local")
  cmd+=' --partitions 1 --replication-factor 3'
  rc,output=kafka_script("e5_create_without_quorum","timeout 30s "+cmd,seconds=38,expected=False)
  if rc==0 or re.search(r"created topic|topic .* created",output,re.I):
   raise RuntimeError("Unexpected Kafka metadata CREATE success without controller quorum")
  classification="timeout_no_confirmation" if rc==124 else "api_error_no_confirmation"
  (OUT/"e5_create_classification.json").write_text(json.dumps({
   "active_controllers":[3],"stopped_controllers":[1,2],
   "operation":"CreateTopics new topic", "client_bootstrap":"kafka3",
   "exit":rc,"classification":classification,
   "claim":"no successful acknowledgement during loss of KRaft majority; not evidence of data loss",
   "evidence":"e5_create_without_quorum.log"},indent=2))
 finally:
  for node in reversed(stopped):run("e5_restore_"+node,["docker","start",node],seconds=35,expected=False)
 wait_isr("e5_restored_isr")
 _,txt=kafka_script("e5_topic_postrestore",
 '/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --list',seconds=25)
 topic="m2_e5_noquorum_"+os.environ.get("GITHUB_RUN_ID","local")
 if topic in txt:
  raise RuntimeError("Topic unexpectedly materialized after restoring majority: classify as ambiguous")
 (OUT/"normative_summary.md").write_text(
 "# M2 normative targeted tests E2/E5/E7/E8\n\n"
 "E2: 36 actual published and consumed keyed records; offsets and partition asserted by client.\n"
 "E5: attempt to CREATE a NEW topic with only kafka3 alive (1 of 3 controllers), no confirmation. "
 "No conclusion about data loss; metadata operation separated from read/write of existing partitions.\n"
 "E7: manual commit of next offset 2 in a consumer group, process stopped without LeaveGroup, "
 "second process reads offsets 2,3,4,5. Topic has one partition, replication factor 3.\n"
 "E8: partition leader epoch recorded via Kafka Metadata API separately from KRaft controller "
 "LeaderEpoch; forced election of partition 0 increases epoch. NATS meta term remains unobserved.\n"
 "Read corresponding raw logs and JSON files; this Markdown contains no invented times.\n")
 print("NORMATIVE_E2_E5_E7_E8_PASSED",flush=True)
try:
 main()
finally:
 run("docker_logs",["docker","compose","-f","kafka/docker-compose.yml","logs","--no-color","--timestamps"],seconds=45,expected=False)
 run("compose_down",["docker","compose","-f","kafka/docker-compose.yml","down","--remove-orphans"],seconds=50,expected=False)

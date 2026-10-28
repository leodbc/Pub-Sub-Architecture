#!/usr/bin/env python3
"""Assemble lossless, SHA256-verified essential Actions artifacts for a permanent release.
Only laboratory artifacts are downloaded, NEVER job logs, environment or Git secrets.
No overwrites of an existing release.
"""
from pathlib import Path
import hashlib, json, os, re, subprocess, zipfile, io, datetime
ROOT=Path(__file__).resolve().parent.parent
OUT=ROOT/"artifacts"/"permanent";OUT.mkdir(parents=True,exist_ok=True)
REPO=os.environ["GITHUB_REPOSITORY"]
if REPO!="leodbc/Pub-Sub-Architecture":raise RuntimeError("Not the authorized source repository")
RUN=os.environ["GITHUB_RUN_ID"];SHA=os.environ["GITHUB_SHA"]
# Explicit allowlist: exact artifacts from successful runs, unchanged ZIP bytes.
ARTIFACTS={
 "run17-e1e4":12835412789, "run17-e5e7":12835118691,
 "run17-e6e8":12835184713, "run17-e9":12835697963,
 "e3-client-visibility":12849007298,
 "e5-e8-audit":12848882349,
 "e6-kafka-isolated":12850545819,
 "e6-nats-isolated":12851670501
}
EXPECTED_SHA256={
 "run17-e1e4":"fd3faa27ef9e84c7cf23f1a74e9f3f2fc2285b12df23019eb62cc8167044c064",
 "run17-e5e7":"0890b463784056323f8f2c5122de2252d3d0c4405b4be99e3399ca7975d9d33e",
 "run17-e6e8":"f1df779b1a00365910810de51fdff99b25f6cbda16ac4078a02a6d823693dd3d",
 "run17-e9":"4a16547dafc2a41bdc74475135595ddf78daca1afe1038239999ac33517cd06a",
 "e3-client-visibility":"9a5f070335a376576858d86137439c51633e2e4a4d34afc14b8709ffe92da410",
 "e5-e8-audit":"f46c2b2390461b32cca0b7e500c2ab8d7c8cc84c2308dc5f82161949f18d3c0c",
 "e6-kafka-isolated":"1a5db4f56707fdd4ce3d832fca57c22055574672a3a6ab3d8e89401932ec26af",
 "e6-nats-isolated":"480a2ba7884714a1c85f2a0ced5d70d36a73377edad0285b1c7e63ebe07d9598"
}
def gh(args,binary=False):
 p=subprocess.run(["gh","api",*args],stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=False)
 if p.returncode:raise RuntimeError(f"GitHub API error on {args[0]}: {p.stderr.decode(errors='replace')[-1200:]}")
 return p.stdout if binary else json.loads(p.stdout)
def sha(b):return hashlib.sha256(b).hexdigest()
def clean_zip(raw):
 # Do not leak any obvious credential material into a public release.
 patterns=[rb'github_pat_[A-Za-z0-9_]{10,}',rb'gh[pousr]_[A-Za-z0-9]{20,}',
           rb'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----',
           rb'Authorization:\s*Bearer\s+\S+']
 with zipfile.ZipFile(io.BytesIO(raw)) as f:
  names=f.namelist()
  if any(n.startswith("/") or ".." in Path(n).parts for n in names):raise RuntimeError("unsafe ZIP member")
  for n in names:
   if n.endswith("/"):continue
   if Path(n).suffix.lower() in (".pem",".key",".p12",".pfx",".env"):
    raise RuntimeError("sensitive file suffix in artifact: "+n)
   d=f.read(n)
   if any(re.search(p,d,re.I) for p in patterns):
    raise RuntimeError("possible credential in artifact member "+n)
  return [{"name":n,"sha256":sha(f.read(n)),"bytes":len(f.read(n))}
          for n in names if not n.endswith("/")]
items=[]
for logical,artifact_id in ARTIFACTS.items():
 meta=gh([f"/repos/{REPO}/actions/artifacts/{artifact_id}"])
 if meta.get("expired"):raise RuntimeError(f"Original artifact {artifact_id} expired")
 raw=gh([f"/repos/{REPO}/actions/artifacts/{artifact_id}/zip"],binary=True)
 measured=sha(raw)
 expected=meta.get("digest","").removeprefix("sha256:")
 if not expected or measured!=expected or measured!=EXPECTED_SHA256[logical]:raise RuntimeError(f"SHA mismatch for artifact {artifact_id}: {measured} != {expected} != anchored {EXPECTED_SHA256[logical]}")
 members=clean_zip(raw)
 name=f"{logical}__artifact_{artifact_id}.zip"
 (OUT/name).write_bytes(raw)
 items.append({"logical":logical,"github_artifact_id":artifact_id,"source_run_id":meta.get("workflow_run",{}).get("id"),
   "artifact_name":meta.get("name"),"sha256":measured,"size_bytes":len(raw),"members":members,"filename":name})
# Targeted normative job produces its own artifact, downloaded by Actions into this path.
target=ROOT/"artifacts"/"normative_download"
if not target.is_dir() or not any(target.rglob("*.log")):
 raise RuntimeError("Targeted normative evidence not downloaded")
import shutil
normative=OUT/f"fase1-normative-run-{RUN}.zip"
with zipfile.ZipFile(normative,"w",compression=zipfile.ZIP_DEFLATED,compresslevel=9) as z:
 for p in sorted(target.rglob("*")):
  if p.is_file():z.write(p,p.relative_to(target).as_posix())
raw=normative.read_bytes()
members=clean_zip(raw)
items.append({"logical":"normative-E2-E5-E7-E8","source_run_id":int(RUN),"sha256":sha(raw),
 "size_bytes":len(raw),"members":members,"filename":normative.name})
manifest={"schema":"m2-fase1-evidence-archive-v1","created_utc":datetime.datetime.now(datetime.timezone.utc).isoformat(),
 "repository":REPO,"source_branch":"feat/m2-actions-evidence-20261028",
 "archive_producing_commit":SHA,"targeted_run_id":int(RUN),
 "historical_original_commit":"6b08f81cbd26477fdc1ab1858654a10953bb18ce",
 "canonical_document_commit":"e2cb5f0120f9ce8fc49ce8940c1b431880e659ea",
 "previous_success_runs":[40732373570,40768778541,40768778718],
 "sources":items,"semantics":"unchanged GitHub Actions artifact zip bytes and raw per-member sha256; normative ZIP repackaged deterministically from job output",
 "constraints":"no workflow job logs, tokens, private keys or runtime environment captured"}
(OUT/"manifest.json").write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+"\n")
notes=f"""# UNIVALI M2 — Evidências da Fase 1

Arquivamento duradouro, separado do vencimento de 27/11/2026 dos artifacts GitHub Actions.
Commit: {SHA}. Workflow targeted: {RUN}.
Resultados: arquivos ZIP originais preservados byte a byte e novo teste normativo E2/E5/E7/E8.
Verifique \`manifest.json\` (SHA-256 por ZIP e membro). Os artifacts originais são
referenciados por ID e commit. Este arquivamento não afirma propriedades não testadas.

Não contém relatório acadêmico final, nem arquivos de credenciais.
"""
(OUT/"NOTAS_RELEASE.md").write_text(notes)
print("VERIFIED_ARCHIVE_ITEMS",len(items),"TOTAL_BYTES",sum(x["size_bytes"] for x in items),flush=True)

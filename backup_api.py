#!/usr/bin/env python3
"""
FastAPI front-end for the CouchDB → S3 backup worker.

The service exposes REST endpoints and a lightweight dashboard for:
- Starting and stopping per-database "watchdog" workers.
- Monitoring worker health, checkpoint position, and errors.
- Restoring selected documents (attachments included) back to CouchDB.

Install and run
---------------
    pip install fastapi uvicorn pydantic requests fsspec s3fs
    uvicorn backup_api:app --host 0.0.0.0 --port 8000

Authentication
--------------
HTTP Basic authentication is required for every request. Configure credentials via
environment variables:

    export BASIC_USER='your-user'
    export BASIC_PASS='your-strong-pass'

Open the dashboard at http://localhost:8000/ and use the Watchdogs panel to manage
workers or the Restore panel to re-hydrate documents.
"""

import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional
import secrets

from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings

# Import your existing module (must be in PYTHONPATH / same folder)
from couchdb_to_s3 import (  # type: ignore
    CouchDBClient,
    S3Storage,
    CouchToS3Backup,
    JSONLogger,
)

# ----------------------------- Settings -----------------------------

class Settings(BaseSettings):
    # Defaults can be overridden by environment variables or request body
    COUCH_URL: str = "http://admin:pass@127.0.0.1:5984"
    S3_BUCKET: str = "my-archive-bucket"
    ROOT_PREFIX: str = ""          # not applied to checkpoint
    AWS_REGION: Optional[str] = None
    S3_ENDPOINT_URL: Optional[str] = None  # e.g. http://127.0.0.1:9000 for MinIO
    LONGPOLL_TIMEOUT: int = 60
    BULK_CHUNK_SIZE: int = 500
    CHECKPOINT_S3_KEY: Optional[str] = None
    DEFAULT_SINCE: Optional[str] = None  # e.g., "now" or None to use checkpoint/"0"

    # Simple auth
    BASIC_USER: str = "admin"
    BASIC_PASS: str = "admin"

    class Config:
        case_sensitive = True

settings = Settings()

security = HTTPBasic()

def require_auth(credentials: HTTPBasicCredentials = Depends(security)):
    user_ok = secrets.compare_digest(credentials.username, settings.BASIC_USER)
    pass_ok = secrets.compare_digest(credentials.password, settings.BASIC_PASS)
    if not (user_ok and pass_ok):
        # Force browser auth dialog
        raise HTTPException(
            status_code=401, detail="Unauthorized", headers={"WWW-Authenticate": "Basic"}
        )
    return True

# ----------------------------- Models -------------------------------

class WatchdogConfig(BaseModel):
    couch_url: Optional[str] = Field(default=None, description="Override default COUCH_URL")
    s3_bucket: Optional[str] = Field(default=None, description="Override default S3_BUCKET")
    root_prefix: Optional[str] = Field(default=None, description="S3 root prefix (not for last_seq)")
    aws_region: Optional[str] = None
    endpoint_url: Optional[str] = Field(default=None, description="Custom S3 endpoint URL (MinIO/LocalStack)")
    checkpoint_s3_key: Optional[str] = None
    longpoll_timeout: Optional[int] = Field(default=None, ge=1, le=3600)
    bulk_chunk_size: Optional[int] = Field(default=None, ge=1, le=5000)
    since: Optional[str] = Field(default=None, description="Since token or 'now' (per DB)")

    def resolved(self) -> "WatchdogConfig":
        """Fill missing fields from global settings."""
        return WatchdogConfig(
            couch_url=self.couch_url or settings.COUCH_URL,
            s3_bucket=self.s3_bucket or settings.S3_BUCKET,
            root_prefix=settings.ROOT_PREFIX if self.root_prefix is None else self.root_prefix,
            aws_region=self.aws_region if self.aws_region is not None else settings.AWS_REGION,
            endpoint_url=self.endpoint_url if self.endpoint_url is not None else settings.S3_ENDPOINT_URL,
            checkpoint_s3_key=self.checkpoint_s3_key if self.checkpoint_s3_key is not None else settings.CHECKPOINT_S3_KEY,
            longpoll_timeout=self.longpoll_timeout if self.longpoll_timeout is not None else settings.LONGPOLL_TIMEOUT,
            bulk_chunk_size=self.bulk_chunk_size if self.bulk_chunk_size is not None else settings.BULK_CHUNK_SIZE,
            since=self.since if self.since is not None else settings.DEFAULT_SINCE,
        )

class StartWatchdogsRequest(BaseModel):
    dbs: List[str] = Field(..., description="Database names to start watchdogs for (comma-separated allowed)")
    config: WatchdogConfig = Field(default_factory=WatchdogConfig)

class StartWatchdogsResponse(BaseModel):
    started: List[str]
    already_running: List[str]

class StopWatchdogResponse(BaseModel):
    stopped: bool

class WatchdogStatus(BaseModel):
    db: str
    running: bool
    thread_name: Optional[str] = None
    started_at: Optional[str] = None
    last_checkpoint: Optional[str] = None
    last_error: Optional[str] = None
    config: WatchdogConfig

class RestoreRequest(BaseModel):
    db: str
    doc_ids: List[str]
    overwrite: bool = False
    # Optional overrides
    config: WatchdogConfig = Field(default_factory=WatchdogConfig)

    @field_validator("doc_ids")
    def non_empty_docs(cls, v):
        if not v:
            raise ValueError("doc_ids must not be empty")
        return v

class RestoreResult(BaseModel):
    db: str
    doc_id: str
    final_rev: Optional[str] = None
    attachments_uploaded: Optional[int] = 0
    ok: bool = True
    error: Optional[str] = None

# --------------------------- Watchdog Core --------------------------

class WatchdogWorker:
    """
    Wraps one backup runner in a managed thread so it can be started/stopped via API.
    """
    def __init__(self, db: str, cfg: WatchdogConfig):
        self.db = db
        self.cfg = cfg.resolved()
        self.started_at: Optional[datetime] = None
        self.last_error: Optional[str] = None

        # Per-DB clients (re-used for status calls like reading checkpoint)
        self._couch = CouchDBClient(base_url=self.cfg.couch_url, db=self.db)
        self._s3 = S3Storage(
            bucket=self.cfg.s3_bucket,
            root_prefix=self.cfg.root_prefix or "",
            db=self.db,
            region=self.cfg.aws_region,
            endpoint_url=self.cfg.endpoint_url,
        )
        self.runner = CouchToS3Backup(
            couch=self._couch,
            s3store=self._s3,
            db=self.db,
            longpoll_timeout=self.cfg.longpoll_timeout,
            bulk_chunk_size=self.cfg.bulk_chunk_size,
            extra_checkpoint_key=self.cfg.checkpoint_s3_key,
        )

    def start(self):
        if self.runner and self.runner.is_alive(): return
        self.runner.start()
        self.started_at = datetime.now(timezone.utc)

    def stop(self, timeout: float = 10.0) -> bool:
        if not self.runner: return True
        # Give the worker enough time (longpoll may have just aborted).
        wait = max(timeout, (self.cfg.longpoll_timeout or 60) + 2)
        self.runner.stop(timeout=wait)
        return not self.runner.is_alive()

    def running(self) -> bool:
        return self.runner is not None and self.runner.is_alive()

    def status(self) -> WatchdogStatus:
        # Read the S3 checkpoint each time status is requested
        last_seq = self._s3.read_checkpoint(self.cfg.checkpoint_s3_key)
        return WatchdogStatus(
            db=self.db,
            running=self.running(),
            thread_name=self.runner._thread.name if self.runner else None,
            started_at=self.started_at.isoformat() if self.started_at else None,
            last_checkpoint=last_seq,
            last_error=self.last_error,
            config=self.cfg,
        )

class WatchdogManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._workers: Dict[str, WatchdogWorker] = {}

    def _normalize_dbs(self, dbs: List[str]) -> List[str]:
        out: List[str] = []
        for v in dbs:
            out.extend([p for p in (s.strip() for s in v.split(",")) if p])
        seen, ordered = set(), []
        for d in out:
            if d not in seen:
                seen.add(d)
                ordered.append(d)
        return ordered

    def start_many(self, dbs: List[str], cfg: WatchdogConfig) -> StartWatchdogsResponse:
        dbs = self._normalize_dbs(dbs)
        started, already = [], []
        with self._lock:
            for db in dbs:
                w = self._workers.get(db)
                if w and w.running():
                    already.append(db)
                    continue
                w = WatchdogWorker(db=db, cfg=cfg)
                w.start()
                self._workers[db] = w
                started.append(db)
        return StartWatchdogsResponse(started=started, already_running=already)

    def stop_one(self, db: str) -> bool:
        with self._lock:
            w = self._workers.get(db)
            if not w:
                return True
            ok = w.stop()
            return ok

    def status_all(self) -> List[WatchdogStatus]:
        with self._lock:
            return [w.status() for w in self._workers.values()]

    def status_one(self, db: str) -> WatchdogStatus:
        with self._lock:
            w = self._workers.get(db)
            if not w:
                raise KeyError(db)
            return w.status()

manager = WatchdogManager()

# ----------------------------- FastAPI ------------------------------

app = FastAPI(title="CouchDB↔S3 Backup API", version="1.1.0")

@app.get("/healthz")
def healthz():
    return {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}

# ---- GUI (protected) ----
@app.get("/", response_class=HTMLResponse)
def home(_: bool = Depends(require_auth)):
    return HTMLResponse(content=_DASHBOARD_HTML)

# ---- Watchdogs (protected) ----
@app.post("/watchdogs/start", response_model=StartWatchdogsResponse)
def start_watchdogs(req: StartWatchdogsRequest, _: bool = Depends(require_auth)):
    dbs = [d.strip() for d in req.dbs if d.strip()]
    if not dbs:
        raise HTTPException(status_code=400, detail="No databases provided.")
    res = manager.start_many(dbs=dbs, cfg=req.config)
    return res

@app.get("/watchdogs", response_model=List[WatchdogStatus])
def list_watchdogs(_: bool = Depends(require_auth)):
    return manager.status_all()

@app.get("/watchdogs/{db}", response_model=WatchdogStatus)
def get_watchdog(db: str, _: bool = Depends(require_auth)):
    try:
        return manager.status_one(db)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Watchdog for db '{db}' not found.")

@app.post("/watchdogs/{db}/stop", response_model=StopWatchdogResponse)
def stop_watchdog(db: str, _: bool = Depends(require_auth)):
    ok = manager.stop_one(db)
    if not ok:
        raise HTTPException(status_code=500, detail=f"Failed to stop watchdog for '{db}'.")
    return StopWatchdogResponse(stopped=True)

# ---- Restore (protected) ----
@app.post("/restore", response_model=List[RestoreResult])
def restore_docs(req: RestoreRequest, _: bool = Depends(require_auth)):
    cfg = req.config.resolved()
    # Create fresh clients for the restore job
    couch = CouchDBClient(base_url=cfg.couch_url, db=req.db)
    s3 = S3Storage(
        bucket=cfg.s3_bucket,
        root_prefix=cfg.root_prefix or "",
        db=req.db,
        region=cfg.aws_region,
        endpoint_url=cfg.endpoint_url,
    )
    results: List[RestoreResult] = []
    for doc_id in req.doc_ids:
        try:
            out = s3.restore_doc_to_couch(couch, doc_id=doc_id, overwrite=req.overwrite)
            results.append(
                RestoreResult(
                    db=req.db,
                    doc_id=doc_id,
                    final_rev=out.get("final_rev"),
                    attachments_uploaded=out.get("attachments_uploaded", 0),
                    ok=True,
                )
            )
        except Exception as e:
            JSONLogger.log("restore-doc-failed", db=req.db, doc_id=doc_id, error=str(e))
            results.append(RestoreResult(db=req.db, doc_id=doc_id, ok=False, error=str(e)))
    return results

# ----------------------------- HTML UI ------------------------------

_DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>CouchDB ↔ S3 Backup Dashboard</title>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <style>
    :root { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji"; }
    body { margin: 0; background: #0b1020; color: #e7ecff; }
    header { padding: 16px 20px; background: #111632; border-bottom: 1px solid #232a4a; }
    h1 { margin: 0; font-size: 20px; }
    main { padding: 20px; display: grid; gap: 16px; grid-template-columns: 1fr; max-width: 1100px; margin: 0 auto; }
    section { background: #121736; border: 1px solid #232a4a; border-radius: 10px; padding: 16px; }
    h2 { margin: 0 0 12px; font-size: 16px; }
    .grid { display: grid; gap: 10px; grid-template-columns: repeat(auto-fit, minmax(220px,1fr)); }
    label { font-size: 12px; color: #9fb0ff; display: block; margin-bottom: 6px; }
    input[type="text"], input[type="number"] { width: 100%; padding: 8px 10px; background: #0b1130; color: #e7ecff; border: 1px solid #28305a; border-radius: 8px; }
    input[type="checkbox"] { transform: scale(1.2); margin-right: 8px; }
    button { padding: 8px 12px; border-radius: 8px; border: 1px solid #33408a; background: #1a237e; color: #fff; cursor: pointer; }
    button:hover { filter: brightness(1.1); }
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th, td { border-bottom: 1px solid #232a4a; padding: 8px; text-align: left; }
    .muted { color: #9fb0ff; }
    .row { display:flex; gap:10px; flex-wrap: wrap; align-items: center; }
    .pill { padding: 2px 8px; border-radius: 999px; font-size: 12px; border: 1px solid #2b3568; }
    .ok { background: #153c2b; border-color: #1b5e37; color:#9ff0c2; }
    .bad { background: #3c1525; border-color: #6b1b2f; color:#ffc1d0; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
    .tiny { font-size: 12px; }
    .vsep { height: 1px; background: #232a4a; margin: 12px 0; }
  </style>
</head>
<body>
  <header><h1>CouchDB ↔ S3 Backup Dashboard</h1></header>
  <main>
    <section>
      <h2>Watchdogs</h2>
      <div class="grid">
        <div>
          <label>DBs (comma-separated)</label>
          <input id="dbs" type="text" placeholder="db1,db2"/>
        </div>
        <div>
          <label>Since (optional: token or 'now')</label>
          <input id="since" type="text" placeholder="e.g. now"/>
        </div>
        <div>
          <label>Longpoll timeout (sec)</label>
          <input id="lp" type="number" value="60" min="1" max="3600"/>
        </div>
        <div>
          <label>Bulk chunk size</label>
          <input id="chunk" type="number" value="500" min="1" max="5000"/>
        </div>
      </div>
      <div class="grid" style="margin-top:10px">
        <div>
          <label>Couch URL</label>
          <input id="couch" type="text" placeholder="http://admin:pass@127.0.0.1:5984"/>
        </div>
        <div>
          <label>S3 Bucket</label>
          <input id="bucket" type="text" placeholder="my-archive-bucket"/>
        </div>
        <div>
          <label>S3 Root Prefix (optional)</label>
          <input id="root" type="text" placeholder="prefix/if/any"/>
        </div>
        <div>
          <label>S3 Endpoint URL (optional)</label>
          <input id="endpoint" type="text" placeholder="http://127.0.0.1:9000 (MinIO)"/>
        </div>
        <div>
          <label>Extra Checkpoint Key (optional)</label>
          <input id="ckey" type="text" placeholder="alt/last_seq/key"/>
        </div>
      </div>
      <div class="row" style="margin-top:10px">
        <button onclick="startWatchdogs()">Start</button>
        <button onclick="refreshWatchdogs()">Refresh</button>
      </div>
      <div class="vsep"></div>
      <div id="wd_table"></div>
    </section>

    <section>
      <h2>Restore Document(s)</h2>
      <div class="grid">
        <div>
          <label>DB</label>
          <input id="r_db" type="text" placeholder="db1"/>
        </div>
        <div>
          <label>Doc IDs (comma-separated)</label>
          <input id="r_docs" type="text" placeholder="user:42, invoice:2024-09"/>
        </div>
        <div style="display:flex; align-items:center; margin-top:22px">
          <label class="row"><input id="r_overwrite" type="checkbox"/>Overwrite if exists</label>
        </div>
      </div>
      <div class="grid" style="margin-top:10px">
        <div>
          <label>Couch URL (optional override)</label>
          <input id="r_couch" type="text" placeholder="leave empty to use defaults"/>
        </div>
        <div>
          <label>S3 Bucket (optional override)</label>
          <input id="r_bucket" type="text" placeholder="leave empty to use defaults"/>
        </div>
        <div>
          <label>Root Prefix (optional)</label>
          <input id="r_root" type="text" placeholder="leave empty to use defaults"/>
        </div>
        <div>
          <label>S3 Endpoint URL (optional)</label>
          <input id="r_endpoint" type="text" placeholder="http://127.0.0.1:9000"/>
        </div>
      </div>
      <div class="row" style="margin-top:10px">
        <button onclick="restoreDocs()">Restore</button>
      </div>
      <div class="vsep"></div>
      <pre id="restore_out" class="mono tiny"></pre>
    </section>

    <section class="tiny muted">
      <div>Tip: Basic-auth is browser-native. To change credentials, set <span class="mono">BASIC_USER</span>/<span class="mono">BASIC_PASS</span> env vars and restart.</div>
    </section>
  </main>

<script>
async function jget(url) {
  const r = await fetch(url, {method: 'GET', headers: {'content-type':'application/json'}});
  if(!r.ok){ throw new Error(await r.text()); }
  return await r.json();
}
async function jpost(url, body) {
  const r = await fetch(url, {method: 'POST', headers: {'content-type':'application/json'}, body: JSON.stringify(body)});
  if(!r.ok){ throw new Error(await r.text()); }
  return await r.json();
}

function val(id){ return document.getElementById(id).value.trim(); }
function set(id, v){ document.getElementById(id).innerText = v; }

function renderTable(rows){
  if(rows.length === 0){
    document.getElementById('wd_table').innerHTML = '<div class="muted">No watchdogs running.</div>'; return;
  }
  let html = '<table><thead><tr><th>DB</th><th>Status</th><th>Checkpoint</th><th>Started</th><th>Thread</th><th>Last Error</th><th></th></tr></thead><tbody>';
  for(const r of rows){
    html += `<tr>
      <td class="mono">${r.db}</td>
      <td>${r.running ? '<span class="pill ok">running</span>' : '<span class="pill bad">stopped</span>'}</td>
      <td class="mono">${r.last_checkpoint ?? ''}</td>
      <td class="tiny">${r.started_at ?? ''}</td>
      <td class="tiny mono">${r.thread_name ?? ''}</td>
      <td class="tiny">${r.last_error ?? ''}</td>
      <td><button onclick="stopOne('${r.db}')">Stop</button></td>
    </tr>`;
  }
  html += '</tbody></table>';
  document.getElementById('wd_table').innerHTML = html;
}

async function refreshWatchdogs(){
  try{
    const data = await jget('/watchdogs');
    renderTable(data);
  }catch(e){ alert('Failed to load watchdogs: ' + e.message); }
}

async function startWatchdogs(){
  const body = {
    dbs: val('dbs').split(',').map(s=>s.trim()).filter(Boolean),
    config: {
      couch_url: val('couch') || null,
      s3_bucket: val('bucket') || null,
      root_prefix: val('root') || null,
      endpoint_url: val('endpoint') || null,
      checkpoint_s3_key: val('ckey') || null,
      since: val('since') || null,
      longpoll_timeout: parseInt(val('lp') || '60', 10),
      bulk_chunk_size: parseInt(val('chunk') || '500', 10)
    }
  };
  try{
    const res = await jpost('/watchdogs/start', body);
    await refreshWatchdogs();
    alert('Started: ' + res.started.join(', ') + (res.already_running.length? ' | Already running: ' + res.already_running.join(', ') : ''));
  }catch(e){ alert('Start failed: ' + e.message); }
}

async function stopOne(db){
  try{
    await jpost(`/watchdogs/${encodeURIComponent(db)}/stop`, {});
    await refreshWatchdogs();
  }catch(e){ alert('Stop failed: ' + e.message); }
}

async function restoreDocs(){
  const body = {
    db: val('r_db'),
    doc_ids: val('r_docs').split(',').map(s=>s.trim()).filter(Boolean),
    overwrite: document.getElementById('r_overwrite').checked,
    config: {
      couch_url: val('r_couch') || null,
      s3_bucket: val('r_bucket') || null,
      root_prefix: val('r_root') || null,
      endpoint_url: val('r_endpoint') || null
    }
  };
  try{
    const res = await jpost('/restore', body);
    set('restore_out', JSON.stringify(res, null, 2));
  }catch(e){ alert('Restore failed: ' + e.message); }
}

refreshWatchdogs();
</script>
</body>
</html>
"""

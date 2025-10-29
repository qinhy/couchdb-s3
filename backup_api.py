#!/usr/bin/env python3
"""
backup_api.py

FastAPI service for:
- Managing "watchdogs" (long-poll backup workers per CouchDB DB) to S3
- Restoring a document (including attachments) from S3 back to CouchDB

Relies on classes provided by your OOP module:
- CouchDBClient
- S3Storage
- CouchToS3Backup
- JSONLogger

Run:
  pip install fastapi uvicorn pydantic boto3 requests
  uvicorn backup_api:app --host 0.0.0.0 --port 8000 --reload

Security note:
  This demo has no authentication or rate limiting. Put this behind an auth proxy
  or add FastAPI auth (API keys / OAuth2) before exposing it publicly.
"""
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, BaseSettings, Field, validator

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
    LONGPOLL_TIMEOUT: int = 60
    BULK_CHUNK_SIZE: int = 500
    CHECKPOINT_S3_KEY: Optional[str] = None
    DEFAULT_SINCE: Optional[str] = None  # e.g., "now" or None to use checkpoint/"0"

    class Config:
        case_sensitive = True


settings = Settings()


# ----------------------------- Models -------------------------------

class WatchdogConfig(BaseModel):
    couch_url: Optional[str] = Field(default=None, description="Override default COUCH_URL")
    s3_bucket: Optional[str] = Field(default=None, description="Override default S3_BUCKET")
    root_prefix: Optional[str] = Field(default=None, description="S3 root prefix (not for last_seq)")
    aws_region: Optional[str] = None
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
            checkpoint_s3_key=self.checkpoint_s3_key if self.checkpoint_s3_key is not None else settings.CHECKPOINT_S3_KEY,
            longpoll_timeout=self.longpoll_timeout if self.longpoll_timeout is not None else settings.LONGPOLL_TIMEOUT,
            bulk_chunk_size=self.bulk_chunk_size if self.bulk_chunk_size is not None else settings.BULK_CHUNK_SIZE,
            since=self.since if self.since is not None else settings.DEFAULT_SINCE,
        )


class StartWatchdogsRequest(BaseModel):
    dbs: List[str] = Field(..., description="Database names to start watchdogs for")
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

    @validator("doc_ids")
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
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.started_at: Optional[datetime] = None
        self.last_error: Optional[str] = None

        # Per-DB clients (re-used for status calls like reading checkpoint)
        self._couch = CouchDBClient(base_url=self.cfg.couch_url, db=self.db)
        self._s3 = S3Storage(
            bucket=self.cfg.s3_bucket,
            root_prefix=self.cfg.root_prefix or "",
            db=self.db,
            region=self.cfg.aws_region,
        )

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        runner = CouchToS3Backup(
            couch=self._couch,
            s3store=self._s3,
            db=self.db,
            longpoll_timeout=self.cfg.longpoll_timeout,
            bulk_chunk_size=self.cfg.bulk_chunk_size,
            extra_checkpoint_key=self.cfg.checkpoint_s3_key,
            stop_event=self.stop_event,
        )

        def _target():
            try:
                runner.run(start_since=self.cfg.since)
            except Exception as e:
                self.last_error = str(e)
                JSONLogger.log("watchdog-crashed", db=self.db, error=str(e))

        self.thread = threading.Thread(target=_target, name=f"watchdog-{self.db}", daemon=True)
        self.thread.start()
        self.started_at = datetime.now(timezone.utc)

    def stop(self, timeout: float = 10.0) -> bool:
        if not self.thread:
            return True
        self.stop_event.set()
        self.thread.join(timeout=timeout)
        return not self.thread.is_alive()

    def running(self) -> bool:
        return self.thread is not None and self.thread.is_alive()

    def status(self) -> WatchdogStatus:
        # Read the S3 checkpoint each time status is requested
        last_seq = self._s3.read_checkpoint(self.cfg.checkpoint_s3_key)
        return WatchdogStatus(
            db=self.db,
            running=self.running(),
            thread_name=self.thread.name if self.thread else None,
            started_at=self.started_at.isoformat() if self.started_at else None,
            last_checkpoint=last_seq,
            last_error=self.last_error,
            config=self.cfg,
        )


class WatchdogManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._workers: Dict[str, WatchdogWorker] = {}

    def start_many(self, dbs: List[str], cfg: WatchdogConfig) -> StartWatchdogsResponse:
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

app = FastAPI(title="CouchDB↔S3 Backup API", version="1.0.0")


@app.get("/healthz")
def healthz():
    return {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}


# ---- Watchdogs ----

@app.post("/watchdogs/start", response_model=StartWatchdogsResponse)
def start_watchdogs(req: StartWatchdogsRequest):
    dbs = [d.strip() for d in req.dbs if d.strip()]
    if not dbs:
        raise HTTPException(status_code=400, detail="No databases provided.")
    res = manager.start_many(dbs=dbs, cfg=req.config)
    return res


@app.get("/watchdogs", response_model=List[WatchdogStatus])
def list_watchdogs():
    return manager.status_all()


@app.get("/watchdogs/{db}", response_model=WatchdogStatus)
def get_watchdog(db: str):
    try:
        return manager.status_one(db)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Watchdog for db '{db}' not found.")


@app.post("/watchdogs/{db}/stop", response_model=StopWatchdogResponse)
def stop_watchdog(db: str):
    ok = manager.stop_one(db)
    if not ok:
        raise HTTPException(status_code=500, detail=f"Failed to stop watchdog for '{db}'.")
    return StopWatchdogResponse(stopped=True)


# ---- Restore ----

@app.post("/restore", response_model=List[RestoreResult])
def restore_docs(req: RestoreRequest):
    cfg = req.config.resolved()
    # Create fresh clients for the restore job
    couch = CouchDBClient(base_url=cfg.couch_url, db=req.db)
    s3 = S3Storage(
        bucket=cfg.s3_bucket,
        root_prefix=cfg.root_prefix or "",
        db=req.db,
        region=cfg.aws_region,
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

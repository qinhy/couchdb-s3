#!/usr/bin/env python3
"""
couchdb_to_s3.py

CouchDB -> S3 "just-backup" with **deduplicated attachments** and **S3-only checkpoint**.
Also supports **restore of selected documents** (including attachments) from S3 back to CouchDB.

S3 layout (per DB)
- Doc JSON (no attachment bodies):   s3://<bucket>/<db>/<url-encoded id>/data.json.zip
- Attachments by digest (deduped):   s3://<bucket>/<db>/_attachments/<url-encoded digest>/data.zip
- Per-doc attachment map:            s3://<bucket>/<db>/<url-encoded id>/attachments.json
- Canonical checkpoint (plain text): s3://<bucket>/<db>/last_seq   (note: NOT under root-prefix)

Usage
  pip install requests boto3

  # Continuous backup (single or multiple DBs)
  python couchdb_to_s3.py \
    --couch-url http://admin:pass@127.0.0.1:5984 \
    --db db1 --db db2,db3 \
    --s3-bucket my-archive-bucket

  # Restore one or more docs for one or more DBs (applies doc list to each DB)
  python couchdb_to_s3.py \
    --couch-url http://admin:pass@127.0.0.1:5984 \
    --db mydb \
    --s3-bucket my-archive-bucket \
    --restore-doc docA --restore-doc docB,docC \
    --overwrite
"""
import argparse
import datetime as dt
import hashlib
import io
import json
import signal
import sys
import threading
import time
import zipfile
from typing import Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
import requests
from urllib.parse import quote as urlquote


# ---------- Utilities ----------

def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat() + "Z"


class JSONLogger:
    @staticmethod
    def log(msg: str, **kv):
        rec = {"ts": utc_now(), "msg": msg}
        rec.update(kv)
        print(json.dumps(rec, separators=(",", ":")), file=sys.stderr, flush=True)


# ---------- CouchDB client ----------

class CouchDBClient:
    def __init__(self, base_url: str, db: str, timeout_connect=10, timeout_read=120):
        self.base_url = base_url.rstrip("/")
        self.db = db
        self.session = requests.Session()
        self.timeout = (timeout_connect, timeout_read)

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        kwargs.setdefault("timeout", self.timeout)
        r = self.session.request(method, url, **kwargs)
        if not r.ok:
            try:
                detail = r.json()
            except Exception:
                detail = r.text[:500]
            raise RuntimeError(f"HTTP {method} {url} -> {r.status_code}: {detail}")
        return r

    def changes(self, since: str, timeout_s: int) -> Dict:
        params = {
            "feed": "longpoll",
            "since": since,
            "timeout": timeout_s * 1000,
            "heartbeat": 60000,
            "style": "main_only",
        }
        url = f"{self.base_url}/{self.db}/_changes"
        return self._request("GET", url, params=params).json()

    def bulk_get_docs(self, ids_revs: List[Dict]) -> List[Dict]:
        payload = {"docs": ids_revs, "attachments": False, "att_encoding_info": False}
        url = f"{self.base_url}/{self.db}/_bulk_get"
        r = self._request("POST", url, json=payload)
        docs = []
        for res in r.json().get("results", []):
            for d in res.get("docs", []):
                if "ok" in d:
                    docs.append(d["ok"])
        return docs

    def fetch_attachment_bytes(self, doc_id: str, rev: str, att_name: str) -> bytes:
        url = (
            f"{self.base_url}/{self.db}/"
            f"{urlquote(doc_id, safe='')}/{urlquote(att_name, safe='')}"
        )
        params = {"rev": rev}
        with self.session.get(url, params=params, stream=True, timeout=(10, 600)) as resp:
            if not resp.ok:
                raise RuntimeError(
                    f"GET attachment failed: {resp.status_code} {resp.text[:200]}"
                )
            return resp.content  # loads into memory

    # ---------- restore helpers ----------
    def get_doc_rev(self, doc_id: str) -> Optional[str]:
        """Return current _rev for doc or None if missing."""
        url = f"{self.base_url}/{self.db}/{urlquote(doc_id, safe='')}"
        r = self.session.head(url, timeout=self.timeout)
        if r.status_code == 404:
            return None
        if not r.ok:
            try:
                detail = r.json()
            except Exception:
                detail = r.text[:500]
            raise RuntimeError(f"HTTP HEAD {url} -> {r.status_code}: {detail}")
        etag = r.headers.get("ETag")
        if etag:
            return etag.strip('"')
        # Fallback: GET
        data = self._request("GET", url, params={"conflicts": "false", "revs": "false"}).json()
        return data.get("_rev")

    def put_doc(self, doc: Dict) -> Dict:
        """Create/update a document (JSON only, no attachments)."""
        doc_id = doc["_id"]
        url = f"{self.base_url}/{self.db}/{urlquote(doc_id, safe='')}"
        return self._request("PUT", url, json=doc).json()

    def put_attachment(
        self,
        doc_id: str,
        att_name: str,
        data: bytes,
        rev: str,
        content_type: Optional[str] = None,
    ) -> str:
        """Upload/replace a single attachment, returning the new _rev."""
        url = f"{self.base_url}/{self.db}/{urlquote(doc_id, safe='')}/{urlquote(att_name, safe='')}"
        headers = {}
        if content_type:
            headers["Content-Type"] = content_type
        r = self.session.put(url, params={"rev": rev}, data=data, headers=headers, timeout=self.timeout)
        if not r.ok:
            try:
                detail = r.json()
            except Exception:
                detail = r.text[:500]
            raise RuntimeError(f"HTTP PUT attachment {url} -> {r.status_code}: {detail}")
        return r.json().get("rev")


# ---------- S3 Storage ----------

class S3Storage:
    def __init__(self, bucket: str, root_prefix: str, db: str, region: Optional[str]):
        self.s3 = boto3.client("s3", region_name=region or None)
        self.bucket = bucket

        rp = root_prefix.strip("/")
        self.base_prefix = (rp + "/" if rp else "") + f"{db}/"
        # Canonical checkpoint: <db>/last_seq (root prefix NOT applied)
        self.canonical_seq_key = f"{db}/last_seq"

    # ---- generic helpers ----
    @staticmethod
    def to_zip_bytes(filename: str, content: bytes, compresslevel=9) -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(
            buf, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=compresslevel
        ) as zf:
            zf.writestr(filename, content)
        return buf.getvalue()

    def key_exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def get_bytes(self, key: str) -> bytes:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()

    def put_bytes(self, key: str, body: bytes, content_type: str):
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType=content_type)

    def put_json(self, key: str, obj: Dict):
        body = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.put_bytes(key, body, "application/json")

    # ---- domain helpers (backup) ----
    def upload_doc_json_zip(self, doc: Dict) -> str:
        key = f"{self.base_prefix}{urlquote(doc['_id'], safe='')}/data.json.zip"
        body = self.to_zip_bytes(
            "data.json",
            json.dumps(doc, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        )
        self.put_bytes(key, body, "application/zip")
        return key

    def read_checkpoint(self, extra_key: Optional[str]) -> Optional[str]:
        # Try canonical first, then extra (if provided)
        for key in filter(None, [self.canonical_seq_key, extra_key]):
            try:
                obj = self.s3.get_object(Bucket=self.bucket, Key=key)
                return obj["Body"].read().decode("utf-8").strip()
            except ClientError:
                continue
        return None

    def write_checkpoint(self, value: str, extra_key: Optional[str]):
        body = value.encode("utf-8")
        self.put_bytes(self.canonical_seq_key, body, "text/plain; charset=utf-8")
        if extra_key:
            self.put_bytes(extra_key, body, "text/plain; charset=utf-8")

    # ---- domain helpers (restore) ----
    def _read_zip_member(self, zip_bytes: bytes, member_name: str) -> bytes:
        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            return zf.read(member_name)

    def _load_doc_from_s3(self, doc_id: str) -> Dict:
        key = f"{self.base_prefix}{urlquote(doc_id, safe='')}/data.json.zip"
        if not self.key_exists(key):
            raise FileNotFoundError(f"Missing doc JSON zip in S3: s3://{self.bucket}/{key}")
        data_json = self._read_zip_member(self.get_bytes(key), "data.json")
        return json.loads(data_json.decode("utf-8"))

    def _load_manifest_from_s3(self, doc_id: str) -> Optional[Dict]:
        key = f"{self.base_prefix}{urlquote(doc_id, safe='')}/attachments.json"
        if not self.key_exists(key):
            return None
        return json.loads(self.get_bytes(key).decode("utf-8"))

    def _load_attachment_bytes_from_s3(self, digest_key: str) -> bytes:
        """digest_key is like '<base_prefix>_attachments/<digest>/data.zip'"""
        if not self.key_exists(digest_key):
            raise FileNotFoundError(f"Missing attachment zip in S3: s3://{self.bucket}/{digest_key}")
        data_zip = self.get_bytes(digest_key)
        return self._read_zip_member(data_zip, "data")

    def restore_doc_to_couch(
        self,
        couch: CouchDBClient,
        doc_id: str,
        overwrite: bool = False,
    ) -> Dict:
        """
        Restore a single document (and its attachments, if present) from S3 to CouchDB.

        Steps:
          1) Load JSON from s3://<bucket>/<base_prefix>/<doc_id>/data.json.zip
          2) Write JSON (without _attachments) to CouchDB (create/update depending on `overwrite`)
          3) If attachments manifest exists, upload each attachment to CouchDB and advance _rev each time
        Returns summary dict.
        """
        # 1) Load base JSON from S3
        doc = self._load_doc_from_s3(doc_id)
        # Prepare JSON body: remove _attachments and possibly stale _rev
        body = {k: v for k, v in doc.items() if k != "_attachments"}
        if "_rev" in body:
            del body["_rev"]

        # 2) Create/update base doc
        current_rev = couch.get_doc_rev(doc_id)
        if current_rev and not overwrite:
            raise RuntimeError(f"Document '{doc_id}' already exists in CouchDB (rev={current_rev}); use overwrite.")

        body["_id"] = doc_id  # ensure id
        if current_rev:
            # overwrite mode
            body["_rev"] = current_rev

        res = couch.put_doc(body)
        new_rev = res.get("rev")
        JSONLogger.log("restore-doc-json-upsert", db=couch.db, doc_id=doc_id, rev=new_rev)

        # 3) Attachments (if any)
        manifest = self._load_manifest_from_s3(doc_id)
        uploaded_attachments = 0
        if manifest and manifest.get("attachments"):
            # Upload each attachment sequentially, threading the rev after each
            for name, meta in manifest["attachments"].items():
                # Resolve S3 location
                digest_key = meta.get("s3_key")
                if not digest_key:
                    # construct from digest if s3_key missing (backward-compat)
                    digest = meta.get("digest")
                    if not digest:
                        raise RuntimeError(f"Attachment entry for '{name}' missing 's3_key' and 'digest'")
                    digest_key = f"{self.base_prefix}_attachments/{urlquote(digest, safe='')}/data.zip"

                data = self._load_attachment_bytes_from_s3(digest_key)
                content_type = meta.get("content_type") or None
                new_rev = couch.put_attachment(doc_id, name, data, rev=new_rev, content_type=content_type)
                uploaded_attachments += 1
                JSONLogger.log("restore-attachment-uploaded", db=couch.db, doc_id=doc_id, name=name, rev=new_rev)

        return {
            "doc_id": doc_id,
            "db": couch.db,
            "final_rev": new_rev,
            "attachments_uploaded": uploaded_attachments,
        }


# ---------- Backup Orchestrator ----------

class CouchToS3Backup:
    def __init__(
        self,
        couch: CouchDBClient,
        s3store: S3Storage,
        db: str,
        longpoll_timeout: int,
        bulk_chunk_size: int,
        extra_checkpoint_key: Optional[str],
        stop_event: Optional[threading.Event] = None,
    ):
        self.couch = couch
        self.s3 = s3store
        self.db = db
        self.longpoll_timeout = longpoll_timeout
        self.bulk_chunk_size = bulk_chunk_size
        self.extra_checkpoint_key = extra_checkpoint_key
        self.stop_event = stop_event or threading.Event()

    # ---- attachment helpers ----
    @staticmethod
    def digest_or_compute(meta: Dict, data: Optional[bytes]) -> Tuple[str, Optional[bytes]]:
        if meta.get("digest"):
            return meta["digest"], data
        if data is None:
            return "", None  # caller will fetch and recompute
        h = hashlib.sha256(); h.update(data)
        return "sha256-" + h.hexdigest(), data

    def upload_attachment_dedup(
        self, doc_id: str, rev: str, att_name: str, meta: Dict
    ) -> Dict:
        content: Optional[bytes] = None
        digest, content = self.digest_or_compute(meta, content)
        if not digest:
            content = self.couch.fetch_attachment_bytes(doc_id, rev, att_name)
            digest, _ = self.digest_or_compute(meta, content)

        digest_key = f"{self.s3.base_prefix}_attachments/{urlquote(digest, safe='')}/data.zip"
        uploaded = False
        if not self.s3.key_exists(digest_key):
            if content is None:
                content = self.couch.fetch_attachment_bytes(doc_id, rev, att_name)
            zip_bytes = self.s3.to_zip_bytes("data", content)
            self.s3.put_bytes(digest_key, zip_bytes, "application/zip")
            uploaded = True

        return {
            "digest": digest,
            "s3_key": digest_key,
            "uploaded": uploaded,
            "content_type": meta.get("content_type"),
            "length": meta.get("length"),
            "revpos": meta.get("revpos"),
            "encoding": meta.get("encoding"),
            "encoded_length": meta.get("encoded_length"),
        }

    # ---- core loop ----
    def run(self, start_since: Optional[str]):
        # Resolve starting since
        if start_since:
            since = start_since
            if since == "now":
                # Get a baseline last_seq for this DB without processing backlog
                ch = self.couch.changes(since="now", timeout_s=1)
                since = ch.get("last_seq", "0")
                JSONLogger.log("start-from-now", db=self.db, resolved_since=since)
        else:
            since = self.s3.read_checkpoint(self.extra_checkpoint_key) or "0"
            JSONLogger.log(
                "start-from-s3-checkpoint" if since != "0" else "start-from-zero",
                db=self.db,
                since=since,
                canonical_key=self.s3.canonical_seq_key,
                extra_key=self.extra_checkpoint_key,
            )

        backoff = 1.0
        while not self.stop_event.is_set():
            try:
                ch = self.couch.changes(since=since, timeout_s=self.longpoll_timeout)
                results = ch.get("results", [])
                new_last_seq = ch.get("last_seq", since)

                if not results:
                    since = new_last_seq
                    self.s3.write_checkpoint(since, self.extra_checkpoint_key)
                    JSONLogger.log("no-changes", db=self.db, last_seq=since)
                    continue

                # Build list for _bulk_get and ignore deletes
                ids_revs = []
                for row in results:
                    if row.get("deleted"):
                        continue
                    revs = row.get("changes", [])
                    if revs:
                        ids_revs.append({"id": row["id"], "rev": revs[0]["rev"]})

                # Fetch docs in chunks
                docs: List[Dict] = []
                for i in range(0, len(ids_revs), self.bulk_chunk_size):
                    chunk = ids_revs[i : i + self.bulk_chunk_size]
                    docs.extend(self.couch.bulk_get_docs(chunk))

                # Upload each doc + attachments manifest
                for d in docs:
                    doc_id = d["_id"]
                    rev = d.get("_rev")

                    # 1) JSON blob
                    self.s3.upload_doc_json_zip(d)

                    # 2) Attachments (dedup by digest) + per-doc manifest
                    atts = d.get("_attachments") or {}
                    if atts:
                        manifest = {
                            "version": "couchbackup/dedupe-v1",
                            "created_at": utc_now(),
                            "db": self.db,
                            "doc_id": doc_id,
                            "rev": rev,
                            "attachments": {},
                        }
                        for name, meta in atts.items():
                            try:
                                info = self.upload_attachment_dedup(doc_id, rev, name, meta)
                                manifest["attachments"][name] = info
                            except Exception as e:
                                JSONLogger.log(
                                    "attachment-upload-failed",
                                    db=self.db,
                                    doc_id=doc_id,
                                    name=name,
                                    error=str(e),
                                )
                                # continue processing other attachments

                        man_key = f"{self.s3.base_prefix}{urlquote(doc_id, safe='')}/attachments.json"
                        self.s3.put_json(man_key, manifest)

                if docs:
                    JSONLogger.log("processed-docs", db=self.db, count=len(docs))

                # Advance checkpoint AFTER successful uploads
                since = new_last_seq
                self.s3.write_checkpoint(since, self.extra_checkpoint_key)
                JSONLogger.log(
                    "checkpoint-advanced",
                    db=self.db,
                    last_seq=since,
                    canonical_key=self.s3.canonical_seq_key,
                )
                backoff = 1.0

            except Exception as e:
                JSONLogger.log("error", db=self.db, error=str(e))
                time.sleep(backoff)
                backoff = min(30.0, backoff * 2)

        JSONLogger.log("exiting", db=self.db, last_seq=since)


# ---------- CLI / Runner ----------

def _parse_list(values: List[str]) -> List[str]:
    # supports: --db db1 --db db2,db3  and  --restore-doc id1,id2
    items: List[str] = []
    for v in values:
        items.extend([p for p in (s.strip() for s in v.split(",")) if p])
    # de-dupe while preserving order
    seen = set()
    ordered = []
    for d in items:
        if d not in seen:
            seen.add(d)
            ordered.append(d)
    return ordered


def parse_args():
    ap = argparse.ArgumentParser(
        description=(
            "Mirror CouchDB to S3 with deduped attachments and S3-only checkpoint at s3://<bucket>/<db>/last_seq. "
            "Optionally restore specific documents from S3 back to CouchDB."
        )
    )
    ap.add_argument("--couch-url", required=True, help="http://user:pass@host:5984")
    ap.add_argument(
        "--db",
        required=True,
        action="append",
        help="Database name. Repeat flag for multiple DBs or pass comma-separated (e.g. --db a,b,c).",
    )
    ap.add_argument(
        "--since",
        default=None,
        help="Starting since token or 'now'. If omitted, reads s3://<bucket>/<db>/last_seq or starts at '0'. Applied to all DBs.",
    )
    ap.add_argument(
        "--longpoll-timeout", type=int, default=60, help="Longpoll timeout seconds (default 60)"
    )
    ap.add_argument(
        "--bulk-chunk-size", type=int, default=500, help="Docs per _bulk_get call (default 500)"
    )
    ap.add_argument("--s3-bucket", required=True, help="Target S3 bucket")
    ap.add_argument(
        "--root-prefix",
        default="",
        help="Optional root prefix before <db>/... for docs & attachments (NOT used for last_seq)",
    )
    ap.add_argument(
        "--checkpoint-s3-key",
        default=None,
        help="Optional EXTRA S3 checkpoint key (in addition to <db>/last_seq)",
    )
    ap.add_argument("--aws-region", default=None, help="AWS region (optional)")

    # --- restore options ---
    ap.add_argument(
        "--restore-doc",
        action="append",
        help="Doc ID to restore from S3 to CouchDB (repeat or comma-separated). If provided, the program performs restore(s) and exits.",
    )
    ap.add_argument(
        "--overwrite",
        action="store_true",
        help="When restoring, overwrite doc if it already exists in CouchDB.",
    )
    return ap.parse_args()


def install_signal_handlers(stop_event: threading.Event):
    def _handler(signum, frame):
        JSONLogger.log("signal", signal=signum)
        stop_event.set()

    # Signals must be set in main thread
    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def _run_restore_flow(args):
    dbs = _parse_list(args.db)
    doc_ids = _parse_list(args.restore_doc)
    results = []
    for db in dbs:
        couch = CouchDBClient(base_url=args.couch_url, db=db)
        s3store = S3Storage(
            bucket=args.s3_bucket, root_prefix=args.root_prefix, db=db, region=args.aws_region
        )
        for doc_id in doc_ids:
            try:
                out = s3store.restore_doc_to_couch(couch, doc_id=doc_id, overwrite=args.overwrite)
                JSONLogger.log("restore-doc-complete", **out)
                results.append(out)
            except Exception as e:
                JSONLogger.log("restore-doc-failed", db=db, doc_id=doc_id, error=str(e))
    return results


def main():
    args = parse_args()

    # If restore requested, do that and exit
    if args.restore_doc:
        _run_restore_flow(args)
        return

    dbs = _parse_list(args.db)
    if not dbs:
        raise SystemExit("No databases provided via --db")

    stop_event = threading.Event()
    install_signal_handlers(stop_event)

    threads: List[threading.Thread] = []

    for db in dbs:
        couch = CouchDBClient(base_url=args.couch_url, db=db)
        s3store = S3Storage(
            bucket=args.s3_bucket,
            root_prefix=args.root_prefix,
            db=db,
            region=args.aws_region,
        )
        runner = CouchToS3Backup(
            couch=couch,
            s3store=s3store,
            db=db,
            longpoll_timeout=args.longpoll_timeout,
            bulk_chunk_size=args.bulk_chunk_size,
            extra_checkpoint_key=args.checkpoint_s3_key,
            stop_event=stop_event,
        )

        t = threading.Thread(target=runner.run, args=(args.since,), name=f"backup-{db}", daemon=True)
        t.start
        t.start()
        threads.append(t)
        JSONLogger.log("worker-started", db=db, thread=t.name)

    # Wait for workers to stop (on signal) and join
    try:
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(timeout=0.5)
            if stop_event.is_set():
                break
    finally:
        # Final join to clean up
        for t in threads:
            t.join(timeout=5.0)
        JSONLogger.log("all-workers-stopped", dbs=dbs)


if __name__ == "__main__":
    main()

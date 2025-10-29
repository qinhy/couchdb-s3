# couchdb-s3

Utilities for continuously backing up CouchDB databases to S3-compatible object stores,
deduplicating attachments, and restoring individual documents on demand. Everything is
powered by [fsspec](https://filesystem-spec.readthedocs.io/en/latest/) so the same code
works against AWS S3, MinIO, LocalStack, or any endpoint that speaks the S3 API.

## Highlights
- Long-poll CouchDB `_changes` feed and write document JSON as zipped blobs.
- Store attachments once by sha256 digest and keep per-document manifests.
- Record checkpoints directly in object storage for stateless workers.
- Restore selected documents (attachments included) back into CouchDB.
- Optional FastAPI UI/API (`backup_api.py`) to orchestrate workers.

## Quick start
```bash
pip install requests fsspec s3fs
python couchdb_to_s3.py --help
```

For the dashboard and API:
```bash
pip install fastapi uvicorn pydantic requests fsspec s3fs
uvicorn backup_api:app --host 0.0.0.0 --port 8000
```

Open <http://localhost:8000/> and authenticate with the credentials supplied via
`BASIC_USER` / `BASIC_PASS` environment variables. Use the Watchdogs panel to launch
backups and the Restore panel to rehydrate documents.

"""Persist acknowledgment documents to IPFS or S3.

The store_ack function serializes the supplied data to JSON, validates it
against the acknowledgment schema, and attempts to persist it to an IPFS
node or S3 bucket. The function returns a reference dictionary containing
information about where the data was stored. If neither backend is
configured, the data is written to a local directory as a fallback.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict

try:  # Optional dependency
    import jsonschema  # type: ignore
except Exception:  # pragma: no cover - optional
    jsonschema = None

try:  # Optional dependency
    import ipfshttpclient  # type: ignore
except Exception:  # pragma: no cover - optional
    ipfshttpclient = None

try:  # Optional dependency
    import boto3  # type: ignore
except Exception:  # pragma: no cover - optional
    boto3 = None

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "acknowledgment.json"


def _validate(data: Dict[str, Any]) -> None:
    """Validate *data* against the acknowledgment schema if jsonschema is available."""

    if jsonschema is None:  # pragma: no cover - validation skipped if unavailable
        return
    with SCHEMA_PATH.open() as fh:
        schema = json.load(fh)
    jsonschema.validate(data, schema)


def _store_ipfs(serialized: bytes) -> Dict[str, str] | None:
    """Store JSON bytes in IPFS and return a reference."""

    api_url = os.getenv("IPFS_API_URL", "/dns/localhost/tcp/5001/http")
    if ipfshttpclient is None:
        return None
    try:
        client = ipfshttpclient.connect(api_url)
        cid = client.add_bytes(serialized)
        return {"backend": "ipfs", "cid": cid}
    except Exception:  # pragma: no cover - network dependent
        return None


def _store_s3(serialized: bytes, digest: str) -> Dict[str, str] | None:
    """Store JSON bytes in S3 using the hash as the object key."""

    bucket = os.getenv("ACK_S3_BUCKET")
    if not bucket or boto3 is None:
        return None
    key = f"{digest}.json"
    try:
        s3 = boto3.client("s3")
        s3.put_object(Bucket=bucket, Key=key, Body=serialized, ContentType="application/json")
        return {"backend": "s3", "hash": digest}
    except Exception:  # pragma: no cover - network dependent
        return None


def _store_local(serialized: bytes, digest: str) -> Dict[str, str]:
    """Persist JSON bytes to a local directory as a last resort."""

    base = Path(os.getenv("ACK_STORE_DIR", "acks"))
    base.mkdir(parents=True, exist_ok=True)
    path = base / f"{digest}.json"
    with path.open("wb") as fh:
        fh.write(serialized)
    return {"backend": "local", "hash": digest}


def store_ack(data: Dict[str, Any]) -> Dict[str, str]:
    """Serialize *data* and store it in the configured backend.

    The function returns a dictionary containing at minimum the ``backend`` key
    and a ``cid`` or ``hash`` key that uniquely identifies the stored
    acknowledgment document.
    """

    _validate(data)
    serialized = json.dumps(data, sort_keys=True).encode("utf-8")
    digest = hashlib.sha256(serialized).hexdigest()

    ref = _store_ipfs(serialized)
    if ref:
        return ref

    ref = _store_s3(serialized, digest)
    if ref:
        return ref

    # Fallback to local storage
    return _store_local(serialized, digest)

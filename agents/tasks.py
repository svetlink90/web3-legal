import json
import os
from typing import Any, Dict

import hashlib

from celery import Celery

from .analysis import analyze_screening

BROKER = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
app = Celery("agents.tasks", broker=BROKER, backend=BACKEND)


@app.task(name="agents.tasks.collect_address")
def collect_address(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {"address": payload.get("address"), "owner_ack": bool(payload.get("ack"))}


@app.task(name="agents.tasks.sanction_screen")
def sanction_screen(prev: Dict[str, Any]) -> Dict[str, Any]:
    address = prev.get("address")
    prev["screening"] = {"address": address, "hit": False, "sources": []}
    return prev


@app.task(name="agents.tasks.analyze_results")
def analyze_results(prev: Dict[str, Any]) -> Dict[str, Any]:
    scr = prev.get("screening", {})
    recommendation = analyze_screening(scr)
    prev["analysis"] = {"decision": recommendation.value}
    return prev


@app.task(name="agents.tasks.store_ack")
def store_ack(prev: Dict[str, Any]) -> Dict[str, Any]:
    prev["stored"] = True
    return prev


@app.task(name="agents.tasks.anchor_log")
def anchor_log(prev: Dict[str, Any]) -> Dict[str, Any]:
    data = json.dumps(prev, sort_keys=True).encode()
    data_hash = hashlib.sha3_256(data).hexdigest()
    prev["anchor_ref"] = data_hash
    prev["certificate"] = {
        "address": prev.get("address"),
        "acknowledged": prev.get("owner_ack", False),
        "data_hash": data_hash,
    }
    return prev


def _state_store():
    import redis

    url = os.getenv("STATE_BACKEND_URL", "redis://localhost:6379/1")
    return redis.Redis.from_url(url)


@app.task(name="agents.tasks.mark_workflow_done")
def mark_workflow_done(prev: Dict[str, Any], workflow_id: str) -> Dict[str, Any]:
    """Persist the final workflow state as SUCCESS."""

    r = _state_store()
    state = {"status": "SUCCESS", "task_type": "workflow", "result": prev}
    r.set(workflow_id, json.dumps(state))
    return prev

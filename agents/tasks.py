import json
import os
from typing import Any, Dict

from celery import Celery

try:
    from storage import store_ack as persist_ack
except Exception:  # pragma: no cover - storage optional during tests
    persist_ack = None

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
    prev["analysis"] = {
        "decision": "pass" if not scr.get("hit") else "block",
        "rationale": "No matches in sources" if not scr.get("hit") else "Positive match",
    }
    return prev


@app.task(name="agents.tasks.store_ack")
def store_ack(prev: Dict[str, Any]) -> Dict[str, Any]:
    """Persist the acknowledgment details and return a reference."""

    if persist_ack is not None:
        ref = persist_ack(prev)
        prev["ack_ref"] = ref
    else:  # pragma: no cover - fallback if storage module missing
        prev["ack_ref"] = {"backend": "unknown"}
    return prev


@app.task(name="agents.tasks.anchor_log")
def anchor_log(prev: Dict[str, Any]) -> Dict[str, Any]:
    prev["anchor_ref"] = "local-simulated-ref"
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

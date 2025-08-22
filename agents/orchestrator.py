"""Orchestration of compliance tasks.

This module provides a simple orchestrator that queues tasks for subordinate
agents using Celery and persists orchestration state in Redis or PostgreSQL
for fault tolerance.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

from celery import Celery, chain

try:  # Optional dependencies are imported lazily so pytest collection does not fail.
    import redis  # type: ignore
except Exception:  # pragma: no cover - redis is optional
    redis = None

try:  # pragma: no cover - psycopg2 might not be available
    import psycopg2  # type: ignore
    from psycopg2.extras import Json  # type: ignore
except Exception:  # pragma: no cover - dependency is optional
    psycopg2 = None


class TaskType(str, Enum):
    """Enumerates all supported task types for the orchestrator."""

    COLLECT_ADDRESS = "collect_address"
    SANCTION_SCREEN = "sanction_screen"
    ANALYZE_RESULTS = "analyze_results"
    STORE_ACK = "store_ack"
    ANCHOR_LOG = "anchor_log"


class RedisStateStore:
    """Simple key/value persistence using Redis."""

    def __init__(self, url: str) -> None:
        if redis is None:  # pragma: no cover - handled at runtime
            raise RuntimeError("redis library is not installed")
        self._client = redis.Redis.from_url(url)

    def set(self, key: str, value: Dict[str, Any]) -> None:
        self._client.set(key, json.dumps(value))

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        data = self._client.get(key)
        return json.loads(data) if data else None


class PostgresStateStore:
    """Stores orchestration state in PostgreSQL."""

    def __init__(self, dsn: str) -> None:
        if psycopg2 is None:  # pragma: no cover - handled at runtime
            raise RuntimeError("psycopg2 is not installed")
        self._dsn = dsn
        self._ensure_table()

    def _ensure_table(self) -> None:
        with psycopg2.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS orchestrator_state (
                        id TEXT PRIMARY KEY,
                        data JSONB
                    )
                    """
                )
                conn.commit()

    def set(self, key: str, value: Dict[str, Any]) -> None:
        with psycopg2.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO orchestrator_state (id, data)
                    VALUES (%s, %s)
                    ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data
                    """,
                    (key, Json(value)),
                )
                conn.commit()

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        with psycopg2.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT data FROM orchestrator_state WHERE id = %s",
                    (key,),
                )
                row = cur.fetchone()
                return row[0] if row else None


@dataclass
class Orchestrator:
    """Dispatches tasks and records orchestration state."""

    broker_url: str = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
    backend_url: str = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
    state_backend_url: str = os.getenv("STATE_BACKEND_URL", "redis://localhost:6379/1")
    use_postgres: bool = bool(os.getenv("STATE_USE_POSTGRES"))
    celery_app: Celery = field(init=False)
    state_store: Any = field(init=False)

    def __post_init__(self) -> None:
        self.celery_app = Celery(
            "orchestrator", broker=self.broker_url, backend=self.backend_url
        )
        if self.use_postgres:
            self.state_store = PostgresStateStore(self.state_backend_url)
        else:
            self.state_store = RedisStateStore(self.state_backend_url)

    def schedule(self, task_type: TaskType, payload: Dict[str, Any]) -> str:
        """Queue a task for asynchronous execution."""

        task_id = str(uuid.uuid4())
        self.state_store.set(
            task_id,
            {"status": "queued", "task_type": task_type.value, "payload": payload},
        )
        self.celery_app.send_task(
            f"agents.tasks.{task_type.value}", args=[payload, task_id]
        )
        return task_id

    def update_status(
        self, task_id: str, status: str, result: Optional[Dict[str, Any]] = None
    ) -> None:
        """Update state for a task."""

        state = self.state_store.get(task_id) or {}
        state.update({"status": status})
        if result is not None:
            state["result"] = result
        self.state_store.set(task_id, state)

    def get_state(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Return stored state for a task."""

        return self.state_store.get(task_id)

    def run_workflow(self, payload: Dict[str, Any]) -> str:
        """Execute the full compliance workflow as a Celery chain."""

        workflow_id = str(uuid.uuid4())
        steps = [
            self.celery_app.signature(
                f"agents.tasks.{TaskType.COLLECT_ADDRESS.value}", args=[payload]
            ),
            self.celery_app.signature(
                f"agents.tasks.{TaskType.SANCTION_SCREEN.value}"
            ),
            self.celery_app.signature(
                f"agents.tasks.{TaskType.ANALYZE_RESULTS.value}"
            ),
            self.celery_app.signature(
                f"agents.tasks.{TaskType.STORE_ACK.value}"
            ),
            self.celery_app.signature(
                f"agents.tasks.{TaskType.ANCHOR_LOG.value}"
            ),
            self.celery_app.signature(
                "agents.tasks.mark_workflow_done", kwargs={"workflow_id": workflow_id}
            ),
        ]
        chain(*steps).apply_async()
        # initial state stored immediately so the workflow ID can be polled
        self.state_store.set(workflow_id, {"status": "queued", "task_type": "workflow"})
        return workflow_id

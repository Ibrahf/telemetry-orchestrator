"""SQLite persistence for batch results and the pending-review queue."""
import json
import os
import sqlite3
import time
from dataclasses import asdict

from . import config
from .models import BatchOutcome

SCHEMA = """
CREATE TABLE IF NOT EXISTS batch_results (
    batch_id       TEXT PRIMARY KEY,
    status         TEXT NOT NULL,
    valid_count    INTEGER NOT NULL,
    rejected_count INTEGER NOT NULL,
    anomaly_count  INTEGER NOT NULL,
    payload        TEXT NOT NULL,
    stored_at      REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS pending_reviews (
    workflow_id    TEXT PRIMARY KEY,
    batch_id       TEXT NOT NULL,
    machine_id     TEXT NOT NULL,
    anomaly_count  INTEGER NOT NULL,
    created_at     REAL NOT NULL
);
"""


def connect(db_path: str = config.DB_PATH) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def save_outcome(outcome: BatchOutcome, db_path: str = config.DB_PATH) -> None:
    # INSERT OR REPLACE keeps this idempotent: a retried activity overwrites
    # the same row instead of creating a duplicate.
    with connect(db_path) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO batch_results VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                outcome.batch_id, outcome.status, outcome.valid_count,
                outcome.rejected_count, outcome.anomaly_count,
                json.dumps(asdict(outcome)), time.time(),
            ),
        )


def add_pending_review(workflow_id: str, batch_id: str, machine_id: str,
                       anomaly_count: int, db_path: str = config.DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO pending_reviews VALUES (?, ?, ?, ?, ?)",
            (workflow_id, batch_id, machine_id, anomaly_count, time.time()),
        )


def remove_pending_review(workflow_id: str, db_path: str = config.DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute("DELETE FROM pending_reviews WHERE workflow_id = ?", (workflow_id,))


def list_pending_reviews(db_path: str = config.DB_PATH) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM pending_reviews ORDER BY created_at").fetchall()
    return [dict(r) for r in rows]


def list_results(limit: int = 50, db_path: str = config.DB_PATH) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT batch_id, status, valid_count, rejected_count, anomaly_count, stored_at "
            "FROM batch_results ORDER BY stored_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]

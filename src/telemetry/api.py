"""Operator API: see what needs review, check workflow status, approve or reject.

Run:  uvicorn telemetry.api:app --reload --port 8000
Docs: http://localhost:8000/docs
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from temporalio.client import Client
from temporalio.service import RPCError

from . import config, storage
from .models import ReviewDecision
from .workflows import TelemetryBatchWorkflow

temporal: Client | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global temporal
    temporal = await Client.connect(config.TEMPORAL_ADDRESS)
    yield


app = FastAPI(title="Telemetry Orchestrator", lifespan=lifespan)


class ReviewRequest(BaseModel):
    approved: bool
    reviewer: str
    note: str = ""


@app.get("/reviews")
def pending_reviews() -> list[dict]:
    return storage.list_pending_reviews()


@app.get("/results")
def recent_results(limit: int = 50) -> list[dict]:
    return storage.list_results(limit)


@app.get("/workflows/{workflow_id}/status")
async def workflow_status(workflow_id: str) -> dict:
    handle = temporal.get_workflow_handle(workflow_id)
    try:
        status = await handle.query(TelemetryBatchWorkflow.status)
    except RPCError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"workflow_id": workflow_id, "status": status}


@app.post("/reviews/{workflow_id}")
async def submit_review(workflow_id: str, body: ReviewRequest) -> dict:
    handle = temporal.get_workflow_handle(workflow_id)
    decision = ReviewDecision(approved=body.approved, reviewer=body.reviewer, note=body.note)
    try:
        await handle.signal(TelemetryBatchWorkflow.submit_review, decision)
    except RPCError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"workflow_id": workflow_id, "submitted": True}

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
from .workflows import ANOMALY_COUNT, MACHINE_ID, TelemetryBatchWorkflow

# Running workflows that have flagged themselves for review. Temporal's
# visibility store updates within about a second of the workflow changing.
PENDING_REVIEW_QUERY = (
    "WorkflowType = 'TelemetryBatchWorkflow' "
    "AND ExecutionStatus = 'Running' "
    "AND ReviewStatus = 'pending'"
)

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
async def pending_reviews() -> list[dict]:
    reviews = []
    async for wf in temporal.list_workflows(PENDING_REVIEW_QUERY):
        attrs = wf.typed_search_attributes
        reviews.append({
            "workflow_id": wf.id,
            "machine_id": attrs.get(MACHINE_ID),
            "anomaly_count": attrs.get(ANOMALY_COUNT),
            "started_at": wf.start_time.isoformat(),
        })
    return sorted(reviews, key=lambda r: r["started_at"])


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
    try:
        status = await handle.query(TelemetryBatchWorkflow.status)
    except RPCError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    if status != "awaiting_review":
        # The workflow would ignore the signal; tell the caller instead.
        raise HTTPException(status_code=409, detail=f"Workflow is not awaiting review (status: {status})")

    decision = ReviewDecision(approved=body.approved, reviewer=body.reviewer, note=body.note)
    await handle.signal(TelemetryBatchWorkflow.submit_review, decision)
    return {"workflow_id": workflow_id, "submitted": True}

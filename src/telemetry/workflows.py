"""Workflow definitions.

Workflow code must be deterministic: no I/O, randomness, or system clock.
All of that lives in activities. Temporal records every step in the
workflow's event history, which is what the Web UI shows.
"""
import asyncio
from datetime import timedelta
from typing import Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from . import config
    from .activities import process_readings, request_review, store_outcome, validate_batch
    from .models import Batch, BatchOutcome, ReviewDecision

# Retry transient failures with exponential back-off, then give up.
DEFAULT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=6,
)
STEP_TIMEOUT = timedelta(seconds=30)


@workflow.defn
class TelemetryBatchWorkflow:
    """Validate -> process -> (human review if anomalies) -> store."""

    def __init__(self) -> None:
        self._status = "started"
        self._decision: Optional[ReviewDecision] = None

    @workflow.run
    async def run(self, batch: Batch) -> BatchOutcome:
        self._status = "validating"
        validation = await workflow.execute_activity(
            validate_batch, batch,
            start_to_close_timeout=STEP_TIMEOUT, retry_policy=DEFAULT_RETRY,
        )

        self._status = "processing"
        processed = await workflow.execute_activity(
            process_readings, validation.valid,
            start_to_close_timeout=STEP_TIMEOUT, retry_policy=DEFAULT_RETRY,
        )

        outcome = BatchOutcome(
            batch_id=batch.batch_id,
            status="stored",
            valid_count=len(validation.valid),
            rejected_count=len(validation.rejected),
            anomaly_count=len(processed.anomalies),
            summaries=processed.summaries,
        )

        if processed.anomalies:
            self._status = "awaiting_review"
            await workflow.execute_activity(
                request_review,
                args=[workflow.info().workflow_id, batch.batch_id,
                      batch.machine_id, len(processed.anomalies)],
                start_to_close_timeout=STEP_TIMEOUT, retry_policy=DEFAULT_RETRY,
            )
            try:
                # Durable wait: survives worker restarts and costs nothing while idle.
                await workflow.wait_condition(
                    lambda: self._decision is not None,
                    timeout=timedelta(minutes=config.REVIEW_TIMEOUT_MINUTES),
                )
            except asyncio.TimeoutError:
                self._decision = ReviewDecision(
                    approved=False, reviewer="system", note="review timed out"
                )
            outcome.review = self._decision
            outcome.status = "approved" if self._decision.approved else "rejected"

        self._status = "storing"
        await workflow.execute_activity(
            store_outcome, args=[workflow.info().workflow_id, outcome],
            start_to_close_timeout=STEP_TIMEOUT, retry_policy=DEFAULT_RETRY,
        )

        self._status = f"completed:{outcome.status}"
        return outcome

    @workflow.signal
    def submit_review(self, decision: ReviewDecision) -> None:
        """Sent by an operator (through the API) to approve or reject a batch."""
        if self._decision is None:
            self._decision = decision

    @workflow.query
    def status(self) -> str:
        """Current step, readable at any time without affecting execution."""
        return self._status

"""Activities: the individual steps a workflow runs.

Activities hold all side effects and non-deterministic work (I/O, randomness,
clocks). Temporal retries them according to the workflow's retry policy.
"""
import math
import random
import time
from collections import defaultdict

from temporalio import activity

from . import config, storage
from .models import (
    METRIC_LIMITS, Batch, BatchOutcome, MetricSummary, ProcessResult,
    Reading, ValidationResult,
)


@activity.defn
def validate_batch(batch: Batch) -> ValidationResult:
    """Separate readings into valid and rejected, recording why each was rejected."""
    valid, rejected, reasons = [], [], []
    now = time.time()
    for r in batch.readings:
        limits = METRIC_LIMITS.get(r.metric)
        if limits is None:
            reason = f"unknown metric '{r.metric}'"
        elif r.value is None or not math.isfinite(r.value):
            reason = f"{r.metric}: missing or non-numeric value"
        elif not limits["min"] <= r.value <= limits["max"]:
            reason = f"{r.metric}: {r.value} outside physical range"
        elif r.timestamp > now + 60:
            reason = f"{r.metric}: timestamp in the future"
        else:
            valid.append(r)
            continue
        rejected.append(r)
        reasons.append(reason)

    activity.logger.info(
        "Validated batch %s: %d valid, %d rejected", batch.batch_id, len(valid), len(rejected)
    )
    return ValidationResult(valid=valid, rejected=rejected, reasons=reasons)


@activity.defn
def process_readings(readings: list[Reading]) -> ProcessResult:
    """Summarize each metric and flag readings above the alarm threshold."""
    by_metric: dict[str, list[float]] = defaultdict(list)
    anomalies = []
    for r in readings:
        by_metric[r.metric].append(r.value)
        if r.value >= METRIC_LIMITS[r.metric]["alarm"]:
            anomalies.append(r)

    summaries = [
        MetricSummary(
            metric=m, count=len(v), minimum=min(v), maximum=max(v),
            mean=round(sum(v) / len(v), 3),
        )
        for m, v in sorted(by_metric.items())
    ]
    return ProcessResult(summaries=summaries, anomalies=anomalies)


@activity.defn
def store_outcome(outcome: BatchOutcome) -> None:
    """Persist the final result. Can be made to fail on purpose to demonstrate retries."""
    if random.random() < config.STORE_FAILURE_RATE:
        raise RuntimeError(
            f"Simulated database outage (attempt {activity.info().attempt})"
        )
    storage.save_outcome(outcome)
    activity.logger.info("Stored batch %s with status %s", outcome.batch_id, outcome.status)


ALL_ACTIVITIES = [validate_batch, process_readings, store_outcome]

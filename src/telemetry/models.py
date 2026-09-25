"""Data passed between the listener, workflows, and activities.

Temporal serializes these dataclasses to JSON automatically, so keep them
to plain types (str, float, int, bool, lists, and other dataclasses).
"""
from dataclasses import dataclass, field
from typing import Optional

# Valid physical range and anomaly threshold for each metric.
METRIC_LIMITS = {
    "temperature_c": {"min": -40.0, "max": 150.0, "alarm": 90.0},
    "vibration_mm_s": {"min": 0.0, "max": 50.0, "alarm": 20.0},
    "pressure_kpa": {"min": 0.0, "max": 1000.0, "alarm": 800.0},
}


@dataclass
class Reading:
    machine_id: str
    metric: str
    value: Optional[float]
    timestamp: float  # Unix epoch seconds


@dataclass
class Batch:
    batch_id: str
    machine_id: str
    readings: list[Reading]


@dataclass
class ValidationResult:
    valid: list[Reading]
    rejected: list[Reading]
    reasons: list[str]


@dataclass
class MetricSummary:
    metric: str
    count: int
    minimum: float
    maximum: float
    mean: float


@dataclass
class ProcessResult:
    summaries: list[MetricSummary]
    anomalies: list[Reading]


@dataclass
class ReviewDecision:
    approved: bool
    reviewer: str
    note: str = ""


@dataclass
class BatchOutcome:
    batch_id: str
    status: str  # "stored", "approved", "rejected"
    valid_count: int
    rejected_count: int
    anomaly_count: int
    review: Optional[ReviewDecision] = None
    summaries: list[MetricSummary] = field(default_factory=list)

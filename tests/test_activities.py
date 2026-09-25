import time

from temporalio.testing import ActivityEnvironment

from telemetry.activities import process_readings, validate_batch
from telemetry.models import Batch, Reading

env = ActivityEnvironment()


def reading(metric: str, value, ts: float | None = None) -> Reading:
    return Reading(machine_id="m1", metric=metric, value=value, timestamp=ts or time.time())


def test_validate_rejects_bad_readings():
    batch = Batch(batch_id="b1", machine_id="m1", readings=[
        reading("temperature_c", 50.0),
        reading("temperature_c", None),
        reading("temperature_c", -999.0),
        reading("humidity", 40.0),
        reading("vibration_mm_s", 5.0, ts=time.time() + 3600),
    ])
    result = env.run(validate_batch, batch)
    assert len(result.valid) == 1
    assert len(result.rejected) == 4
    assert len(result.reasons) == 4


def test_process_flags_anomalies_and_summarizes():
    readings = [
        reading("temperature_c", 50.0),
        reading("temperature_c", 100.0),
        reading("vibration_mm_s", 4.0),
    ]
    result = env.run(process_readings, readings)
    assert [a.value for a in result.anomalies] == [100.0]
    temp = next(s for s in result.summaries if s.metric == "temperature_c")
    assert temp.count == 2 and temp.mean == 75.0

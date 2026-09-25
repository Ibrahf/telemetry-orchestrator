"""Event bridge: MQTT messages in, Temporal workflows out.

Buffers readings per machine and starts one TelemetryBatchWorkflow each
time a machine accumulates BATCH_SIZE readings.

Run:  python -m telemetry.listener
"""
import asyncio
import json
import logging
import uuid
from collections import defaultdict

import paho.mqtt.client as mqtt
from temporalio.client import Client

from . import config
from .models import Batch, Reading
from .workflows import TelemetryBatchWorkflow

log = logging.getLogger("listener")


def parse_reading(payload: bytes) -> Reading:
    data = json.loads(payload)
    return Reading(
        machine_id=data["machine_id"],
        metric=data["metric"],
        value=data.get("value"),
        timestamp=float(data["timestamp"]),
    )


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    temporal = await Client.connect(config.TEMPORAL_ADDRESS)
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[bytes] = asyncio.Queue()

    # paho runs callbacks on its own network thread; hand messages to asyncio safely.
    def on_message(_client, _userdata, msg):
        loop.call_soon_threadsafe(queue.put_nowait, msg.payload)

    def on_connect(client, _userdata, _flags, reason_code, _properties):
        log.info("Connected to MQTT (%s); subscribing to %s", reason_code, config.MQTT_TOPIC)
        client.subscribe(config.MQTT_TOPIC, qos=1)

    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="telemetry-listener")
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.connect(config.MQTT_HOST, config.MQTT_PORT)
    mqttc.loop_start()

    buffers: dict[str, list[Reading]] = defaultdict(list)
    try:
        while True:
            payload = await queue.get()
            try:
                reading = parse_reading(payload)
            except (ValueError, KeyError, TypeError) as exc:
                log.warning("Dropping malformed message: %s", exc)
                continue

            buf = buffers[reading.machine_id]
            buf.append(reading)
            if len(buf) < config.BATCH_SIZE:
                continue

            batch = Batch(batch_id=uuid.uuid4().hex[:12], machine_id=reading.machine_id, readings=list(buf))
            buf.clear()
            workflow_id = f"telemetry-{batch.machine_id}-{batch.batch_id}"
            await temporal.start_workflow(
                TelemetryBatchWorkflow.run, batch, id=workflow_id, task_queue=config.TASK_QUEUE,
            )
            log.info("Started workflow %s (%d readings)", workflow_id, len(batch.readings))
    finally:
        mqttc.loop_stop()
        mqttc.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

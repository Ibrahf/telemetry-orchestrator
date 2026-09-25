"""Simulated machines publishing telemetry to the MQTT broker.

Mostly normal readings, with occasional anomalies (above alarm threshold)
and bad data (missing or out-of-range values) so every pipeline path runs.

Run:  python -m telemetry.simulator --machines 3 --interval 0.5
"""
import argparse
import json
import random
import time

import paho.mqtt.client as mqtt

from . import config

NORMAL = {
    "temperature_c": (55.0, 8.0),
    "vibration_mm_s": (6.0, 2.0),
    "pressure_kpa": (400.0, 50.0),
}
ANOMALY = {"temperature_c": 105.0, "vibration_mm_s": 28.0, "pressure_kpa": 880.0}


def make_reading(machine_id: str, anomaly_rate: float, bad_rate: float) -> dict:
    metric = random.choice(list(NORMAL))
    roll = random.random()
    if roll < bad_rate:
        value = random.choice([None, -999.0])
    elif roll < bad_rate + anomaly_rate:
        value = round(ANOMALY[metric] + random.uniform(0, 10), 2)
    else:
        mean, spread = NORMAL[metric]
        value = round(random.gauss(mean, spread), 2)
    return {"machine_id": machine_id, "metric": metric, "value": value, "timestamp": time.time()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--machines", type=int, default=3)
    parser.add_argument("--interval", type=float, default=0.5, help="seconds between publishes")
    parser.add_argument("--anomaly-rate", type=float, default=0.03)
    parser.add_argument("--bad-rate", type=float, default=0.05)
    args = parser.parse_args()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="telemetry-simulator")
    client.connect(config.MQTT_HOST, config.MQTT_PORT)
    client.loop_start()

    machines = [f"machine-{i + 1:02d}" for i in range(args.machines)]
    print(f"Publishing for {machines} to {config.MQTT_HOST}:{config.MQTT_PORT} (Ctrl+C to stop)")
    try:
        while True:
            machine_id = random.choice(machines)
            reading = make_reading(machine_id, args.anomaly_rate, args.bad_rate)
            topic = f"spBv1.0/plant1/DDATA/edge-01/{machine_id}"
            client.publish(topic, json.dumps(reading), qos=1)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()

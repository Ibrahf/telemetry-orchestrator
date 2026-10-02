"""Central configuration, overridable through environment variables."""
import os

from dotenv import load_dotenv

# Load secrets from .env (git-ignored). Real environment variables take precedence.
load_dotenv()

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
TASK_QUEUE = os.getenv("TASK_QUEUE", "telemetry-pipeline")

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
# Sparkplug-style topic namespace: spBv1.0/<group>/DDATA/<edge node>/<device>
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "spBv1.0/plant1/DDATA/+/+")

# Each component has its own broker account; see infra/mosquitto.acl for permissions.
SIMULATOR_MQTT_USER = os.getenv("SIMULATOR_MQTT_USER", "simulator")
SIMULATOR_MQTT_PASSWORD = os.getenv("SIMULATOR_MQTT_PASSWORD", "")
LISTENER_MQTT_USER = os.getenv("LISTENER_MQTT_USER", "listener")
LISTENER_MQTT_PASSWORD = os.getenv("LISTENER_MQTT_PASSWORD", "")

# Listener batching: start a workflow once a machine has this many readings.
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))

DB_PATH = os.getenv("DB_PATH", "data/telemetry.db")

# Probability (0.0-1.0) that the store activity fails, to demonstrate retries.
STORE_FAILURE_RATE = float(os.getenv("STORE_FAILURE_RATE", "0.0"))

# How long a batch waits for a human review before it is auto-rejected.
REVIEW_TIMEOUT_MINUTES = int(os.getenv("REVIEW_TIMEOUT_MINUTES", "60"))

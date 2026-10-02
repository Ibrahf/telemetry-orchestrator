"""Authenticated MQTT connection shared by the simulator and the listener."""
import threading

import paho.mqtt.client as mqtt

from . import config


class MQTTConnectionError(RuntimeError):
    pass


def connect(client_id: str, username: str, password: str, *,
            subscribe: str | None = None, on_message=None,
            timeout: float = 10.0) -> mqtt.Client:
    """Log in to the broker and wait for its answer.

    Raises MQTTConnectionError if the broker refuses the login (for example
    a wrong password) or does not answer, instead of retrying silently.
    If `subscribe` is given, the subscription is renewed on every reconnect.
    """
    if not password:
        raise MQTTConnectionError(
            f"No MQTT password configured for '{username}'. Copy .env.example to .env and set it."
        )

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    client.username_pw_set(username, password)
    connected = threading.Event()
    result = {}

    def on_connect(c, _userdata, _flags, reason_code, _properties):
        result["reason"] = reason_code
        connected.set()
        if not reason_code.is_failure and subscribe:
            c.subscribe(subscribe, qos=1)

    client.on_connect = on_connect
    if on_message is not None:
        client.on_message = on_message

    client.connect(config.MQTT_HOST, config.MQTT_PORT)
    client.loop_start()

    if not connected.wait(timeout):
        client.loop_stop()
        raise MQTTConnectionError(
            f"No response from MQTT broker at {config.MQTT_HOST}:{config.MQTT_PORT}"
        )
    if result["reason"].is_failure:
        client.loop_stop()
        client.disconnect()
        raise MQTTConnectionError(f"MQTT broker refused login for '{username}': {result['reason']}")
    return client

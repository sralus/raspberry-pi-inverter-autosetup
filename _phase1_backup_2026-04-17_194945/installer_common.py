\
from pathlib import Path
from common.detect_ports import choose_port_interactive
from common.prompts import ask_string, ask_int, ask_yes_no, ask_secret
from common.validators import validate_device_name, validate_ui_dir


def collect_common_answers(default_name: str, default_ui_dir: str):
    device_path = choose_port_interactive()
    device_name = validate_device_name(ask_string("Gerätename", default_name))

    mqtt_enabled = ask_yes_no("MQTT verwenden?", True)
    mqtt_host = ""
    mqtt_port = 1883
    mqtt_username = ""
    mqtt_password = ""

    if mqtt_enabled:
        mqtt_host = ask_string("MQTT Broker Host", "192.168.0.69")
        mqtt_port = ask_int("MQTT Broker Port", 1883)
        mqtt_username = ask_string("MQTT Username", "")
        mqtt_password = ask_secret("MQTT Passwort")

    poll_interval = ask_int("Poll-Intervall", 15)
    ui_dir = validate_ui_dir(ask_string("UI-Zielordner", default_ui_dir))

    return {
        "device_path": device_path,
        "device_name": device_name,
        "mqtt_enabled": mqtt_enabled,
        "mqtt_host": mqtt_host,
        "mqtt_port": mqtt_port,
        "mqtt_username": mqtt_username,
        "mqtt_password": mqtt_password,
        "poll_interval": poll_interval,
        "ui_dir": ui_dir,
    }

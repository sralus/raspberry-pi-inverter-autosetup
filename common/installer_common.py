#!/usr/bin/env python3
from pathlib import Path
from common.detect_ports import choose_port_interactive
from common.prompts import ask_string, ask_int, ask_yes_no, ask_secret, ask_choice
from common.validators import (
    validate_device_name,
    validate_ui_dir,
    validate_port,
    validate_poll_interval,
    slugify_service_base,
)


def collect_common_answers(
    *,
    default_name: str,
    default_ui_dir: str,
    default_ui_port: int,
) -> dict:
    device_path = choose_port_interactive()
    device_name = validate_device_name(ask_string("Gerätename", default_name))

    mqtt_enabled = ask_yes_no("MQTT verwenden?", True)
    mqtt_host = ""
    mqtt_port = 1883
    mqtt_username = ""
    mqtt_password = ""

    if mqtt_enabled:
        mqtt_host = ask_string("MQTT Broker Host", "192.168.0.69")
        mqtt_port = validate_port(ask_int("MQTT Broker Port", 1883))
        mqtt_username = ask_string("MQTT Username", "")
        mqtt_password = ask_secret("MQTT Passwort")

    poll_interval = validate_poll_interval(ask_int("Poll-Intervall", 15))
    ui_dir = validate_ui_dir(ask_string("UI-Zielordner", default_ui_dir))

    ui_mode_choice = ask_choice(
        "UI-Modus auswählen:",
        {
            0: "built-in (empfohlen)",
            1: "external (optional)",
        },
        0,
    )
    ui_mode = "built-in" if ui_mode_choice == 0 else "external"

    ui_port = default_ui_port
    if ui_mode == "built-in":
        ui_port = validate_port(ask_int("UI-Port", default_ui_port))

    service_slug = slugify_service_base(device_name)

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
        "ui_mode": ui_mode,
        "ui_port": ui_port,
        "service_slug": service_slug,
    }


def build_common_install_paths(
    *,
    install_root: Path,
    ui_dir: Path,
    device_name: str,
) -> dict:
    return {
        "install_root": str(install_root),
        "runtime_dir": str(install_root / "runtime"),
        "tools_dir": str(install_root / "tools"),
        "latest_json": f"/home/pi/wr-logs/{device_name}_latest.json",
        "state_json": str(ui_dir / f"{device_name.lower()}_state.json"),
        "config_path": str(install_root / "config.json"),
        "build_dir": str(install_root / "build"),
    }

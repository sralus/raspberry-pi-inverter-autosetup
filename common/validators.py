#!/usr/bin/env python3
from pathlib import Path
import re


def validate_device_name(name: str) -> str:
    cleaned = name.strip()
    if not cleaned:
        raise ValueError("Gerätename darf nicht leer sein.")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", cleaned):
        raise ValueError("Gerätename darf nur Buchstaben, Zahlen, Punkt, Unterstrich und Bindestrich enthalten.")
    return cleaned


def validate_ui_dir(path: str) -> str:
    cleaned = path.strip()
    if not cleaned:
        raise ValueError("UI-Zielordner darf nicht leer sein.")
    if not cleaned.startswith("/"):
        raise ValueError("UI-Zielordner muss absolut sein.")
    return str(Path(cleaned))


def validate_port(port: int) -> int:
    if not (1 <= int(port) <= 65535):
        raise ValueError("Port muss zwischen 1 und 65535 liegen.")
    return int(port)


def validate_poll_interval(interval: int) -> int:
    if int(interval) < 1:
        raise ValueError("Poll-Intervall muss mindestens 1 Sekunde sein.")
    return int(interval)


def slugify_service_base(name: str) -> str:
    cleaned = name.strip().lower()
    cleaned = re.sub(r"[^a-z0-9._-]+", "-", cleaned)
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-")
    return cleaned or "reader"

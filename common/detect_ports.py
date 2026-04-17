#!/usr/bin/env python3
from pathlib import Path


def list_candidate_ports() -> list[str]:
    preferred: list[str] = []
    normal: list[str] = []

    for fixed in ["/dev/WR1", "/dev/WR2"]:
        if Path(fixed).exists() and fixed not in preferred:
            preferred.append(fixed)

    patterns = [
        "/dev/serial/by-id/*",
        "/dev/ttyUSB*",
        "/dev/ttyACM*",
        "/dev/hidraw*",
    ]

    for pattern in patterns:
        for p in sorted(Path("/").glob(pattern.lstrip("/"))):
            s = str(p)
            if s in preferred or s in normal:
                continue
            normal.append(s)

    return preferred + normal


def choose_port_interactive(default_index: int = 1) -> str:
    ports = list_candidate_ports()
    if not ports:
        raise RuntimeError("Keine Ports gefunden.")

    print("Gefundene Ports:")
    for idx, port in enumerate(ports, start=1):
        marker = "  (bevorzugt)" if port in {"/dev/WR1", "/dev/WR2"} else ""
        print(f"{idx}) {port}{marker}")

    while True:
        raw = input(f"Bitte Port auswählen [{default_index}]: ").strip() or str(default_index)
        try:
            idx = int(raw)
            if 1 <= idx <= len(ports):
                return ports[idx - 1]
        except ValueError:
            pass
        print("Ungültige Auswahl.")

#!/usr/bin/env python3
from pathlib import Path


def render_template(template_text: str, values: dict[str, str]) -> str:
    out = template_text
    for key, value in values.items():
        out = out.replace("{{" + key + "}}", str(value))
    return out


def load_template(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_service_preview(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_systemd_unit(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def build_systemctl_commands(
    *,
    reader_service_name: str,
    builder_service_name: str,
    builder_timer_name: str,
    ui_service_name: str,
    ui_mode: str,
) -> list[str]:
    cmds = [
        "sudo systemctl daemon-reload",
        f"sudo systemctl enable {reader_service_name}",
        f"sudo systemctl enable {builder_timer_name}",
    ]
    if ui_mode == "built-in":
        cmds.append(f"sudo systemctl enable {ui_service_name}")

    cmds.extend([
        f"sudo systemctl restart {reader_service_name}",
        f"sudo systemctl restart {builder_timer_name}",
    ])
    if ui_mode == "built-in":
        cmds.append(f"sudo systemctl restart {ui_service_name}")

    return cmds

#!/usr/bin/env python3
from pathlib import Path


def load_template(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def render_template(template_text: str, values: dict[str, str]) -> str:
    out = template_text
    for key, value in values.items():
        out = out.replace("{{" + key + "}}", str(value))
    return out


def write_service_preview(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")

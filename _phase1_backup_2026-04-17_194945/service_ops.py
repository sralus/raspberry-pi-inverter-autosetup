\
#!/usr/bin/env python3
from pathlib import Path
from typing import Dict


def render_template(template_text: str, values: Dict[str, str]) -> str:
    out = template_text
    for key, value in values.items():
        out = out.replace("{{" + key + "}}", str(value))
    return out


def load_template(path: Path) -> str:
    return path.read_text(encoding="utf-8")

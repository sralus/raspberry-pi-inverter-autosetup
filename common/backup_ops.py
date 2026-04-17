\
#!/usr/bin/env python3
from pathlib import Path
import shutil
from datetime import datetime
from typing import Iterable


def timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H%M%S")


def backup_paths(paths: Iterable[Path], backup_root: Path) -> None:
    backup_root.mkdir(parents=True, exist_ok=True)
    for src in paths:
        if not src.exists():
            continue
        dst = backup_root / src.name
        if src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dst)

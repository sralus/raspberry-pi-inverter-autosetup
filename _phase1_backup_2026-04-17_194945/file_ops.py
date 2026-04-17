\
#!/usr/bin/env python3
from pathlib import Path
import shutil


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def copy_file(src: Path, dst: Path) -> None:
    ensure_dir(dst.parent)
    shutil.copy2(src, dst)


def copy_tree(src: Path, dst: Path) -> None:
    ensure_dir(dst.parent)
    shutil.copytree(src, dst, dirs_exist_ok=True)


def write_text(path: Path, content: str) -> None:
    ensure_dir(path.parent)
    path.write_text(content, encoding="utf-8")

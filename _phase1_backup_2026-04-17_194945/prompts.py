\
#!/usr/bin/env python3
from getpass import getpass
from typing import Any, Callable, Optional


def ask_string(prompt: str, default: str = "") -> str:
    suffix = f" [{default}]" if default != "" else ""
    value = input(f"{prompt}{suffix}: ").strip()
    return value if value else default


def ask_int(prompt: str, default: int) -> int:
    while True:
        raw = input(f"{prompt} [{default}]: ").strip()
        if raw == "":
            return default
        try:
            return int(raw)
        except ValueError:
            print("Bitte eine ganze Zahl eingeben.")


def ask_yes_no(prompt: str, default: bool = True) -> bool:
    label = "Y/n" if default else "y/N"
    while True:
        raw = input(f"{prompt} [{label}]: ").strip().lower()
        if raw == "":
            return default
        if raw in {"y", "yes", "j", "ja"}:
            return True
        if raw in {"n", "no", "nein"}:
            return False
        print("Bitte ja oder nein eingeben.")


def ask_secret(prompt: str, default_empty: bool = True) -> str:
    value = getpass(f"{prompt}: ")
    if default_empty and value == "":
        return ""
    return value

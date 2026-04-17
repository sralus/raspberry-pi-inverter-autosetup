\
#!/usr/bin/env python3

def validate_device_name(name: str) -> str:
    cleaned = name.strip()
    if not cleaned:
        raise ValueError("Gerätename darf nicht leer sein.")
    return cleaned


def validate_ui_dir(path: str) -> str:
    cleaned = path.strip()
    if not cleaned.startswith("/"):
        raise ValueError("UI-Zielordner muss absolut sein.")
    return cleaned

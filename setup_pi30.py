#!/usr/bin/env python3
from pathlib import Path
from common.installer_common import collect_common_answers, build_common_install_paths
from common.file_ops import ensure_dir, copy_tree, write_text
from common.service_ops import (
    load_template,
    render_template,
    write_service_preview,
    build_systemctl_commands,
)
from common.config_ops import save_json

PROJECT_ROOT = Path(__file__).resolve().parent
PLATFORM_DIR = PROJECT_ROOT / "watchpower-like"
RUNTIME_SRC = PLATFORM_DIR / "runtime"
UI_SRC = PLATFORM_DIR / "ui"
TOOLS_SRC = PROJECT_ROOT / "tools"
TEMPLATES_DIR = PLATFORM_DIR / "templates"

INSTALL_ROOT = Path("/home/pi/inverter-autosetup/watchpower-like")
SYSTEMD_DIR = Path("/etc/systemd/system")


def main() -> int:
    print("== PI30 / WR1 Auto-Setup ==")
    answers = collect_common_answers(
        default_name="WR1",
        default_ui_dir="/home/pi/wr1-ui",
        default_ui_port=8095,
    )

    device_name = answers["device_name"]
    service_slug = answers["service_slug"]
    ui_dir = Path(answers["ui_dir"])
    install_root = INSTALL_ROOT

    paths = build_common_install_paths(
        install_root=install_root,
        ui_dir=ui_dir,
        device_name=device_name,
    )

    reader_service_name = f"{service_slug}-reader.service"
    builder_service_name = f"{service_slug}-builder.service"
    builder_timer_name = f"{service_slug}-builder.timer"
    ui_service_name = f"{service_slug}-ui.service"

    config = {
        **answers,
        **paths,
        "protocol": "pi30",
        "service_name": reader_service_name,
        "builder_service_name": builder_service_name,
        "builder_timer_name": builder_timer_name,
        "ui_service_name": ui_service_name,
        "mode": "install",
        "systemd_dir": str(SYSTEMD_DIR),
    }

    reader_tpl = load_template(TEMPLATES_DIR / "pi30-reader.service.tpl")
    builder_tpl = load_template(TEMPLATES_DIR / "pi30-builder.service.tpl")
    timer_tpl = load_template(TEMPLATES_DIR / "pi30-builder.timer.tpl")
    ui_tpl = load_template(TEMPLATES_DIR / "pi30-ui.service.tpl")

    values = {
        "install_root": paths["install_root"],
        "runtime_dir": paths["runtime_dir"],
        "tools_dir": paths["tools_dir"],
        "latest_json": paths["latest_json"],
        "ui_dir": str(ui_dir),
        "state_json": paths["state_json"],
        "config_path": paths["config_path"],
        "device_path": answers["device_path"],
        "device_name": answers["device_name"],
        "poll_interval": str(answers["poll_interval"]),
        "mqtt_enabled": "true" if answers["mqtt_enabled"] else "false",
        "mqtt_host": answers["mqtt_host"],
        "mqtt_port": str(answers["mqtt_port"]),
        "mqtt_username": answers["mqtt_username"],
        "mqtt_password": answers["mqtt_password"],
        "reader_service_name": reader_service_name,
        "builder_service_name": builder_service_name,
        "builder_timer_name": builder_timer_name,
        "ui_service_name": ui_service_name,
        "ui_port": str(answers["ui_port"]),
        "service_description": "WR1 MQTT Reader",
        "builder_description": "WR1 UI State Builder",
        "timer_description": "Refresh WR1 UI State JSON",
        "ui_description": "WR1 Built-in UI",
    }

    build_dir = install_root / "build"
    ensure_dir(build_dir)
    ensure_dir(install_root)

    copy_tree(RUNTIME_SRC, install_root / "runtime")
    copy_tree(TOOLS_SRC, install_root / "tools")
    copy_tree(UI_SRC, ui_dir)

    save_json(Path(paths["config_path"]), config)

    reader_text = render_template(reader_tpl, values)
    builder_text = render_template(builder_tpl, values)
    timer_text = render_template(timer_tpl, values)
    ui_text = render_template(ui_tpl, values)

    write_service_preview(build_dir / reader_service_name, reader_text)
    write_service_preview(build_dir / builder_service_name, builder_text)
    write_service_preview(build_dir / builder_timer_name, timer_text)

    if answers["ui_mode"] == "built-in":
        write_service_preview(build_dir / ui_service_name, ui_text)

    install_commands = [
        f"sudo cp '{build_dir / reader_service_name}' '{SYSTEMD_DIR / reader_service_name}'",
        f"sudo cp '{build_dir / builder_service_name}' '{SYSTEMD_DIR / builder_service_name}'",
        f"sudo cp '{build_dir / builder_timer_name}' '{SYSTEMD_DIR / builder_timer_name}'",
    ]
    if answers["ui_mode"] == "built-in":
        install_commands.append(
            f"sudo cp '{build_dir / ui_service_name}' '{SYSTEMD_DIR / ui_service_name}'"
        )

    install_commands.extend(
        build_systemctl_commands(
            reader_service_name=reader_service_name,
            builder_service_name=builder_service_name,
            builder_timer_name=builder_timer_name,
            ui_service_name=ui_service_name,
            ui_mode=answers["ui_mode"],
        )
    )

    summary = [
        "FERTIG (Repo-/Projektstand, built-in-first).",
        f"Gerätename: {device_name}",
        f"Service-Slug: {service_slug}",
        f"Port: {answers['device_path']}",
        f"MQTT aktiv: {answers['mqtt_enabled']}",
        f"MQTT Host/Port: {answers['mqtt_host']}:{answers['mqtt_port']}" if answers["mqtt_enabled"] else "MQTT: deaktiviert",
        f"UI-Modus: {answers['ui_mode']}",
        f"UI-Zielordner: {ui_dir}",
        f"UI-Port: {answers['ui_port']}" if answers["ui_mode"] == "built-in" else "UI-Port: entfällt (external)",
        f"Install-Root: {install_root}",
        f"Runtime: {install_root / 'runtime'}",
        f"Tools: {install_root / 'tools'}",
        f"Konfig: {paths['config_path']}",
        f"Reader-Service-Datei: {build_dir / reader_service_name}",
        f"Builder-Service-Datei: {build_dir / builder_service_name}",
        f"Builder-Timer-Datei: {build_dir / builder_timer_name}",
    ]
    if answers["ui_mode"] == "built-in":
        summary.append(f"UI-Service-Datei: {build_dir / ui_service_name}")

    summary.append("")
    summary.append("Installationsbefehle für echten Einsatz:")
    summary.extend(install_commands)

    write_text(build_dir / "SUMMARY.txt", "\n".join(summary) + "\n")
    write_text(build_dir / "INSTALL_COMMANDS.sh", "#!/usr/bin/env bash\nset -euo pipefail\n\n" + "\n".join(install_commands) + "\n")

    print()
    for line in summary:
        print(line)
    print()
    print("Hinweis: Dieses Setup kopiert Projektdateien und erzeugt echte systemd-Vorlagen plus Installationsskript.")
    print("Es führt absichtlich noch keine systemctl-Befehle selbst aus.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

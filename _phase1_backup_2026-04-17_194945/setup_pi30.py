#!/usr/bin/env python3
from pathlib import Path
from common.installer_common import collect_common_answers
from common.file_ops import copy_tree, write_text, ensure_dir
from common.service_ops import load_template, render_template
from common.config_ops import save_json

PROJECT_ROOT = Path(__file__).resolve().parent
RUNTIME_SRC = PROJECT_ROOT / "pi30" / "runtime"
WEB_SRC = PROJECT_ROOT / "pi30" / "web"
SERVICE_TEMPLATE = PROJECT_ROOT / "pi30" / "templates" / "pi30-reader.service.tpl"

INSTALL_RUNTIME_DIR = Path("/home/pi/inverter-autosetup-test/pi30")
CONFIG_PATH = Path("/home/pi/inverter-autosetup-test/config_pi30_test.json")

def main() -> int:
    print("== PI30 Auto-Setup TESTMODUS ==")
    answers = collect_common_answers(default_name="WR1", default_ui_dir="/var/www/html/wr1test-auto-ui")

    ensure_dir(INSTALL_RUNTIME_DIR.parent)
    copy_tree(RUNTIME_SRC, INSTALL_RUNTIME_DIR)
    copy_tree(WEB_SRC, Path(answers["ui_dir"]))

    save_json(CONFIG_PATH, {
        **answers,
        "protocol": "pi30",
        "install_runtime_dir": str(INSTALL_RUNTIME_DIR),
        "service_name": "pi30-reader-test.service",
        "mode": "test",
    })

    template = load_template(SERVICE_TEMPLATE)
    service_text = render_template(template, {
        "install_runtime_dir": str(INSTALL_RUNTIME_DIR),
        "device_path": answers["device_path"],
        "device_name": answers["device_name"],
        "poll_interval": str(answers["poll_interval"]),
        "service_description": "PI30 Reader TEST",
    })

    local_service_copy = PROJECT_ROOT / "build" / "pi30-reader-test.service"
    write_text(local_service_copy, service_text)

    print()
    print("FERTIG (TESTMODUS).")
    print(f"Gerätename: {answers['device_name']}")
    print(f"Port: {answers['device_path']}")
    print(f"MQTT aktiv: {answers['mqtt_enabled']}")
    if answers["mqtt_enabled"]:
        print(f"MQTT: {answers['mqtt_host']}:{answers['mqtt_port']}")
    print(f"UI-Testordner: {answers['ui_dir']}")
    print(f"Runtime-Testordner: {INSTALL_RUNTIME_DIR}")
    print(f"Konfig: {CONFIG_PATH}")
    print(f"Service-Vorschau: {local_service_copy}")
    print()
    print("Hinweis: Diese Testversion schreibt noch keinen echten systemd-Service nach /etc/systemd/system.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

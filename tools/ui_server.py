#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import mimetypes
import os
import posixpath
import subprocess
import sys
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


def parse_args():
    p = argparse.ArgumentParser(description="Built-in full UI/API server")
    p.add_argument("--dir", required=True, help="Directory to serve")
    p.add_argument("--port", required=True, type=int, help="Port to listen on")
    p.add_argument("--state-json", default="", help="Explicit path to state json")
    p.add_argument("--ctl", default="", help="Controller script path, e.g. wr2_ctl.py")
    p.add_argument("--device", default="", help="Override controller device path")
    p.add_argument("--reader-service", default="", help="Override reader service name for controller")
    return p.parse_args()


ARGS = parse_args()
UI_DIR = Path(ARGS.dir).resolve()
STATE_JSON = Path(ARGS.state_json).resolve() if ARGS.state_json else (UI_DIR / "wr2_state.json")
CTL_PATH = Path(ARGS.ctl).resolve() if ARGS.ctl else None
DEVICE = (ARGS.device or "").strip()
READER_SERVICE = (ARGS.reader_service or "").strip()
OVERRIDE_JSON = (CTL_PATH.parent / "wr2_ui_state_override.json") if CTL_PATH else (Path(__file__).resolve().parent.parent / "runtime" / "wr2_ui_state_override.json")

ACTION_MAP = {
    "set_psp": "set-psp",
    "set_pcp": "set-pcp",
    "set_pop": "set-pop",
    "set_bulk_voltage": "set-bulk-float",
    "set_float_voltage": "set-bulk-float",
    "set_bucd": "set-bucd",
    "set_psdv": "set-psdv",
    "set_flag": "set-flag",
}

UNSUPPORTED_ACTIONS = {}


def json_bytes(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


def content_type_for(path: Path) -> str:
    ctype, _ = mimetypes.guess_type(str(path))
    return ctype or "application/octet-stream"


def safe_join(base: Path, req_path: str) -> Path:
    req_path = posixpath.normpath(req_path)
    req_path = req_path.lstrip("/")
    candidate = (base / req_path).resolve()
    if candidate == base or base in candidate.parents:
        return candidate
    raise PermissionError("path traversal blocked")


def extract_json_from_output(text: str):
    text = (text or "").strip()
    if not text:
        return None

    try:
        return json.loads(text)
    except Exception:
        pass

    lines = text.splitlines()
    candidate_starts = [i for i, line in enumerate(lines) if line.strip().startswith("{")]
    for start in reversed(candidate_starts):
        candidate = "\n".join(lines[start:]).strip()
        try:
            return json.loads(candidate)
        except Exception:
            continue
    return None


def load_json_file(path: Path):
    try:
        if path and path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def resolve_bulk_float_pair(action: str, value: str):
    state = load_json_file(STATE_JSON)
    override = load_json_file(OVERRIDE_JSON)

    state_settings = state.get("settings", {}) if isinstance(state, dict) else {}
    ovr_settings = override.get("settings", {}) if isinstance(override, dict) else {}

    bulk_current = ovr_settings.get("bulk_voltage_v") or state_settings.get("bulk_voltage_v") or "56.4"
    float_current = ovr_settings.get("floating_voltage_v") or state_settings.get("floating_voltage_v") or "54.0"

    if action == "set_bulk_voltage":
        return str(value), str(float_current)
    if action == "set_float_voltage":
        return str(bulk_current), str(value)
    return str(bulk_current), str(float_current)


def run_ctl(action: str, value):
    if action in UNSUPPORTED_ACTIONS:
        return 400, {
            "ok": False,
            "error": UNSUPPORTED_ACTIONS[action],
            "action": action,
            "requested_value": value,
        }

    cmd_name = ACTION_MAP.get(action)
    if not cmd_name:
        return 400, {
            "ok": False,
            "error": f"Unbekannte action: {action}",
            "action": action,
            "requested_value": value,
        }

    if not CTL_PATH or not CTL_PATH.exists():
        return 500, {
            "ok": False,
            "error": f"Controller-Skript nicht gefunden: {CTL_PATH}",
            "action": action,
            "requested_value": value,
        }

    env = os.environ.copy()
    if DEVICE:
        env["WR2_CTL_DEVICE"] = DEVICE
        env["WR1_CTL_DEVICE"] = DEVICE
    if READER_SERVICE:
        env["WR2_CTL_SERVICE"] = READER_SERVICE
        env["WR1_CTL_SERVICE"] = READER_SERVICE

    if action in ("set_bulk_voltage", "set_float_voltage"):
        bulk_v, float_v = resolve_bulk_float_pair(action, str(value))
        cmd = [sys.executable, str(CTL_PATH), cmd_name, str(bulk_v), str(float_v)]

    elif action == "set_bucd":
        if not isinstance(value, dict):
            return 400, {
                "ok": False,
                "error": "set_bucd erwartet Objekt",
                "action": action,
                "requested_value": value,
            }
        recharge_v = str(value.get("recharge_v", "")).strip()
        redischarge_v = str(value.get("redischarge_v", "")).strip()
        cmd = [sys.executable, str(CTL_PATH), cmd_name, recharge_v, redischarge_v]

    elif action == "set_psdv":
        if not isinstance(value, dict):
            return 400, {
                "ok": False,
                "error": "set_psdv erwartet Objekt",
                "action": action,
                "requested_value": value,
            }
        cutoff_v = str(value.get("cutoff_v", "")).strip()
        cmd = [sys.executable, str(CTL_PATH), cmd_name, cutoff_v]

    elif action == "set_flag":
        if not isinstance(value, dict):
            return 400, {
                "ok": False,
                "error": "set_flag erwartet Objekt",
                "action": action,
                "requested_value": value,
            }
        key = str(value.get("key", "")).strip().upper()
        enabled = str(value.get("enabled", "")).strip()
        cmd = [sys.executable, str(CTL_PATH), cmd_name, key, enabled]

    else:
        cmd = [sys.executable, str(CTL_PATH), cmd_name, str(value)]

    proc = subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        env=env,
    )

    parsed = extract_json_from_output(proc.stdout)
    if parsed is None:
        parsed = {
            "ok": proc.returncode == 0,
            "error": "Controller-Ausgabe konnte nicht als JSON gelesen werden.",
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "returncode": proc.returncode,
            "action": action,
            "requested_value": value,
            "cmd": cmd,
        }

    parsed.setdefault("returncode", proc.returncode)
    parsed.setdefault("action", action)
    parsed.setdefault("requested_value", value)
    parsed.setdefault("cmd", cmd)
    if proc.stderr:
        parsed.setdefault("stderr", proc.stderr)

    status = 200 if proc.returncode == 0 and parsed.get("ok") else 400
    return status, parsed


class Handler(BaseHTTPRequestHandler):
    server_version = "BuiltInFullUI/1.1"

    def log_message(self, fmt, *args):
        sys.stdout.write("%s - - [%s] %s\n" % (
            self.client_address[0],
            self.log_date_time_string(),
            fmt % args
        ))
        sys.stdout.flush()

    def _send_bytes(self, status: int, data: bytes, ctype: str):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.end_headers()
        self.wfile.write(data)

    def _send_json(self, status: int, obj):
        self._send_bytes(status, json_bytes(obj), "application/json; charset=utf-8")

    def _send_text(self, status: int, text: str):
        self._send_bytes(status, text.encode("utf-8"), "text/plain; charset=utf-8")

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path or "/"

            if path in ("/wr2_state.json", "/wr1_state.json"):
                if not STATE_JSON.exists():
                    return self._send_json(404, {"ok": False, "error": f"State JSON not found: {STATE_JSON}"})
                data = STATE_JSON.read_bytes()
                return self._send_bytes(200, data, "application/json; charset=utf-8")

            if path == "/health":
                return self._send_json(200, {
                    "ok": True,
                    "ui_dir": str(UI_DIR),
                    "state_json": str(STATE_JSON),
                    "ctl": str(CTL_PATH) if CTL_PATH else "",
                    "device": DEVICE,
                    "reader_service": READER_SERVICE,
                    "override_json": str(OVERRIDE_JSON),
                })

            if path == "/":
                file_path = UI_DIR / "index.html"
            else:
                file_path = safe_join(UI_DIR, path)

            if not file_path.exists() or not file_path.is_file():
                return self._send_text(404, f"Not found: {path}")

            return self._send_bytes(200, file_path.read_bytes(), content_type_for(file_path))

        except PermissionError as e:
            return self._send_json(403, {"ok": False, "error": str(e)})
        except Exception as e:
            traceback.print_exc()
            return self._send_json(500, {"ok": False, "error": str(e)})

    def do_HEAD(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path or "/"

            if path == "/":
                file_path = UI_DIR / "index.html"
            elif path in ("/wr2_state.json", "/wr1_state.json"):
                file_path = STATE_JSON
            else:
                file_path = safe_join(UI_DIR, path)

            if not file_path.exists() or not file_path.is_file():
                self.send_response(404)
                self.end_headers()
                return

            data = file_path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", content_type_for(file_path))
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            self.end_headers()
        except Exception:
            self.send_response(500)
            self.end_headers()

    def do_POST(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path or ""
            qs = parse_qs(parsed.query or "")

            if path not in ("/api/wr1-api.php", "/api/wr2-api.php", "/api/apply", "/wr2-api/set", "/wr2-api/set-bulk-float", "/wr2-api/set-bucd", "/wr2-api/set-psdv", "/wr2-api/set-flag"):
                return self._send_json(404, {"ok": False, "error": f"Unknown API path: {path}"})

            try:
                length = int(self.headers.get("Content-Length", "0"))
            except Exception:
                length = 0

            raw = self.rfile.read(length) if length > 0 else b""
            try:
                body = json.loads(raw.decode("utf-8") if raw else "{}")
            except Exception:
                return self._send_json(400, {"ok": False, "error": "Ungültiges JSON im POST-Body."})

            action = str(body.get("action", "") or "").strip()
            value = body.get("value", "")

            if not action and path == "/api/wr2-api.php":
                q_action = str(qs.get("action", [""])[0]).strip()
                kind = str(body.get("kind", "") or "").strip().lower()

                if q_action == "set" and kind in ("psp", "pcp", "pop"):
                    action = f"set_{kind}"
                    value = str(body.get("value", "")).strip()

                elif q_action == "set-bulk-float":
                    bulk_v = str(body.get("bulk_v", "")).strip()
                    float_v = str(body.get("float_v", "")).strip()

                    status, payload = run_ctl("set_bulk_voltage", bulk_v)
                    if status != 200:
                        return self._send_json(status, payload)

                    status, payload = run_ctl("set_float_voltage", float_v)
                    return self._send_json(status, payload)

                elif q_action == "set-bucd":
                    action = "set_bucd"
                    value = {
                        "recharge_v": str(body.get("recharge_v", "")).strip(),
                        "redischarge_v": str(body.get("redischarge_v", "")).strip(),
                    }

                elif q_action in ("set-cutoff", "set-psdv"):
                    action = "set_psdv"
                    value = {
                        "cutoff_v": str(body.get("cutoff_v", "")).strip(),
                    }

                elif q_action == "set-flag":
                    action = "set_flag"
                    value = {
                        "key": str(body.get("key", "")).strip().upper(),
                        "enabled": str(body.get("enabled", "")).strip(),
                    }

            if not action and path == "/wr2-api/set":
                kind = str(body.get("kind", "") or "").strip().lower()
                if kind in ("psp", "pcp", "pop"):
                    action = f"set_{kind}"
                    value = str(body.get("value", "")).strip()

            elif not action and path == "/wr2-api/set-bulk-float":
                bulk_v = str(body.get("bulk_v", "")).strip()
                float_v = str(body.get("float_v", "")).strip()

                status, payload = run_ctl("set_bulk_voltage", bulk_v)
                if status != 200:
                    return self._send_json(status, payload)

                status, payload = run_ctl("set_float_voltage", float_v)
                return self._send_json(status, payload)

            elif not action and path == "/wr2-api/set-bucd":
                action = "set_bucd"
                value = {
                    "recharge_v": str(body.get("recharge_v", "")).strip(),
                    "redischarge_v": str(body.get("redischarge_v", "")).strip(),
                }

            elif not action and path == "/wr2-api/set-psdv":
                action = "set_psdv"
                value = {
                    "cutoff_v": str(body.get("cutoff_v", "")).strip(),
                }

            elif not action and path == "/wr2-api/set-flag":
                action = "set_flag"
                value = {
                    "key": str(body.get("key", "")).strip().upper(),
                    "enabled": str(body.get("enabled", "")).strip(),
                }

            if action == "":
                return self._send_json(400, {"ok": False, "error": "Feld 'action' fehlt."})

            status, payload = run_ctl(action, value)
            return self._send_json(status, payload)

        except Exception as e:
            traceback.print_exc()
            return self._send_json(500, {"ok": False, "error": str(e)})


def main() -> int:
    if not UI_DIR.exists():
        raise SystemExit(f"UI directory not found: {UI_DIR}")

    print(f"Serving UI from {UI_DIR}", flush=True)
    print(f"State JSON: {STATE_JSON}", flush=True)
    print(f"Controller: {CTL_PATH}", flush=True)
    if DEVICE:
        print(f"Controller device override: {DEVICE}", flush=True)
    if READER_SERVICE:
        print(f"Controller reader service override: {READER_SERVICE}", flush=True)
    print(f"Override JSON: {OVERRIDE_JSON}", flush=True)

    httpd = ThreadingHTTPServer(("0.0.0.0", ARGS.port), Handler)
    try:
        print(f"Listening on 0.0.0.0:{ARGS.port}", flush=True)
        httpd.serve_forever()
    finally:
        httpd.server_close()


if __name__ == "__main__":
    raise SystemExit(main())

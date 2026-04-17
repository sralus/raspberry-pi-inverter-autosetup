#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOST = "127.0.0.1"
PORT = 9137

WR2_CTL = os.environ.get("WR2_CTL", str(BASE_DIR / "wr2_ctl.py"))
STATE_BUILDER = os.environ.get("STATE_BUILDER", str(BASE_DIR / "wr2_state_builder.py"))

ALLOWED = {
    "psp": {"0", "1", "2"},
    "pcp": {"0", "1", "2"},
    "pop": {"0", "1"},
}

SAFE_FLAG_KEYS = {"F"}

def run_cmd(cmd):
    return subprocess.run(cmd, text=True, capture_output=True)

class Handler(BaseHTTPRequestHandler):
    server_version = "wr2-ui-api/0.4"

    def _send_json(self, code, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return

    def _norm_path(self):
        p = self.path.split("?", 1)[0]
        if p.startswith("/wr2-api/"):
            p = p[len("/wr2-api"):]
        return p

    def do_GET(self):
        p = self._norm_path()
        if p == "/health":
            return self._send_json(200, {"ok": True, "service": "wr2-ui-api", "path": p})
        return self._send_json(404, {"ok": False, "error": "not found", "path": p, "orig_path": self.path})

    def do_POST(self):
        p = self._norm_path()

        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            data = json.loads(raw.decode("utf-8"))
        except Exception as e:
            return self._send_json(400, {"ok": False, "error": f"invalid json: {e}"})

        if p == "/set":
            kind = str(data.get("kind", "")).strip().lower()
            value = str(data.get("value", "")).strip()

            if kind not in ALLOWED:
                return self._send_json(400, {"ok": False, "error": f"invalid kind: {kind}"})
            if value not in ALLOWED[kind]:
                return self._send_json(400, {"ok": False, "error": f"invalid value for {kind}: {value}"})

            ctl_cmd = [WR2_CTL, f"set-{kind}", value]

        elif p == "/set-bulk-float":
            bulk_v = str(data.get("bulk_v", "")).strip()
            float_v = str(data.get("float_v", "")).strip()
            ctl_cmd = [WR2_CTL, "set-bulk-float", bulk_v, float_v]

        elif p == "/set-bucd":
            recharge_v = str(data.get("recharge_v", "")).strip()
            redischarge_v = str(data.get("redischarge_v", "")).strip()
            ctl_cmd = [WR2_CTL, "set-bucd", recharge_v, redischarge_v]

        elif p == "/set-psdv":
            cutoff_v = str(data.get("cutoff_v", "")).strip()
            ctl_cmd = [WR2_CTL, "set-psdv", cutoff_v]

        elif p == "/set-flag":
            key = str(data.get("key", "")).strip().upper()
            enabled = str(data.get("enabled", "")).strip()
            if key not in SAFE_FLAG_KEYS:
                return self._send_json(400, {"ok": False, "error": f"flag not allowed: {key}"})
            if enabled not in {"0", "1"}:
                return self._send_json(400, {"ok": False, "error": f"invalid enabled: {enabled}"})
            ctl_cmd = [WR2_CTL, "set-flag", key, enabled]

        else:
            return self._send_json(404, {"ok": False, "error": "not found", "path": p, "orig_path": self.path})

        ctl = run_cmd(ctl_cmd)
        if ctl.returncode != 0:
            return self._send_json(500, {
                "ok": False,
                "error": "wr2_ctl failed",
                "stdout": ctl.stdout,
                "stderr": ctl.stderr,
                "cmd": ctl_cmd
            })

        build = run_cmd(["/usr/bin/python3", STATE_BUILDER])
        payload = {
            "ok": build.returncode == 0,
            "ctl_stdout": ctl.stdout,
            "ctl_stderr": ctl.stderr,
            "builder_stdout": build.stdout,
            "builder_stderr": build.stderr,
        }
        if build.returncode != 0:
            payload["error"] = "state builder failed"
            return self._send_json(500, payload)

        return self._send_json(200, payload)

def main():
    httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    httpd.serve_forever()

if __name__ == "__main__":
    main()

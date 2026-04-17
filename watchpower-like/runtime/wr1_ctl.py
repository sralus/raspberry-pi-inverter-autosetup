#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import errno
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Optional

DEVICE = "/dev/WR1"
SERVICE = "wr1-reader.service"
READER_WAS_ACTIVE = False

STOP_WAIT_SEC = 2.0
START_WAIT_SEC = 3.0
OPEN_SETTLE_SEC = 1.00
REOPEN_WAIT_SEC = 1.50

SAFE_OUTPUT_PRIORITY = {
    "0": "POP00",
    "1": "POP01",
    "2": "POP02",
}

SAFE_CHARGER_PRIORITY = {
    "0": "PCP00",
    "2": "PCP02",
    "3": "PCP03",
}

WRITE_RETRIES = 3
WRITE_SETTLE_SEC = 2.20
READ_GAP_SEC = 0.80

QPIRI_ONLY_ACTIONS = {
    "set_recharge_voltage",
    "set_redischarge_voltage",
    "set_bulk_voltage",
    "set_float_voltage",
}


@dataclass
class CmdResult:
    command: str
    kind: str
    data: Optional[str]
    raw_hex: str
    raw_len: int
    crc_ok: Optional[bool] = None


def run_cmd(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def service_is_active() -> bool:
    res = run_cmd(["systemctl", "is-active", SERVICE], check=False)
    return (res.stdout or "").strip() == "active"


def stop_reader() -> None:
    global READER_WAS_ACTIVE
    READER_WAS_ACTIVE = service_is_active()
    if not READER_WAS_ACTIVE:
        print(f"== stop {SERVICE} skipped (already inactive) ==", flush=True)
        return
    print(f"== stop {SERVICE} ==", flush=True)
    run_cmd(["sudo", "-n", "systemctl", "stop", SERVICE])
    time.sleep(STOP_WAIT_SEC)


def start_reader() -> None:
    global READER_WAS_ACTIVE
    if not READER_WAS_ACTIVE:
        print(f"== start {SERVICE} skipped (was inactive before) ==", flush=True)
        return
    print(f"== start {SERVICE} ==", flush=True)
    run_cmd(["sudo", "-n", "systemctl", "start", SERVICE])
    time.sleep(START_WAIT_SEC)


def crc_xmodem(data: bytes) -> int:
    crc = 0
    for b in data:
        crc ^= (b << 8)
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc


def adapt_crc_bytes(crc_bytes: bytes) -> bytes:
    out = bytearray()
    for b in crc_bytes:
        if b == 0x0A:
            out.append(0x0B)
        elif b == 0x0D:
            out.append(0x0E)
        elif b == 0x28:
            out.append(0x29)
        else:
            out.append(b)
    return bytes(out)


def build_frame(cmd: str) -> bytes:
    payload = cmd.encode("ascii")
    crc = crc_xmodem(payload)
    crc_bytes = bytes([(crc >> 8) & 0xFF, crc & 0xFF])
    crc_bytes = adapt_crc_bytes(crc_bytes)
    return payload + crc_bytes + b"\x0D"


def is_hidraw_device(path: str) -> bool:
    try:
        real = os.path.realpath(path)
    except Exception:
        real = path
    return "/hidraw" in real or os.path.basename(real).startswith("hidraw")


def chunk8(data: bytes) -> list[bytes]:
    parts = []
    for i in range(0, len(data), 8):
        part = data[i:i + 8]
        if len(part) < 8:
            part = part + b"\x00" * (8 - len(part))
        parts.append(part)
    return parts


def chunk8_report_write(fd: int, data: bytes) -> int:
    total = 0
    for part in chunk8(data):
        total += os.write(fd, part)
        time.sleep(0.12)
    return total


def device_write(fd: int, data: bytes) -> int:
    if is_hidraw_device(DEVICE):
        return chunk8_report_write(fd, data)
    return os.write(fd, data)


def open_device() -> int:
    fd = os.open(DEVICE, os.O_RDWR | os.O_NONBLOCK)
    time.sleep(OPEN_SETTLE_SEC)
    return fd


def close_device(fd: Optional[int]) -> None:
    if fd is None:
        return
    try:
        os.close(fd)
    except Exception:
        pass


def reopen_device(fd: Optional[int]) -> int:
    close_device(fd)
    time.sleep(REOPEN_WAIT_SEC)
    return open_device()


def drain(fd: int, seconds: float = 0.45) -> bytes:
    end = time.time() + seconds
    buf = b""
    while time.time() < end:
        try:
            chunk = os.read(fd, 4096)
            if chunk:
                buf += chunk
            else:
                time.sleep(0.02)
        except BlockingIOError:
            time.sleep(0.02)
        except OSError:
            break
    return buf


def read_frame(fd: int, timeout: float = 5.0, max_bytes: int = 4096) -> bytes:
    end = time.time() + timeout
    started = False
    buf = bytearray()

    while time.time() < end and len(buf) < max_bytes:
        try:
            chunk = os.read(fd, 256)
            if not chunk:
                time.sleep(0.02)
                continue
        except BlockingIOError:
            time.sleep(0.02)
            continue
        except OSError:
            break

        for b in chunk:
            if not started:
                if b == 0x28:
                    started = True
                    buf.append(b)
            else:
                buf.append(b)
                if b == 0x0D:
                    return bytes(buf)

    return bytes(buf)


def decode_frame(raw_frame: bytes) -> CmdResult:
    raw = raw_frame.rstrip(b"\x00")
    had_cr = raw.endswith(b"\r")
    body = raw[:-1] if had_cr else raw

    crc_ok: Optional[bool] = None
    payload_bytes = body

    if len(body) >= 3:
        payload_bytes = body[:-2]
        rx_crc = body[-2:]
        calc_crc = crc_xmodem(payload_bytes)
        calc_crc_bytes = adapt_crc_bytes(bytes([(calc_crc >> 8) & 0xFF, calc_crc & 0xFF]))
        crc_ok = (rx_crc == calc_crc_bytes)

    payload_text = payload_bytes.decode("ascii", errors="replace")
    if payload_text.startswith("("):
        payload_text = payload_text[1:]
    payload_text = payload_text.replace("\x00", "").strip()

    kind = "DATA"
    if payload_text in ("ACK", "A"):
        kind = "ACK"
    elif payload_text in ("NAK", "N"):
        kind = "NAK"
    elif payload_text == "":
        kind = "EMPTY"

    return CmdResult(
        command="",
        kind=kind,
        data=payload_text,
        raw_hex=raw_frame.hex(),
        raw_len=len(raw_frame),
        crc_ok=crc_ok,
    )


def send_cmd(fd: int, cmd: str, timeout: float = 5.0, post_write_sleep: float = 0.24) -> tuple[int, CmdResult]:
    drain(fd, 0.35)
    frame = build_frame(cmd)
    device_write(fd, frame)
    time.sleep(post_write_sleep)
    raw = read_frame(fd, timeout=timeout)
    res = decode_frame(raw)
    res.command = cmd
    return fd, res


def send_cmd_resilient(fd: int, cmd: str, timeout: float = 5.0, post_write_sleep: float = 0.24) -> tuple[int, CmdResult]:
    last_err = None
    for _ in range(1, 4):
        try:
            return send_cmd(fd, cmd, timeout=timeout, post_write_sleep=post_write_sleep)
        except BrokenPipeError as e:
            last_err = e
            fd = reopen_device(fd)
        except OSError as e:
            if e.errno == errno.EPIPE:
                last_err = e
                fd = reopen_device(fd)
            else:
                raise
    raise RuntimeError(f"send_cmd_resilient failed for {cmd}: {last_err}")


def parse_qpiri_fields(qpiri_text: str) -> dict:
    out = {}
    try:
        parts = str(qpiri_text).split()
        if len(parts) >= 23:
            out["battery_rating_voltage_v"] = parts[7]
            out["battery_recharge_voltage_v"] = parts[8]
            out["battery_cutoff_voltage_v"] = parts[9]
            out["battery_redischarge_voltage_v"] = parts[22]
            out["bulk_voltage_v"] = parts[10]
            out["float_voltage_v"] = parts[11]
            out["max_ac_charge_current_a"] = parts[13]
            out["max_charge_current_a"] = parts[14]
            out["output_source_priority_raw"] = parts[16]
            out["charger_source_priority_raw"] = parts[17]
    except Exception:
        pass
    return out


def safe_read(fd: int, cmd: str) -> tuple[int, dict]:
    time.sleep(READ_GAP_SEC)
    fd, res = send_cmd_resilient(fd, cmd, timeout=5.0, post_write_sleep=0.18)
    return fd, {
        "kind": res.kind,
        "crc_ok": res.crc_ok,
        "raw_len": res.raw_len,
        "raw_hex": res.raw_hex,
        "data": res.data,
    }


def readback(fd: int) -> tuple[int, dict]:
    out = {}
    for cmd in ["QMOD", "QPIGS", "QPIRI", "QPIWS", "QFLAG"]:
        try:
            fd, out[cmd] = safe_read(fd, cmd)
        except Exception as e:
            out[cmd] = {
                "kind": "ERROR",
                "crc_ok": None,
                "raw_len": 0,
                "raw_hex": "",
                "data": str(e),
            }
    qpiri_data = str(out.get("QPIRI", {}).get("data", "") or "").strip()
    out["QPIRI_PARSED"] = parse_qpiri_fields(qpiri_data)
    return fd, out


def read_qpiri_only(fd: int) -> tuple[int, dict]:
    fd, qpiri = safe_read(fd, "QPIRI")
    out = {
        "QPIRI": qpiri,
        "QPIRI_PARSED": parse_qpiri_fields(str(qpiri.get("data", "") or "").strip()),
    }
    return fd, out


def verify_key_with_extra_qpiri(fd: int, verify_key: str, requested_value: str, loops: int = 4, delay: float = 0.70):
    last_rb = {}
    last_verified = ""
    for _ in range(loops):
        try:
            time.sleep(delay)
            fd, rb = read_qpiri_only(fd)
            last_rb = rb
            parsed = rb.get("QPIRI_PARSED", {}) if isinstance(rb.get("QPIRI_PARSED"), dict) else {}
            verified_value = str(parsed.get(verify_key, "") or "").strip()
            last_verified = verified_value
            if verified_value == requested_value:
                return fd, True, verified_value, rb
        except Exception:
            pass
    return fd, False, last_verified, last_rb


def set_verified_value(action: str, requested_value: str, write_command: str, verify_key: str) -> int:
    fd = None
    result = {
        "ok": False,
        "action": action,
        "requested_value": requested_value,
        "write_command": write_command,
        "attempts": [],
        "verified_by": "",
        "error": "",
    }

    qpiri_only = action in QPIRI_ONLY_ACTIONS

    try:
        stop_reader()
        fd = open_device()

        for attempt in range(1, WRITE_RETRIES + 1):
            attempt_info = {
                "attempt": attempt,
                "write_result": None,
                "readback": {},
                "verified": False,
                "verified_value": None,
            }

            fd, write_res = send_cmd_resilient(fd, write_command, timeout=5.2, post_write_sleep=0.32)
            attempt_info["write_result"] = {
                "kind": write_res.kind,
                "crc_ok": write_res.crc_ok,
                "raw_len": write_res.raw_len,
                "raw_hex": write_res.raw_hex,
                "data": write_res.data,
            }

            if write_res.kind == "NAK":
                attempt_info["readback"] = {"QPIRI_PARSED": {}}
                result["attempts"].append(attempt_info)
                fd = reopen_device(fd)
                time.sleep(1.20)
                continue

            time.sleep(WRITE_SETTLE_SEC)

            if qpiri_only:
                fd, rb = read_qpiri_only(fd)
            else:
                fd, rb = readback(fd)

            attempt_info["readback"] = rb

            parsed = rb.get("QPIRI_PARSED", {}) if isinstance(rb.get("QPIRI_PARSED"), dict) else {}
            verified_value = str(parsed.get(verify_key, "") or "").strip()
            attempt_info["verified_value"] = verified_value

            if write_res.kind == "ACK" and verified_value == requested_value:
                attempt_info["verified"] = True
                attempt_info["verified_by"] = "direct_ack+qpiri"
                result["ok"] = True
                result["verified_by"] = "direct_ack+qpiri"
                result["attempts"].append(attempt_info)
                break

            if verified_value == requested_value:
                attempt_info["verified"] = True
                attempt_info["verified_by"] = "qpiri_readback"
                result["ok"] = True
                result["verified_by"] = "qpiri_readback"
                result["attempts"].append(attempt_info)
                break

            if qpiri_only:
                extra_loops = 4
                extra_delay = 0.80
            else:
                extra_loops = 5
                extra_delay = 0.45
                if action == "set_redischarge_voltage":
                    extra_loops = 12
                    extra_delay = 0.70
                elif action in ("set_recharge_voltage", "set_bulk_voltage", "set_float_voltage"):
                    extra_loops = 6
                    extra_delay = 0.50

            fd, extra_ok, extra_verified_value, extra_rb = verify_key_with_extra_qpiri(
                fd, verify_key, requested_value, loops=extra_loops, delay=extra_delay
            )

            if extra_rb:
                attempt_info["readback_after_wait"] = extra_rb
            if extra_verified_value:
                attempt_info["verified_value_after_wait"] = extra_verified_value

            if extra_ok:
                attempt_info["verified"] = True
                attempt_info["verified_by"] = "delayed_qpiri_readback"
                attempt_info["verified_value"] = extra_verified_value
                if extra_rb:
                    attempt_info["readback"] = extra_rb
                result["ok"] = True
                result["verified_by"] = "delayed_qpiri_readback"
                result["attempts"].append(attempt_info)
                break

            result["attempts"].append(attempt_info)
            fd = reopen_device(fd)
            time.sleep(1.00)

        if not result["ok"]:
            last = result["attempts"][-1] if result["attempts"] else {}
            last_write = last.get("write_result", {})
            last_verified = last.get("verified_value")
            last_qpiri = last.get("readback", {}).get("QPIRI", {}).get("data")
            result["error"] = (
                f"Write not verified after {WRITE_RETRIES} attempts. "
                f"last_write_kind={last_write.get('kind')} "
                f"last_write_data={last_write.get('data')!r} "
                f"last_qpiri={last_qpiri!r} "
                f"last_verified_value={last_verified!r}"
            )

    finally:
        close_device(fd)
        try:
            start_reader()
        except Exception as e:
            if not result["ok"] and not result["error"]:
                result["error"] = f"restart failed: {e}"

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["ok"] else 1


def normalize_voltage_arg(value: str) -> str:
    return f"{float(value):.1f}"


def normalize_voltage_write_arg(value: str) -> str:
    return normalize_voltage_arg(value).replace(".", ",")


def set_output_priority(value: str) -> int:
    if value not in SAFE_OUTPUT_PRIORITY:
        raise ValueError("Nur Output-Priority 0, 1 oder 2 ist aktuell im Controller freigegeben")
    return set_verified_value("set_output_priority", value, SAFE_OUTPUT_PRIORITY[value], "output_source_priority_raw")


def set_charger_priority(value: str) -> int:
    if value not in SAFE_CHARGER_PRIORITY:
        raise ValueError("Nur Charger-Priority 0, 2 oder 3 ist aktuell freigegeben")
    return set_verified_value("set_charger_priority", value, SAFE_CHARGER_PRIORITY[value], "charger_source_priority_raw")


def set_recharge_voltage(value: str) -> int:
    val = normalize_voltage_arg(value)
    write_val = normalize_voltage_write_arg(value)
    return set_verified_value("set_recharge_voltage", val, f"PBCV{write_val}", "battery_recharge_voltage_v")


def set_redischarge_voltage(value: str) -> int:
    val = normalize_voltage_arg(value)
    write_val = normalize_voltage_write_arg(value)
    return set_verified_value("set_redischarge_voltage", val, f"PBDV{write_val}", "battery_redischarge_voltage_v")


def set_bulk_voltage(value: str) -> int:
    val = normalize_voltage_arg(value)
    write_val = normalize_voltage_write_arg(value)
    return set_verified_value("set_bulk_voltage", val, f"PCVV{write_val}", "bulk_voltage_v")


def set_float_voltage(value: str) -> int:
    val = normalize_voltage_arg(value)
    write_val = normalize_voltage_write_arg(value)
    return set_verified_value("set_float_voltage", val, f"PBFT{write_val}", "float_voltage_v")


def main() -> int:
    parser = argparse.ArgumentParser(description="WR1 robust safe controller")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_out = sub.add_parser("set-output-priority", help="Set WR1 output priority")
    p_out.add_argument("value", choices=sorted(SAFE_OUTPUT_PRIORITY.keys()))

    p_chg = sub.add_parser("set-charger-priority", help="Set WR1 charger priority")
    p_chg.add_argument("value", choices=sorted(SAFE_CHARGER_PRIORITY.keys()))

    p_recharge = sub.add_parser("set-recharge-voltage", help="Set WR1 recharge voltage")
    p_recharge.add_argument("value")

    p_redischarge = sub.add_parser("set-redischarge-voltage", help="Set WR1 redischarge voltage")
    p_redischarge.add_argument("value")

    p_bulk = sub.add_parser("set-bulk-voltage", help="Set WR1 bulk voltage")
    p_bulk.add_argument("value")

    p_float = sub.add_parser("set-float-voltage", help="Set WR1 float voltage")
    p_float.add_argument("value")

    args = parser.parse_args()

    if args.cmd == "set-output-priority":
        return set_output_priority(args.value)
    if args.cmd == "set-charger-priority":
        return set_charger_priority(args.value)
    if args.cmd == "set-recharge-voltage":
        return set_recharge_voltage(args.value)
    if args.cmd == "set-redischarge-voltage":
        return set_redischarge_voltage(args.value)
    if args.cmd == "set-bulk-voltage":
        return set_bulk_voltage(args.value)
    if args.cmd == "set-float-voltage":
        return set_float_voltage(args.value)

    return 1


if __name__ == "__main__":
    sys.exit(main())

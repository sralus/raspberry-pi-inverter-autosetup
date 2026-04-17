#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


DEVICE = "/dev/WR2"
SERVICE = "wr2-reader.service"
OVERRIDE_FILE = os.environ.get("OVERRIDE_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "wr2_ui_state_override.json"))

STOP_WAIT_SEC = 2.0
START_WAIT_SEC = 3.0


@dataclass
class CmdResult:
    command: str
    kind: str
    data: Optional[str]
    raw_hex: str
    raw_len: int


SAFE_FLAG_KEYS = {"F"}

FLAG_WRITE_COMMANDS = {
    "F": {
        "1": "^S006PEFL",
        "0": "^S006PDFL",
    }
}


def log(msg: str) -> None:
    print(msg, flush=True)


def run_cmd(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def stop_reader() -> None:
    log(f"== stop {SERVICE} ==")
    run_cmd(["sudo", "systemctl", "stop", SERVICE])
    time.sleep(STOP_WAIT_SEC)


def start_reader() -> None:
    log(f"== start {SERVICE} ==")
    run_cmd(["sudo", "systemctl", "start", SERVICE])
    time.sleep(START_WAIT_SEC)


def status_reader() -> None:
    log(f"== status {SERVICE} ==")
    cp = run_cmd(["systemctl", "--no-pager", "-l", "--full", "status", SERVICE], check=False)
    print(cp.stdout.rstrip())
    if cp.stderr.strip():
        print(cp.stderr.rstrip())


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
        elif b == 0x00:
            out.append(0x01)
        else:
            out.append(b)
    return bytes(out)


def build_infini_frame(cmd: str) -> bytes:
    payload = cmd.encode("ascii")
    crc = crc_xmodem(payload)
    crc_bytes = bytes([(crc >> 8) & 0xFF, crc & 0xFF])
    crc_bytes = adapt_crc_bytes(crc_bytes)
    return payload + crc_bytes + b"\x0D"


def write_chunked(fd: int, payload: bytes) -> None:
    plen = len(payload)

    if plen <= 5:
        chunks = [payload]
        delay = 0.010
    elif plen <= 10:
        chunks = [payload[0:4], payload[4:]]
        delay = 0.010
    elif plen <= 15:
        chunks = [payload[0:4], payload[4:8], payload[8:]]
        delay = 0.005
    else:
        # Keine 1-Byte- oder sonstigen Mini-Restchunks an hidraw schicken.
        chunks = []
        pos = 0
        while (plen - pos) > 6:
            chunks.append(payload[pos:pos+4])
            pos += 4
        if pos < plen:
            chunks.append(payload[pos:])
        delay = 0.010

    for chunk in chunks:
        if chunk:
            os.write(fd, chunk)
            time.sleep(delay)


def drain(fd: int, seconds: float = 0.35) -> bytes:
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


def read_infini_response(fd: int, timeout: float = 6.5) -> bytes:
    end = time.time() + timeout
    buf = b""

    while time.time() < end:
        try:
            chunk = os.read(fd, 4096)
            if chunk:
                buf += chunk
            else:
                time.sleep(0.03)
        except BlockingIOError:
            time.sleep(0.03)

        if buf.startswith(b"^0"):
            return buf
        if buf.startswith(b"^1"):
            return buf

        if buf.startswith(b"^D") and len(buf) >= 5:
            length_txt = buf[2:5]
            if all(48 <= c <= 57 for c in length_txt):
                total_len = int(length_txt.decode("ascii", errors="ignore"))
                if len(buf) > total_len:
                    return buf

    return buf


def read_until_quiet(fd: int, timeout: float = 2.4, settle: float = 0.25) -> bytes:
    end = time.time() + timeout
    buf = b""
    last_rx = None

    while time.time() < end:
        try:
            chunk = os.read(fd, 4096)
            if chunk:
                buf += chunk
                last_rx = time.time()
            else:
                time.sleep(0.03)
        except BlockingIOError:
            time.sleep(0.03)
        except OSError:
            break

        if last_rx is not None and (time.time() - last_rx) >= settle:
            break

    return buf


def decode_infini_answer(raw: bytes) -> CmdResult:
    raw_clean = raw.rstrip(b"\x00")

    if raw_clean.startswith(b"^0"):
        return CmdResult("", "NAK", None, raw.hex(), len(raw))

    if raw_clean.startswith(b"^1"):
        return CmdResult("", "ACK", None, raw.hex(), len(raw))

    if raw_clean.startswith(b"^D") and len(raw_clean) >= 5:
        length_txt = raw_clean[2:5]
        if all(48 <= c <= 57 for c in length_txt):
            total_len = int(length_txt.decode("ascii", errors="ignore"))
            data_len = total_len - 3
            data = raw_clean[5:5 + data_len]
            return CmdResult(
                "",
                "DATA",
                data.decode("ascii", errors="replace"),
                raw.hex(),
                len(raw),
            )

    return CmdResult("", "UNKNOWN", raw_clean.decode("ascii", errors="replace"), raw.hex(), len(raw))


def decode_raw_flag_answer(raw: bytes) -> CmdResult:
    raw_clean = raw.rstrip(b"\x00")
    text = raw_clean.decode("ascii", errors="replace")

    if b"^0" in raw_clean:
        return CmdResult("", "ACK", text, raw.hex(), len(raw))
    if b"^1" in raw_clean:
        return CmdResult("", "ACK", text, raw.hex(), len(raw))

    return CmdResult("", "RAW", text, raw.hex(), len(raw))


def send_cmd(fd: int, cmd: str) -> CmdResult:
    drain(fd, 0.30)
    frame = build_infini_frame(cmd)
    payload = frame[:-3]
    trailer = frame[-3:]

    write_chunked(fd, payload)
    time.sleep(0.015)
    os.write(fd, trailer)
    time.sleep(0.030)

    raw = read_infini_response(fd, timeout=6.5)
    res = decode_infini_answer(raw)
    res.command = cmd
    return res


def send_raw_ascii_single(fd: int, cmd: str, timeout: float = 2.4) -> CmdResult:
    drain(fd, 0.35)
    raw_cmd = cmd.encode("ascii", errors="strict") + b"\x0D"
    os.write(fd, raw_cmd)
    time.sleep(0.25)
    raw = read_until_quiet(fd, timeout=timeout, settle=0.25)
    res = decode_raw_flag_answer(raw)
    res.command = cmd
    return res


def send_cmd_debug_raw(fd: int, cmd: str, timeout: float = 8.0) -> bytes:
    drain(fd, 0.30)
    frame = build_infini_frame(cmd)
    payload = frame[:-3]
    trailer = frame[-3:]

    write_chunked(fd, payload)
    time.sleep(0.015)
    os.write(fd, trailer)
    time.sleep(0.030)

    end = time.time() + timeout
    buf = b""
    idle_rounds = 0

    while time.time() < end:
        got = False
        try:
            chunk = os.read(fd, 4096)
            if chunk:
                buf += chunk
                got = True
                idle_rounds = 0
            else:
                idle_rounds += 1
                time.sleep(0.05)
        except BlockingIOError:
            idle_rounds += 1
            time.sleep(0.05)
        except OSError:
            break

        if got:
            continue

        if buf and idle_rounds >= 6:
            break

    return buf


def send_expect_data(fd: int, cmd: str, retries: int = 3) -> CmdResult:
    last: Optional[CmdResult] = None
    for _ in range(retries):
        res = send_cmd(fd, cmd)
        last = res
        if res.kind == "DATA" and res.data is not None:
            return res
        time.sleep(0.40)

    if last is None:
        raise RuntimeError(f"{cmd}: no response")
    raise RuntimeError(f"{cmd}: unexpected response kind={last.kind} data={last.data!r}")


def send_expect_ack_or_data(fd: int, cmd: str, retries: int = 3) -> CmdResult:
    last: Optional[CmdResult] = None
    for _ in range(retries):
        res = send_cmd(fd, cmd)
        last = res
        if res.kind in {"ACK", "DATA"}:
            return res
        time.sleep(0.40)

    if last is None:
        raise RuntimeError(f"{cmd}: no response")
    raise RuntimeError(f"{cmd}: unexpected response kind={last.kind} data={last.data!r}")


def open_device(path: str) -> int:
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} existiert nicht")
    return os.open(path, os.O_RDWR | os.O_NONBLOCK)


def print_result(res: CmdResult) -> None:
    log(f"COMMAND={res.command}")
    log(f"KIND={res.kind}")
    log(f"RAW_LEN={res.raw_len}")
    log(f"RAW_HEX={res.raw_hex}")
    log(f"DATA={res.data!r}")


def read_status_block(fd: int) -> None:
    log("== read GS ==")
    print_result(send_expect_data(fd, "^P005GS"))
    print()
    log("== read MOD ==")
    print_result(send_expect_data(fd, "^P006MOD"))
    print()
    log("== read FWS ==")
    print_result(send_expect_data(fd, "^P006FWS"))
    print()
    log("== read FLAG ==")
    print_result(send_expect_data(fd, "^P007FLAG"))


def load_override() -> dict:
    if not os.path.exists(OVERRIDE_FILE):
        return {
            "priority": {
                "psp": {"code": None, "source": "missing"},
                "pcp": {"code": None, "source": "missing"},
                "pop": {"code": None, "source": "missing"},
            },
            "settings": {},
            "updated_at": None,
        }
    try:
        with open(OVERRIDE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("override not dict")
        data.setdefault("priority", {})
        for key in ("psp", "pcp", "pop"):
            data["priority"].setdefault(key, {"code": None, "source": "missing"})
            if not isinstance(data["priority"][key], dict):
                data["priority"][key] = {"code": None, "source": "missing"}
            data["priority"][key].setdefault("code", None)
            data["priority"][key].setdefault("source", "missing")
        data.setdefault("settings", {})
        data.setdefault("updated_at", None)
        return data
    except Exception:
        return {
            "priority": {
                "psp": {"code": None, "source": "invalid"},
                "pcp": {"code": None, "source": "invalid"},
                "pop": {"code": None, "source": "invalid"},
            },
            "settings": {},
            "updated_at": None,
        }


def atomic_write_json(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def update_override_priority(kind: str, value: str) -> None:
    mapping = {"psp": f"PSP{value}", "pcp": f"PCP{value}", "pop": f"POP{value}"}
    code = mapping[kind]
    data = load_override()
    data["priority"][kind]["code"] = code
    data["priority"][kind]["source"] = "wr2_ctl_success"
    data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    atomic_write_json(OVERRIDE_FILE, data)
    try:
        os.chmod(OVERRIDE_FILE, 0o664)
    except Exception:
        pass
    log(f"== override updated: {kind} -> {code} ==")


def update_override_settings(settings_update: dict) -> None:
    data = load_override()
    data.setdefault("settings", {})
    for key, value in settings_update.items():
        data["settings"][key] = value
    data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    atomic_write_json(OVERRIDE_FILE, data)
    try:
        os.chmod(OVERRIDE_FILE, 0o664)
    except Exception:
        pass
    log(f"== settings override updated: {settings_update} ==")


def v_to_tenths_str(v: str) -> str:
    value = float(v.replace(",", "."))
    iv = int(round(value * 10))
    if iv < 0 or iv > 999:
        raise ValueError("Spannungswert außerhalb 0.0..99.9 V")
    return f"{iv:03d}"


def set_pop(fd: int, value: str) -> None:
    if value not in {"0", "1"}:
        raise ValueError("POP erlaubt nur 0 oder 1")
    cmd = f"^S007POP{value}"
    log(f"== write POP -> {value} ==")
    res = send_expect_ack_or_data(fd, cmd)
    print_result(res)
    if res.kind in {"ACK", "DATA"}:
        update_override_priority("pop", value)


def set_psp(fd: int, value: str) -> None:
    if value not in {"0", "1", "2"}:
        raise ValueError("PSP erlaubt nur 0, 1 oder 2")
    cmd = f"^S007PSP{value}"
    log(f"== write PSP -> {value} ==")
    res = send_expect_ack_or_data(fd, cmd)
    print_result(res)
    if res.kind in {"ACK", "DATA"}:
        update_override_priority("psp", value)


def set_pcp(fd: int, value: str) -> None:
    if value not in {"0", "1", "2"}:
        raise ValueError("PCP erlaubt nur 0, 1 oder 2")
    cmd = f"^S009PCP0,{value}"
    log(f"== write PCP -> {value} ==")
    res = send_expect_ack_or_data(fd, cmd)
    print_result(res)
    if res.kind in {"ACK", "DATA"}:
        update_override_priority("pcp", value)


def set_bulk_float(fd: int, bulk_v: str, float_v: str) -> None:
    bulk = v_to_tenths_str(bulk_v)
    flt = v_to_tenths_str(float_v)
    cmd = f"^S015MCHGV{bulk},{flt}"
    log(f"== write BULK/FLOAT -> {bulk_v} / {float_v} ==")
    res = send_expect_ack_or_data(fd, cmd)
    print_result(res)
    if res.kind in {"ACK", "DATA"}:
        update_override_settings({
            "bulk_voltage_v": f"{float(bulk_v):.1f}",
            "floating_voltage_v": f"{float(float_v):.1f}",
            "bulk_float_source": "wr2_ctl_success",
        })


def set_bucd(fd: int, recharge_v: str, redischarge_v: str) -> None:
    rec = v_to_tenths_str(recharge_v)
    red = v_to_tenths_str(redischarge_v)
    cmd = f"^S014BUCD{rec},{red}"
    log(f"== write BUCD -> {recharge_v} / {redischarge_v} ==")
    res = send_expect_ack_or_data(fd, cmd)
    print_result(res)
    if res.kind in {"ACK", "DATA"}:
        update_override_settings({
            "battery_recharge_voltage_v": f"{float(recharge_v):.1f}",
            "battery_redischarge_voltage_v": f"{float(redischarge_v):.1f}",
            "bucd_source": "wr2_ctl_success",
        })


def set_psdv(fd: int, cutoff_v: str) -> None:
    cut = v_to_tenths_str(cutoff_v)
    cmd = f"^S010PSDV{cut}"
    log(f"== write PSDV -> {cutoff_v} ==")
    res = send_expect_ack_or_data(fd, cmd)
    print_result(res)
    if res.kind in {"ACK", "DATA"}:
        update_override_settings({
            "battery_cutoff_voltage_v": f"{float(cutoff_v):.1f}",
            "psdv_source": "wr2_ctl_success",
        })


def set_flag(fd: int, key: str, enabled: str) -> None:
    key = key.upper()
    if key not in SAFE_FLAG_KEYS:
        raise ValueError(f"Flag {key} ist nicht freigegeben")
    if enabled not in {"0", "1"}:
        raise ValueError("enabled erlaubt nur 0 oder 1")
    cmd = FLAG_WRITE_COMMANDS[key][enabled]
    log(f"== write FLAG {key} -> {enabled} ==")
    log(f"== real FLAG CMD (raw) : {cmd} ==")

    last = None
    for attempt in range(1, 4):
        log(f"== FLAG raw attempt {attempt}/3 ==")
        res = send_raw_ascii_single(fd, cmd, timeout=2.4)
        print_result(res)
        last = res
        if res.kind == "ACK":
            return
        if attempt < 3:
            time.sleep(0.45)

    last_kind = last.kind if last is not None else "NONE"
    last_data = last.data if last is not None else None
    raise RuntimeError(f"FLAG {key} -> {enabled}: keine ACK-Bestätigung erkannt; last_kind={last_kind} last_data={last_data!r}")


def do_status() -> int:
    status_reader()
    return 0


def do_read_debug_raw(command: str) -> int:
    fd = None
    try:
        stop_reader()
        fd = open_device(DEVICE)
        time.sleep(0.20)

        cmd_map = {
            "read-qdi-debug": "QDI",
        }

        cmd = cmd_map[command]
        raw = send_cmd_debug_raw(fd, cmd, timeout=8.0)

        log(f"COMMAND={cmd}")
        log(f"RAW_LEN={len(raw)}")
        log(f"RAW_HEX={raw.hex()}")
        try:
            log(f"RAW_ASCII={raw.decode('ascii', errors='replace')!r}")
        except Exception as e:
            log(f"RAW_ASCII_DECODE_ERROR={e}")

        res = decode_infini_answer(raw)
        res.command = cmd
        log(f"DECODE_KIND={res.kind}")
        log(f"DECODE_DATA={res.data!r}")
        return 0
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        start_reader()


def do_read_any_debug(cmd: str) -> int:
    fd = None
    try:
        stop_reader()
        fd = open_device(DEVICE)
        time.sleep(0.20)

        raw = send_cmd_debug_raw(fd, cmd, timeout=8.0)

        log(f"COMMAND={cmd}")
        log(f"RAW_LEN={len(raw)}")
        log(f"RAW_HEX={raw.hex()}")
        try:
            log(f"RAW_ASCII={raw.decode('ascii', errors='replace')!r}")
        except Exception as e:
            log(f"RAW_ASCII_DECODE_ERROR={e}")

        res = decode_infini_answer(raw)
        res.command = cmd
        log(f"DECODE_KIND={res.kind}")
        log(f"DECODE_DATA={res.data!r}")
        return 0
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        start_reader()


def do_read_single(command: str) -> int:
    fd = None
    try:
        stop_reader()
        fd = open_device(DEVICE)
        time.sleep(0.20)

        cmd_map = {
            "read-gs": "^P005GS",
            "read-mod": "^P006MOD",
            "read-fws": "^P006FWS",
            "read-pop": "^P007POP",
            "read-psp": "^P007PSP",
            "read-pcp": "^P009PCP",
            "read-pop-short": "POP",
            "read-pcp-short": "PCP",
            "read-qflag": "QFLAG",
            "read-qdi": "QDI",
        }
        res = send_expect_data(fd, cmd_map[command])
        print_result(res)
        return 0
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        start_reader()


def do_set(kind: str, value: str) -> int:
    fd = None
    try:
        stop_reader()
        fd = open_device(DEVICE)
        time.sleep(0.20)

        if kind == "pop":
            set_pop(fd, value)
        elif kind == "psp":
            set_psp(fd, value)
        elif kind == "pcp":
            set_pcp(fd, value)
        else:
            raise RuntimeError(f"Unbekannter Set-Typ: {kind}")

        return 0
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        start_reader()


def do_set_bulk_float(bulk_v: str, float_v: str) -> int:
    fd = None
    try:
        stop_reader()
        fd = open_device(DEVICE)
        time.sleep(0.20)
        set_bulk_float(fd, bulk_v, float_v)
        return 0
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        start_reader()


def do_set_bucd(recharge_v: str, redischarge_v: str) -> int:
    fd = None
    try:
        stop_reader()
        fd = open_device(DEVICE)
        time.sleep(0.20)
        set_bucd(fd, recharge_v, redischarge_v)
        return 0
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        start_reader()


def do_set_psdv(cutoff_v: str) -> int:
    fd = None
    try:
        stop_reader()
        fd = open_device(DEVICE)
        time.sleep(0.20)
        set_psdv(fd, cutoff_v)
        return 0
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        start_reader()


def do_set_flag(key: str, enabled: str) -> int:
    fd = None
    try:
        stop_reader()
        fd = open_device(DEVICE)
        time.sleep(0.20)

        set_flag(fd, key, enabled)
        return 0
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        start_reader()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WR2 Infini/IGrid Steuerung über /dev/WR2")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status", help="Service-Status anzeigen")
    sub.add_parser("read-gs", help="GS lesen")
    sub.add_parser("read-mod", help="MOD lesen")
    sub.add_parser("read-fws", help="FWS lesen")
    sub.add_parser("read-pop", help="POP lesen mit ^P007POP")
    sub.add_parser("read-psp", help="PSP lesen mit ^P007PSP")
    sub.add_parser("read-pcp", help="PCP lesen mit ^P009PCP")
    sub.add_parser("read-pop-short", help="POP lesen mit kurzem PI18-Befehl POP")
    sub.add_parser("read-pcp-short", help="PCP lesen mit kurzem PI18-Befehl PCP")
    sub.add_parser("read-qflag", help="Flags lesen mit kurzem PI18-Befehl QFLAG")
    sub.add_parser("read-qdi", help="QDI lesen")
    sub.add_parser("read-qdi-debug", help="QDI roh/debug lesen")
    p_any = sub.add_parser("read-any-debug", help="beliebigen Befehl roh/debug lesen")
    p_any.add_argument("command")

    p_pop = sub.add_parser("set-pop", help="POP setzen")
    p_pop.add_argument("value", choices=["0", "1"])

    p_psp = sub.add_parser("set-psp", help="PSP setzen")
    p_psp.add_argument("value", choices=["0", "1", "2"])

    p_pcp = sub.add_parser("set-pcp", help="PCP setzen")
    p_pcp.add_argument("value", choices=["0", "1", "2"])

    p_bf = sub.add_parser("set-bulk-float", help="Bulk + Float setzen")
    p_bf.add_argument("bulk_v")
    p_bf.add_argument("float_v")

    p_bucd = sub.add_parser("set-bucd", help="Recharge + Redischarge setzen")
    p_bucd.add_argument("recharge_v")
    p_bucd.add_argument("redischarge_v")

    p_psdv = sub.add_parser("set-psdv", help="Battery cut-off voltage setzen")
    p_psdv.add_argument("cutoff_v")

    p_flag = sub.add_parser("set-flag", help="harmlosen Flag setzen")
    p_flag.add_argument("key", choices=sorted(SAFE_FLAG_KEYS))
    p_flag.add_argument("enabled", choices=["0", "1"])

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        if args.cmd == "status":
            return do_status()
        if args.cmd in {
            "read-gs", "read-mod", "read-fws",
            "read-pop", "read-psp", "read-pcp",
            "read-pop-short", "read-pcp-short", "read-qflag",
            "read-qdi"
        }:
            return do_read_single(args.cmd)
        if args.cmd == "read-qdi-debug":
            return do_read_debug_raw(args.cmd)
        if args.cmd == "read-any-debug":
            return do_read_any_debug(args.command)
        if args.cmd == "set-pop":
            return do_set("pop", args.value)
        if args.cmd == "set-psp":
            return do_set("psp", args.value)
        if args.cmd == "set-pcp":
            return do_set("pcp", args.value)
        if args.cmd == "set-bulk-float":
            return do_set_bulk_float(args.bulk_v, args.float_v)
        if args.cmd == "set-bucd":
            return do_set_bucd(args.recharge_v, args.redischarge_v)
        if args.cmd == "set-psdv":
            return do_set_psdv(args.cutoff_v)
        if args.cmd == "set-flag":
            return do_set_flag(args.key, args.enabled)
        raise RuntimeError(f"Unbekannter Befehl: {args.cmd}")
    except subprocess.CalledProcessError as e:
        if e.stdout:
            print(e.stdout.rstrip(), file=sys.stderr)
        if e.stderr:
            print(e.stderr.rstrip(), file=sys.stderr)
        print(f"Fehler beim Systemkommando: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Fehler: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

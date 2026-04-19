#!/usr/bin/env python3
import os
import sys
import time
import json
import signal
import errno
import urllib.request
import urllib.parse
from datetime import datetime
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt

BROKER_HOST = os.environ.get("BROKER_HOST", "192.168.0.69")
BROKER_PORT = int(os.environ.get("BROKER_PORT", "1883"))
BROKER_USERNAME = os.environ.get("BROKER_USERNAME", "")
BROKER_PASSWORD = os.environ.get("BROKER_PASSWORD", "")
MQTT_ENABLED = os.environ.get("MQTT_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")

DEVICE_PATH = "/dev/WR1"
DEVICE_NAME = "WR1"
TOPIC_ROOT = "wr/WR1/status"

MQTT_KEEPALIVE = 30
MQTT_QOS = 0
MQTT_RETAIN = True

POLL_INTERVAL_SEC = 10.0
STARTUP_DELAY_SEC = 1.0

READ_GAP_SEC = 0.60
REOPEN_DELAY_SEC = 1.50
ERROR_BACKOFF_SEC = 5.0
NAK_RETRY_COUNT = 2
QMOD_RETRY_COUNT = 2

LOG_BASE_DIR = "/home/pi/wr-logs"

INFLUX_ENABLED = True
INFLUX_HOST = "127.0.0.1"
INFLUX_PORT = 8086
INFLUX_USERNAME = ""
INFLUX_PASSWORD = ""
INFLUX_DB = "wr1"
INFLUX_TIMEOUT_SEC = 5.0

RUN = True
LAST_GOOD_MODE: Dict[str, str] = {
    "mode_code": "",
    "mode_text": "Unknown",
}

ENERGY_STATE_PATH = os.environ.get("ENERGY_STATE_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "wr1_energy_state.json"))
MAX_INTEGRATION_SEC = 30.0
MAX_STALE_WRITE_SEC = 90.0

LAST_GOOD_DATA: Optional[Dict[str, Any]] = None
LAST_GOOD_TS: float = 0.0


def handle_signal(signum, frame):
    global RUN
    RUN = False


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def ensure_log_dir() -> None:
    os.makedirs(LOG_BASE_DIR, exist_ok=True)


def latest_json_path(device_name: str) -> str:
    return os.path.join(LOG_BASE_DIR, f"{device_name}_latest.json")


def history_jsonl_path(device_name: str) -> str:
    return os.path.join(LOG_BASE_DIR, f"{device_name}_history.jsonl")


def last_error_path(device_name: str) -> str:
    return os.path.join(LOG_BASE_DIR, f"{device_name}_last_error.json")


def loop_log_path(device_name: str) -> str:
    return os.path.join(LOG_BASE_DIR, f"{device_name}_loop.log")


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def iso_to_epoch(value: Any) -> float:
    try:
        if not value:
            return 0.0
        return datetime.fromisoformat(str(value)).timestamp()
    except Exception:
        return 0.0


def load_last_good_snapshot(device_name: str) -> None:
    global LAST_GOOD_DATA, LAST_GOOD_TS

    try:
        with open(latest_json_path(device_name), "r", encoding="utf-8") as f:
            payload = json.load(f)
        data = payload.get("data")
        if isinstance(data, dict):
            LAST_GOOD_DATA = data
            LAST_GOOD_TS = iso_to_epoch(data.get("timestamp")) or iso_to_epoch(payload.get("saved_at")) or time.time()
    except Exception:
        LAST_GOOD_DATA = None
        LAST_GOOD_TS = 0.0


def mark_live_data(data: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(data)
    out["stale"] = False
    out["read_ok"] = True
    out["data_age_sec"] = 0.0
    out["stale_reason"] = ""
    return out


def build_stale_data(error_message: str) -> Optional[Dict[str, Any]]:
    global LAST_GOOD_DATA, LAST_GOOD_TS

    if not isinstance(LAST_GOOD_DATA, dict) or LAST_GOOD_TS <= 0:
        return None

    age = max(0.0, time.time() - LAST_GOOD_TS)
    if age > MAX_STALE_WRITE_SEC:
        return None

    stale = dict(LAST_GOOD_DATA)
    stale["timestamp"] = now_iso()
    stale["stale"] = True
    stale["read_ok"] = False
    stale["data_age_sec"] = round(age, 1)
    stale["stale_reason"] = str(error_message)
    return stale


def append_text_line(path: str, line: str) -> None:
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def write_json_atomic(path: str, data: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, path)


def append_jsonl(path: str, data: Dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")


def log(msg: str) -> None:
    line = f"[{now_iso()}] {msg}"
    print(line, flush=True)
    append_text_line(loop_log_path(DEVICE_NAME), line)


def save_success_snapshot(device_name: str, data: Dict[str, Any]) -> None:
    ensure_log_dir()
    payload = {
        "saved_at": now_iso(),
        "device_name": device_name,
        "kind": "success",
        "data": data,
    }
    write_json_atomic(latest_json_path(device_name), payload)
    append_jsonl(history_jsonl_path(device_name), payload)


def save_error_snapshot(device_name: str, message: str) -> None:
    ensure_log_dir()
    payload = {
        "saved_at": now_iso(),
        "device_name": device_name,
        "kind": "error",
        "error": message,
    }
    write_json_atomic(last_error_path(device_name), payload)
    append_jsonl(history_jsonl_path(device_name), payload)


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


def build_frame(cmd: str) -> bytes:
    payload = cmd.encode("ascii")
    crc = crc_xmodem(payload)
    return payload + bytes([(crc >> 8) & 0xFF, crc & 0xFF, 0x0D])


def try_float(value: str) -> Any:
    try:
        return float(value)
    except Exception:
        return value


def try_int(value: str) -> Any:
    try:
        return int(value)
    except Exception:
        return value


def sleep_abortable(seconds: float) -> None:
    end = time.time() + seconds
    while RUN and time.time() < end:
        time.sleep(0.1)


def drain_input(fd: int, drain_time: float = 0.30) -> bytes:
    end = time.time() + drain_time
    buf = b""
    while time.time() < end:
        try:
            chunk = os.read(fd, 512)
            if chunk:
                buf += chunk
            else:
                time.sleep(0.02)
        except BlockingIOError:
            time.sleep(0.02)
        except OSError:
            break
    return buf


def read_candidate_frame(fd: int, timeout: float = 3.0, max_bytes: int = 4096) -> bytes:
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


def decode_frame(raw_frame: bytes) -> Dict[str, Any]:
    raw = raw_frame.rstrip(b"\x00")
    had_cr = raw.endswith(b"\r")
    body = raw[:-1] if had_cr else raw

    crc_ok: Optional[bool] = None
    crc_hex: Optional[str] = None
    payload_bytes = body

    if len(body) >= 3:
        payload_bytes = body[:-2]
        rx_crc = body[-2:]
        calc_crc = crc_xmodem(payload_bytes)
        calc_crc_bytes = bytes([(calc_crc >> 8) & 0xFF, calc_crc & 0xFF])
        crc_ok = (rx_crc == calc_crc_bytes)
        crc_hex = rx_crc.hex()

    payload_text = payload_bytes.decode("ascii", errors="replace")
    if payload_text.startswith("("):
        payload_text = payload_text[1:]
    payload_text = payload_text.replace("\x00", "").strip()

    return {
        "raw_hex": raw_frame.hex(),
        "raw_len": len(raw_frame),
        "payload_text": payload_text,
        "crc_ok": crc_ok,
        "crc_hex": crc_hex,
        "had_cr": had_cr,
    }


def is_valid_qmod_payload(payload_text: str) -> bool:
    return len(payload_text) >= 1 and payload_text[:1] in {"P", "S", "L", "B", "F", "H"}


def is_plausible_response(cmd: str, payload_text: str) -> bool:
    if not payload_text:
        return False

    if payload_text == "NAK":
        return True

    if cmd == "QMOD":
        return is_valid_qmod_payload(payload_text)
    if cmd == "QPIGS":
        return payload_text.count(" ") >= 15 and payload_text[0].isdigit()

    return True


def send_cmd(fd: int, cmd: str, max_attempts: int = 3) -> Dict[str, Any]:
    pre_drain = drain_input(fd, drain_time=0.20)
    frame = build_frame(cmd)

    attempts = []
    chosen = None

    for attempt in range(1, max_attempts + 1):
        os.write(fd, frame)
        time.sleep(0.15)

        raw = read_candidate_frame(fd, timeout=3.0)
        decoded = decode_frame(raw)

        attempt_info = {
            "attempt": attempt,
            "raw_hex": decoded["raw_hex"],
            "raw_len": decoded["raw_len"],
            "payload_text": decoded["payload_text"],
            "crc_ok": decoded["crc_ok"],
            "crc_hex": decoded["crc_hex"],
            "had_cr": decoded["had_cr"],
        }
        attempts.append(attempt_info)

        payload_text = decoded["payload_text"]
        crc_ok = decoded["crc_ok"]

        if payload_text == "NAK":
            chosen = decoded
            break

        if crc_ok is True and is_plausible_response(cmd, payload_text):
            chosen = decoded
            break

        time.sleep(0.30)

    if chosen is None:
        chosen = attempts[-1] if attempts else {
            "raw_hex": "",
            "raw_len": 0,
            "payload_text": "",
            "crc_ok": None,
            "crc_hex": None,
            "had_cr": False,
        }

    return {
        "command": cmd,
        "tx_hex": frame.hex(),
        "pre_drain_hex": pre_drain.hex(),
        "attempts": attempts,
        "rx": chosen,
    }


def parse_qmod(text: str) -> Dict[str, Any]:
    mode_code = text[:1] if text else ""
    mode_map = {
        "P": "Power On",
        "S": "Standby",
        "L": "Line",
        "B": "Battery",
        "F": "Fault",
        "H": "Power Saving",
    }
    return {
        "mode_code": mode_code,
        "mode_text": mode_map.get(mode_code, "Unknown")
    }


def get_valid_mode_from_qmod(fd: int) -> Dict[str, str]:
    global LAST_GOOD_MODE

    last_error = None

    for _ in range(QMOD_RETRY_COUNT + 1):
        qmod = send_cmd(fd, "QMOD")
        qmod_rx = qmod["rx"]
        qmod_text = qmod_rx["payload_text"]

        if qmod_text != "NAK" and qmod_rx.get("crc_ok") is True and is_valid_qmod_payload(qmod_text):
            mode = parse_qmod(qmod_text)
            LAST_GOOD_MODE = {
                "mode_code": mode.get("mode_code", "") or "",
                "mode_text": mode.get("mode_text", "Unknown") or "Unknown",
            }
            return LAST_GOOD_MODE

        last_error = f"QMOD invalid: payload={qmod_text!r} crc_ok={qmod_rx.get('crc_ok')}"
        sleep_abortable(0.40)

    if LAST_GOOD_MODE.get("mode_code"):
        log(f"warn {last_error}; keep last good mode={LAST_GOOD_MODE.get('mode_text')}")
        return LAST_GOOD_MODE

    raise RuntimeError(last_error or "QMOD read failed")


def decode_device_status_bits(bits: str) -> Dict[str, Any]:
    bits = bits.strip()
    out = {"raw": bits}

    if len(bits) >= 8 and all(ch in "01" for ch in bits):
        padded = bits.ljust(8, "0")
        out.update({
            "sbu_priority_version": padded[0] == "1",
            "config_changed": padded[1] == "1",
            "scc_firmware_updated": padded[2] == "1",
            "load_on": padded[3] == "1",
            "battery_voltage_to_steady": padded[4] == "1",
            "charging": padded[5] == "1",
            "scc_charging": padded[6] == "1",
            "ac_charging": padded[7] == "1",
        })

    return out


def parse_qpigs(text: str) -> Dict[str, Any]:
    fields = text.split()
    result: Dict[str, Any] = {
        "raw_fields": fields,
        "field_count": len(fields),
    }

    names = [
        ("ac_grid_voltage_v", "float"),
        ("ac_grid_frequency_hz", "float"),
        ("ac_output_voltage_v", "float"),
        ("ac_output_frequency_hz", "float"),
        ("load_va", "int"),
        ("load_watt", "int"),
        ("load_percent", "int"),
        ("bus_voltage_v", "int"),
        ("battery_voltage_v", "float"),
        ("battery_charge_current_a", "int"),
        ("battery_capacity_percent", "int"),
        ("heatsink_temperature_c", "int"),
        ("pv_input_current_a", "float"),
        ("pv_input_voltage_v", "float"),
        ("scc_voltage_v", "float"),
        ("battery_discharge_current_a", "int"),
        ("device_status_bits", "str"),
        ("reserved_1", "str"),
        ("reserved_2", "str"),
        ("pv_input_power_w", "int"),
        ("reserved_3", "str"),
    ]

    for idx, (name, kind) in enumerate(names):
        if idx >= len(fields):
            break
        val = fields[idx]
        if kind == "float":
            result[name] = try_float(val)
        elif kind == "int":
            result[name] = try_int(val)
        else:
            result[name] = val

    bits = result.get("device_status_bits")
    if isinstance(bits, str):
        result["device_status"] = decode_device_status_bits(bits)

    return result


def parse_qpiri(text: str) -> Dict[str, Any]:
    fields = text.split()
    result: Dict[str, Any] = {
        "raw_fields": fields,
        "field_count": len(fields),
    }

    if len(fields) < 26:
        result["parse_error"] = f"too_few_fields:{len(fields)}"
        return result

    try:
        result.update({
            "grid_rating_voltage_v": try_float(fields[0]),
            "grid_rating_current_a": try_float(fields[1]),
            "ac_output_rating_voltage_v": try_float(fields[2]),
            "ac_output_rating_frequency_hz": try_float(fields[3]),
            "ac_output_rating_current_a": try_float(fields[4]),
            "ac_output_rating_apparent_power_va": try_int(fields[5]),
            "ac_output_rating_active_power_w": try_int(fields[6]),
            "battery_rating_voltage_v": try_float(fields[7]),
            "battery_recharge_voltage_v": try_float(fields[8]),
            "battery_cutoff_voltage_v": try_float(fields[9]),
            "bulk_voltage_v": try_float(fields[10]),
            "float_voltage_v": try_float(fields[11]),
            "battery_type_raw": fields[12],
            "max_ac_charge_current_a": try_int(fields[13]),
            "max_charge_current_a": try_int(fields[14]),
            "input_voltage_range_raw": fields[15],
            "output_source_priority_raw": fields[16],
            "charger_source_priority_raw": fields[17],
            "parallel_max_num_raw": fields[18],
            "machine_type_raw": fields[19],
            "topology_raw": fields[20],
            "output_mode_raw": fields[21],
            "battery_redischarge_voltage_v": try_float(fields[22]),
            "pv_ok_parallel_raw": fields[23],
            "pv_power_balance_raw": fields[24],
            "reserved_tail_raw": fields[25],
        })
    except Exception as e:
        result["parse_error"] = str(e)

    return result


def parse_qpiws(text: str) -> Dict[str, Any]:
    bits = str(text).strip()
    out: Dict[str, Any] = {
        "raw": bits,
        "all_zero": (bits != "" and set(bits) == {"0"}),
        "bit_count": len(bits),
    }
    if bits and all(ch in "01" for ch in bits):
        out["bits"] = list(bits)
    return out


def parse_qflag(text: str) -> Dict[str, Any]:
    return {
        "raw": str(text).strip(),
    }


def load_energy_state() -> Dict[str, Any]:
    try:
        with open(ENERGY_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        return data
    except Exception:
        return {}


def save_energy_state(state: Dict[str, Any]) -> None:
    tmp = ENERGY_STATE_PATH + ".tmp"
    os.makedirs(os.path.dirname(ENERGY_STATE_PATH), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, ENERGY_STATE_PATH)


def today_key() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def update_daily_wh(pv_power_w: Any) -> float:
    power_w = 0.0
    try:
        if pv_power_w is not None:
            power_w = max(0.0, float(pv_power_w))
    except Exception:
        power_w = 0.0

    now_ts = time.time()
    day = today_key()

    state = load_energy_state()
    state_day = str(state.get("day", ""))
    state_wh = float(state.get("wh_today", 0.0) or 0.0)
    state_ts = float(state.get("last_ts", 0.0) or 0.0)

    if state_day != day:
        state_wh = 0.0
        state_ts = now_ts

    if state_ts > 0:
        dt = now_ts - state_ts
        if 0 < dt <= MAX_INTEGRATION_SEC:
            state_wh += (power_w * dt) / 3600.0

    state = {
        "day": day,
        "wh_today": round(state_wh, 3),
        "last_ts": now_ts,
        "last_power_w": power_w,
        "saved_at": now_iso(),
    }
    save_energy_state(state)
    return round(state_wh, 3)


def read_optional_query(fd: int, cmd: str, parser=None, retries: int = 1) -> Dict[str, Any]:
    last_error = None

    for _ in range(retries + 1):
        rx = send_cmd(fd, cmd)
        rx_data = rx["rx"]
        payload_text = rx_data.get("payload_text", "")
        crc_ok = rx_data.get("crc_ok")

        if payload_text != "NAK" and crc_ok is True and payload_text != "":
            parsed = parser(payload_text) if parser else {"raw": payload_text}
            return {
                "ok": True,
                "command": cmd,
                "payload_text": payload_text,
                "parsed": parsed,
            }

        last_error = f"{cmd} invalid: payload={payload_text!r} crc_ok={crc_ok}"
        sleep_abortable(0.20)

    return {
        "ok": False,
        "command": cmd,
        "payload_text": "",
        "parsed": {},
        "error": last_error or f"{cmd} failed",
    }


def read_cycle_once(dev: str, device_name: str) -> Dict[str, Any]:
    fd = None
    try:
        fd = os.open(dev, os.O_RDWR | os.O_NONBLOCK)
        sleep_abortable(0.20)

        mode = get_valid_mode_from_qmod(fd)
        sleep_abortable(READ_GAP_SEC)

        qpigs = None
        last_error = None

        for _ in range(NAK_RETRY_COUNT + 1):
            qpigs = send_cmd(fd, "QPIGS")
            qpigs_rx = qpigs["rx"]
            qpigs_text = qpigs_rx["payload_text"]

            if qpigs_text != "NAK" and qpigs_rx.get("crc_ok") is True:
                break

            last_error = f"QPIGS invalid: payload={qpigs_text!r} crc_ok={qpigs_rx.get('crc_ok')}"
            sleep_abortable(0.80)

        if qpigs is None:
            raise RuntimeError("QPIGS read failed: no response object")

        qpigs_rx = qpigs["rx"]
        qpigs_text = qpigs_rx["payload_text"]

        if qpigs_text == "NAK" or qpigs_rx.get("crc_ok") is not True:
            raise RuntimeError(last_error or f"QPIGS invalid: payload={qpigs_text!r} crc_ok={qpigs_rx.get('crc_ok')}")

        general = parse_qpigs(qpigs_text)
        wh_today = update_daily_wh(general.get("pv_input_power_w"))

        sleep_abortable(0.15)
        qid = read_optional_query(fd, "QID", parser=lambda t: {"device_serial": str(t).strip()}, retries=0)

        sleep_abortable(0.15)
        qpiri = read_optional_query(fd, "QPIRI", parser=parse_qpiri, retries=0)

        sleep_abortable(0.15)
        qpiws = read_optional_query(fd, "QPIWS", parser=parse_qpiws, retries=0)

        sleep_abortable(0.15)
        qflag = read_optional_query(fd, "QFLAG", parser=parse_qflag, retries=0)

        out = {
            "timestamp": now_iso(),
            "device_name": device_name,
            "device_path": dev,
            "ok": True,
            "protocol_id": "PI30",
            "mode_code": mode.get("mode_code"),
            "mode_text": mode.get("mode_text"),
            "ac_grid_voltage_v": general.get("ac_grid_voltage_v"),
            "ac_grid_frequency_hz": general.get("ac_grid_frequency_hz"),
            "ac_output_voltage_v": general.get("ac_output_voltage_v"),
            "ac_output_frequency_hz": general.get("ac_output_frequency_hz"),
            "load_va": general.get("load_va"),
            "load_watt": general.get("load_watt"),
            "load_percent": general.get("load_percent"),
            "bus_voltage_v": general.get("bus_voltage_v"),
            "battery_voltage_v": general.get("battery_voltage_v"),
            "battery_charge_current_a": general.get("battery_charge_current_a"),
            "battery_capacity_percent": general.get("battery_capacity_percent"),
            "heatsink_temperature_c": general.get("heatsink_temperature_c"),
            "pv_input_current_a": general.get("pv_input_current_a"),
            "pv_input_voltage_v": general.get("pv_input_voltage_v"),
            "scc_voltage_v": general.get("scc_voltage_v"),
            "battery_discharge_current_a": general.get("battery_discharge_current_a"),
            "pv_input_power_w": general.get("pv_input_power_w"),
            "wh_today": wh_today,
            "device_status_bits": general.get("device_status_bits"),
            "device_status": general.get("device_status"),
            "field_count": general.get("field_count"),
        }

        if qid.get("ok"):
            qid_parsed = qid.get("parsed", {})
            out["device_serial"] = qid_parsed.get("device_serial")

        if qpiri.get("ok"):
            qpiri_parsed = qpiri.get("parsed", {})
            out["piri_raw"] = qpiri.get("payload_text")

            qpiri_key_map = {
                "battery_cutoff_voltage_v": "piri_battery_cutoff_voltage_v",
                "battery_recharge_voltage_v": "piri_battery_recharge_voltage_v",
                "battery_redischarge_voltage_v": "piri_battery_redischarge_voltage_v",
                "bulk_voltage_v": "piri_bulk_voltage_v",
                "float_voltage_v": "piri_float_voltage_v",
                "output_source_priority_raw": "piri_output_source_priority_raw",
                "charger_source_priority_raw": "piri_charger_source_priority_raw",
                "ac_output_rating_apparent_power_va": "piri_ac_output_rating_apparent_power_va",
                "ac_output_rating_active_power_w": "piri_ac_output_rating_active_power_w",
                "max_ac_charge_current_a": "piri_max_ac_charge_current_a",
                "max_charge_current_a": "piri_max_charge_current_a",
            }

            for src_key, dst_key in qpiri_key_map.items():
                if qpiri_parsed.get(src_key) is not None:
                    out[dst_key] = qpiri_parsed.get(src_key)

            direct_alias_map = {
                "battery_cutoff_voltage_v": "battery_cutoff_voltage_v",
                "battery_recharge_voltage_v": "battery_recharge_voltage_v",
                "battery_redischarge_voltage_v": "battery_redischarge_voltage_v",
                "bulk_voltage_v": "battery_bulk_voltage_v",
                "float_voltage_v": "battery_float_voltage_v",
            }

            for src_key, dst_key in direct_alias_map.items():
                if qpiri_parsed.get(src_key) is not None:
                    out[dst_key] = qpiri_parsed.get(src_key)

            out["piri"] = qpiri_parsed
            for key, value in qpiri_parsed.items():
                if key not in ("raw_fields", "parse_error"):
                    out[f"piri_{key}"] = value
        else:
            out["piri_read_error"] = qpiri.get("error", "")

        if qpiws.get("ok"):
            qpiws_parsed = qpiws.get("parsed", {})
            out["warnings_raw"] = qpiws_parsed.get("raw", qpiws.get("payload_text"))
            out["warnings"] = qpiws_parsed
        else:
            out["qpiws_read_error"] = qpiws.get("error", "")

        if qflag.get("ok"):
            qflag_parsed = qflag.get("parsed", {})
            out["flags_raw"] = qflag_parsed.get("raw", qflag.get("payload_text"))
            out["flags"] = qflag_parsed
        else:
            out["qflag_read_error"] = qflag.get("error", "")

        return out
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass


def mqtt_connect() -> mqtt.Client:
    client = mqtt.Client(client_id=f"{DEVICE_NAME}_live_loop")
    if BROKER_USERNAME:
        client.username_pw_set(BROKER_USERNAME, BROKER_PASSWORD)
    client.connect(BROKER_HOST, BROKER_PORT, MQTT_KEEPALIVE)
    client.loop_start()
    for _ in range(20):
        try:
            if client.is_connected():
                break
        except Exception:
            pass
        time.sleep(0.2)
    return client


def publish_value(client: mqtt.Client, topic: str, value: Any) -> None:
    if isinstance(value, bool):
        payload = "true" if value else "false"
    elif value is None:
        payload = ""
    elif isinstance(value, (dict, list)):
        payload = json.dumps(value, ensure_ascii=False)
    else:
        payload = str(value)

    try:
        try:
            connected = client.is_connected()
        except Exception:
            connected = True

        if not connected:
            try:
                client.reconnect()
                time.sleep(0.2)
            except Exception as re:
                log(f"mqtt reconnect failed topic={topic} reason={re}")
                return

        info = client.publish(topic, payload=payload, qos=MQTT_QOS, retain=MQTT_RETAIN)
        try:
            info.wait_for_publish(timeout=2.0)
        except TypeError:
            info.wait_for_publish()
    except Exception as e:
        log(f"mqtt publish skipped topic={topic} reason={e}")


def publish_payload(client: mqtt.Client, topic_root: str, data: Dict[str, Any]) -> None:
    publish_value(client, f"{topic_root}/json", data)
    publish_value(client, f"{topic_root}/online", True)
    publish_value(client, f"{topic_root}/last_update", data.get("timestamp"))
    publish_value(client, f"{topic_root}/error", data.get("stale_reason", "") if data.get("stale") else "")

    scalar_keys = [
        "ok",
        "protocol_id",
        "mode_code",
        "mode_text",
        "ac_grid_voltage_v",
        "ac_grid_frequency_hz",
        "ac_output_voltage_v",
        "ac_output_frequency_hz",
        "load_va",
        "load_watt",
        "load_percent",
        "bus_voltage_v",
        "battery_voltage_v",
        "battery_charge_current_a",
        "battery_capacity_percent",
        "heatsink_temperature_c",
        "pv_input_current_a",
        "pv_input_voltage_v",
        "scc_voltage_v",
        "battery_discharge_current_a",
        "pv_input_power_w",
        "wh_today",
        "stale",
        "read_ok",
        "data_age_sec",
        "device_status_bits",
        "field_count",
        "device_serial",
        "warnings_raw",
        "flags_raw",
        "piri_grid_rating_voltage_v",
        "piri_grid_rating_current_a",
        "piri_ac_output_rating_voltage_v",
        "piri_ac_output_rating_frequency_hz",
        "piri_ac_output_rating_current_a",
        "piri_ac_output_rating_apparent_power_va",
        "piri_ac_output_rating_active_power_w",
        "piri_battery_rating_voltage_v",
        "piri_battery_cutoff_voltage_v",
        "piri_battery_recharge_voltage_v",
        "piri_bulk_voltage_v",
        "piri_float_voltage_v",
        "piri_battery_type_raw",
        "piri_max_ac_charge_current_a",
        "piri_max_charge_current_a",
        "piri_input_voltage_range_raw",
        "piri_output_source_priority_raw",
        "piri_charger_source_priority_raw",
        "piri_parallel_max_num_raw",
        "piri_machine_type_raw",
        "piri_topology_raw",
        "piri_output_mode_raw",
        "piri_battery_redischarge_voltage_v",
        "piri_pv_ok_parallel_raw",
        "piri_pv_power_balance_raw",
        "piri_reserved_tail_raw",
    ]

    for key in scalar_keys:
        publish_value(client, f"{topic_root}/{key}", data.get(key))

    device_status = data.get("device_status")
    if isinstance(device_status, dict):
        for key, value in device_status.items():
            publish_value(client, f"{topic_root}/device_status/{key}", value)

    warnings = data.get("warnings")
    if isinstance(warnings, dict):
        for key, value in warnings.items():
            publish_value(client, f"{topic_root}/warnings/{key}", value)

    flags = data.get("flags")
    if isinstance(flags, dict):
        for key, value in flags.items():
            publish_value(client, f"{topic_root}/flags/{key}", value)

    piri = data.get("piri")
    if isinstance(piri, dict):
        for key, value in piri.items():
            if key == "raw_fields":
                continue
            publish_value(client, f"{topic_root}/piri/{key}", value)


def publish_error(client: mqtt.Client, message: str) -> None:
    publish_value(client, f"{TOPIC_ROOT}/online", False)
    publish_value(client, f"{TOPIC_ROOT}/error", message)
    publish_value(client, f"{TOPIC_ROOT}/last_update", now_iso())


def is_broken_pipe_error(exc: Exception) -> bool:
    if isinstance(exc, BrokenPipeError):
        return True
    if isinstance(exc, OSError) and exc.errno == errno.EPIPE:
        return True
    return False


def influx_escape_string(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def influx_line(measurement: str, fields: Dict[str, Any]) -> str:
    parts = []
    for key, value in fields.items():
        if isinstance(value, bool):
            parts.append('%s=%s' % (key, "true" if value else "false"))
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            parts.append('%s=%s' % (key, float(value)))
        else:
            parts.append('%s="%s"' % (key, influx_escape_string(value)))
    return measurement + " " + ",".join(parts)


def influx_request(url: str, payload: Optional[bytes] = None) -> bytes:
    req = urllib.request.Request(url, data=payload)
    with urllib.request.urlopen(req, timeout=INFLUX_TIMEOUT_SEC) as resp:
        return resp.read()


def influx_auth_qs() -> str:
    params = {}
    if INFLUX_USERNAME:
        params["u"] = INFLUX_USERNAME
    if INFLUX_PASSWORD:
        params["p"] = INFLUX_PASSWORD
    if not params:
        return ""
    return "&" + urllib.parse.urlencode(params)


def ensure_influx_db(db_name: str) -> None:
    qs = urllib.parse.urlencode({"q": 'CREATE DATABASE "%s"' % db_name})
    url = "http://%s:%s/query?%s%s" % (INFLUX_HOST, INFLUX_PORT, qs, influx_auth_qs())
    influx_request(url)


def wr1_device_status_value(data: Dict[str, Any]) -> float:
    mode = str(data.get("mode_code", "")).strip().upper()
    mapping = {
        "B": 3.0,
        "L": 0.0,
        "S": 5.0,
    }
    return mapping.get(mode, 0.0)


def wr1_ladestatus_value(data: Dict[str, Any]) -> float:
    scc = bool(data.get("device_status", {}).get("scc_charging", False))
    ac = bool(data.get("device_status", {}).get("ac_charging", False))
    return 1.0 if (scc or ac) else 0.0


def build_influx_lines_wr1(data: Dict[str, Any]) -> list:
    pv = {
        "Leistung": data.get("pv_input_power_w", 0) or 0,
        "Leistung1": data.get("pv_input_power_w", 0) or 0,
        "Leistung2": 0.0,
        "Spannung": data.get("pv_input_voltage_v", 0) or 0,
        "Spannung1": data.get("pv_input_voltage_v", 0) or 0,
        "Spannung2": 0.0,
        "Strom": data.get("pv_input_current_a", 0) or 0,
        "Strom1": data.get("pv_input_current_a", 0) or 0,
        "Strom2": 0.0,
    }

    ac = {
        "Ausgangslast": data.get("load_percent", 0) or 0,
        "Frequenz": data.get("ac_output_frequency_hz", 0) or 0,
        "Scheinleistung": data.get("load_va", 0) or 0,
        "Spannung": data.get("ac_output_voltage_v", 0) or 0,
        "Wirkleistung": data.get("load_watt", 0) or 0,
    }

    batterie = {
        "Entladestrom": data.get("battery_discharge_current_a", 0) or 0,
        "Kapazitaet": data.get("battery_capacity_percent", 0) or 0,
        "Ladestrom": data.get("battery_charge_current_a", 0) or 0,
        "Spannung": data.get("battery_voltage_v", 0) or 0,
        "Spannung_WR": data.get("battery_voltage_v", 0) or 0,
    }

    service = {
        "Device_Status": wr1_device_status_value(data),
        "Fehlercode": 0.0,
        "Fehlermeldung": "",
        "Ladestatus": wr1_ladestatus_value(data),
        "Modus": data.get("mode_text", "") or "",
        "OutputMode": 0.0,
        "Temperatur": data.get("heatsink_temperature_c", 0) or 0,
        "Warnungen": 0.0,
        "Stale": 1.0 if data.get("stale") else 0.0,
        "Read_OK": 1.0 if data.get("read_ok", True) else 0.0,
        "DataAgeSec": data.get("data_age_sec", 0) or 0,
    }

    netz = {
        "Spannung": data.get("ac_grid_voltage_v", 0) or 0,
        "Frequenz": data.get("ac_grid_frequency_hz", 0) or 0,
    }

    summen = {
        "Wh_Heute": data.get("wh_today", 0) or 0,
    }

    return [
        influx_line("PV", pv),
        influx_line("AC", ac),
        influx_line("Batterie", batterie),
        influx_line("Service", service),
        influx_line("Netz", netz),
        influx_line("Summen", summen),
    ]


def write_influx_wr1(data: Dict[str, Any]) -> None:
    if not INFLUX_ENABLED:
        return
    lines = build_influx_lines_wr1(data)
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    qs = urllib.parse.urlencode({"db": INFLUX_DB, "precision": "ns"})
    url = "http://%s:%s/write?%s%s" % (INFLUX_HOST, INFLUX_PORT, qs, influx_auth_qs())
    influx_request(url, payload=payload)


def main() -> int:
    dev = sys.argv[1] if len(sys.argv) > 1 else DEVICE_PATH
    device_name = sys.argv[2] if len(sys.argv) > 2 else DEVICE_NAME
    interval = float(sys.argv[3]) if len(sys.argv) > 3 else POLL_INTERVAL_SEC

    ensure_log_dir()
    if INFLUX_ENABLED:
        try:
            ensure_influx_db(INFLUX_DB)
        except Exception as e:
            log(f"influx init error {e}")

    log(f"start device={dev} name={device_name} mqtt_enabled={MQTT_ENABLED} broker={BROKER_HOST}:{BROKER_PORT} interval={interval}s")
    load_last_good_snapshot(device_name)

    client = mqtt_connect() if MQTT_ENABLED else None
    sleep_abortable(STARTUP_DELAY_SEC)

    try:
        while RUN:
            cycle_start = time.time()

            try:
                global LAST_GOOD_DATA, LAST_GOOD_TS
                data = mark_live_data(read_cycle_once(dev, device_name))
                LAST_GOOD_DATA = dict(data)
                LAST_GOOD_TS = time.time()
                publish_payload(client, TOPIC_ROOT, data)
                save_success_snapshot(device_name, data)
                write_influx_wr1(data)
                log(
                    "ok "
                    f"mode={data.get('mode_text')} "
                    f"bat={data.get('battery_voltage_v')}V "
                    f"load={data.get('load_watt')}W "
                    f"pv={data.get('pv_input_power_w')}W "
                    f"wh_today={data.get('wh_today')}"
                )
            except Exception as e:
                stale_data = build_stale_data(str(e))
                if stale_data is not None:
                    publish_payload(client, TOPIC_ROOT, stale_data)
                    write_influx_wr1(stale_data)
                    save_error_snapshot(device_name, str(e))
                    log(
                        "warn stale-write "
                        f"age={stale_data.get('data_age_sec')}s "
                        f"mode={stale_data.get('mode_text')} "
                        f"bat={stale_data.get('battery_voltage_v')}V "
                        f"load={stale_data.get('load_watt')}W "
                        f"pv={stale_data.get('pv_input_power_w')}W "
                        f"wh_today={stale_data.get('wh_today')} "
                        f"reason={e}"
                    )
                else:
                    publish_error(client, str(e))
                    save_error_snapshot(device_name, str(e))
                    log(f"error {e}")

                if is_broken_pipe_error(e):
                    sleep_abortable(REOPEN_DELAY_SEC)
                else:
                    sleep_abortable(ERROR_BACKOFF_SEC)

            elapsed = time.time() - cycle_start
            sleep_time = max(0.5, interval - elapsed)
            sleep_abortable(sleep_time)

    finally:
        try:
            publish_value(client, f"{TOPIC_ROOT}/online", False)
            publish_value(client, f"{TOPIC_ROOT}/last_update", now_iso())
        except Exception:
            pass

        try:
            client.loop_stop()
            if client is not None:
                client.disconnect()
        except Exception:
            pass

        log("stopped")

    return 0


if __name__ == "__main__":
    sys.exit(main())

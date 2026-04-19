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

DEVICE_PATH = "/dev/WR2"
DEVICE_NAME = "WR2"
TOPIC_ROOT = "wr/WR2/status"

MQTT_KEEPALIVE = 30
MQTT_QOS = 0
MQTT_RETAIN = True

POLL_INTERVAL_SEC = 15.0
STARTUP_DELAY_SEC = 1.0

READ_GAP_SEC = 0.80
ERROR_BACKOFF_SEC = 5.0
BROKEN_PIPE_BACKOFF_SEC = 2.0

GS_RETRIES = 3
AUX_RETRIES = 2

LOG_BASE_DIR = "/home/pi/wr-logs"

INFLUX_ENABLED = True
INFLUX_HOST = "127.0.0.1"
INFLUX_PORT = 8086
INFLUX_USERNAME = ""
INFLUX_PASSWORD = ""
INFLUX_DB = "wr2"
INFLUX_TIMEOUT_SEC = 5.0

ENERGY_STATE_PATH = os.environ.get("ENERGY_STATE_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "wr2_energy_state.json"))
MAX_INTEGRATION_SEC = 30.0
MAX_STALE_WRITE_SEC = 90.0

RUN = True
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


def sleep_abortable(seconds: float) -> None:
    end = time.time() + seconds
    while RUN and time.time() < end:
        time.sleep(0.1)


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
    if plen > 15:
        chunks = [payload[0:4], payload[4:8], payload[8:12], payload[12:16], payload[16:]]
        delay = 0.010
    elif plen > 10:
        chunks = [payload[0:4], payload[4:8], payload[8:]]
        delay = 0.005
    elif plen > 5:
        chunks = [payload[0:4], payload[4:]]
        delay = 0.010
    else:
        chunks = [payload]
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


def decode_infini_answer(raw: bytes) -> Dict[str, Any]:
    raw_clean = raw.rstrip(b"\x00")

    if raw_clean.startswith(b"^0"):
        return {
            "kind": "NAK",
            "data": None,
            "raw_hex": raw.hex(),
            "raw_len": len(raw),
        }

    if raw_clean.startswith(b"^1"):
        return {
            "kind": "ACK",
            "data": None,
            "raw_hex": raw.hex(),
            "raw_len": len(raw),
        }

    if raw_clean.startswith(b"^D") and len(raw_clean) >= 5:
        length_txt = raw_clean[2:5]
        if all(48 <= c <= 57 for c in length_txt):
            total_len = int(length_txt.decode("ascii", errors="ignore"))
            data_len = total_len - 3
            data = raw_clean[5:5 + data_len]
            return {
                "kind": "DATA",
                "data": data.decode("ascii", errors="replace"),
                "raw_hex": raw.hex(),
                "raw_len": len(raw),
                "length_field": total_len,
                "data_len": data_len,
            }

    return {
        "kind": "UNKNOWN",
        "data": raw_clean.decode("ascii", errors="replace"),
        "raw_hex": raw.hex(),
        "raw_len": len(raw),
    }


def looks_like_gs_data(text: Optional[str]) -> bool:
    if not text:
        return False
    parts = text.split(",")
    if len(parts) < 20:
        return False
    return parts[0].isdigit() and parts[1].isdigit() and parts[2].isdigit()


def send_cmd_once(fd: int, cmd: str) -> Dict[str, Any]:
    pre = drain(fd, 0.30)
    frame = build_infini_frame(cmd)

    payload = frame[:-3]
    trailer = frame[-3:]

    write_chunked(fd, payload)
    time.sleep(0.015)
    os.write(fd, trailer)
    time.sleep(0.030)

    raw = read_infini_response(fd, timeout=6.5)
    decoded = decode_infini_answer(raw)

    return {
        "command": cmd,
        "tx_hex": frame.hex(),
        "pre_drain_hex": pre.hex(),
        "rx": decoded,
    }


def send_cmd_retry(fd: int, cmd: str, retries: int, validator=None) -> Dict[str, Any]:
    last = None
    for attempt in range(1, retries + 1):
        res = send_cmd_once(fd, cmd)
        res["attempt"] = attempt
        last = res

        rx = res["rx"]
        kind = rx.get("kind")
        data = rx.get("data")

        if kind == "DATA":
            if validator is None or validator(data):
                return res

        if kind == "NAK":
            time.sleep(0.50)
        else:
            time.sleep(0.35)

    return last


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


def parse_gs(data: str) -> Dict[str, Any]:
    teile = data.split(",")
    result: Dict[str, Any] = {
        "raw_fields": teile,
        "field_count": len(teile),
    }

    def get(idx: int, kind: str = "str", div: Optional[float] = None) -> Any:
        if idx >= len(teile):
            return None
        val = teile[idx]
        if kind == "int":
            v = try_int(val)
        elif kind == "float":
            v = try_float(val)
        else:
            v = val
        if div is not None and isinstance(v, (int, float)):
            return v / div
        return v

    result["netzspannung_v"] = get(0, "float", 10)
    result["netzfrequenz_hz"] = get(1, "float", 10)
    result["ac_ausgangsspannung_v"] = get(2, "float", 10)
    result["ac_ausgangsfrequenz_hz"] = get(3, "float", 10)
    result["ac_scheinleistung_va"] = get(4, "int")
    result["ac_wirkleistung_w"] = get(5, "int")
    result["ausgangslast_percent"] = get(6, "int")
    result["batteriespannung_v"] = get(7, "float", 10)

    result["batterieentladestrom_a"] = get(10, "int")
    result["batterieladestrom_a"] = get(11, "int")
    result["batteriekapazitaet_percent"] = get(12, "int")
    result["temperatur_c"] = get(13, "int")
    result["mppt1_temperatur_c"] = get(14, "int")
    result["mppt2_temperatur_c"] = get(15, "int")
    result["solarleistung1_w"] = get(16, "int")
    result["solarleistung2_w"] = get(17, "int")
    result["solarspannung1_v"] = get(18, "float", 10)
    result["solarspannung2_v"] = get(19, "float", 10)
    result["status_raw_20"] = get(20, "str")
    result["ladestatus1"] = get(21, "int")
    result["ladestatus2"] = get(22, "int")
    result["batteriestromrichtung"] = get(23, "int")
    result["wr_stromrichtung"] = get(24, "int")
    result["netzstromrichtung"] = get(25, "int")
    result["status_raw_26"] = get(26, "str")
    result["status_raw_27"] = get(27, "str")

    if isinstance(result.get("solarleistung1_w"), int) and isinstance(result.get("solarleistung2_w"), int):
        result["solarleistung_gesamt_w"] = result["solarleistung1_w"] + result["solarleistung2_w"]

    return result


def parse_mod(data: str) -> Dict[str, Any]:
    return {"modus_raw": data}


def parse_pi(data: str) -> Dict[str, Any]:
    return {"firmware_raw": data}


def parse_fws(data: str) -> Dict[str, Any]:
    teile = data.split(",")
    result: Dict[str, Any] = {
        "raw_fields": teile,
        "field_count": len(teile),
    }

    if len(teile) > 0:
        result["fehlercode"] = try_int(teile[0])

    warnungen = []
    for i in range(1, min(len(teile), 16)):
        if str(teile[i]) == "1":
            warnungen.append(i)

    result["warnungen_aktiv"] = warnungen
    result["warnungen_anzahl"] = len(warnungen)

    return result


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


def build_payload(device_name: str, dev: str, gs_data: str, pi_data: Optional[str], mod_data: Optional[str], fws_data: Optional[str]) -> Dict[str, Any]:
    gs = parse_gs(gs_data)
    pi = parse_pi(pi_data) if pi_data is not None else {}
    mod = parse_mod(mod_data) if mod_data is not None else {}
    fws = parse_fws(fws_data) if fws_data is not None else {}

    wh_today = update_daily_wh(gs.get("solarleistung_gesamt_w"))

    return {
        "timestamp": now_iso(),
        "device_name": device_name,
        "device_path": dev,
        "ok": True,
        "firmware_raw": pi.get("firmware_raw"),
        "modus_raw": mod.get("modus_raw"),
        "fehlercode": fws.get("fehlercode"),
        "warnungen_aktiv": fws.get("warnungen_aktiv"),
        "warnungen_anzahl": fws.get("warnungen_anzahl"),
        "netzspannung_v": gs.get("netzspannung_v"),
        "netzfrequenz_hz": gs.get("netzfrequenz_hz"),
        "ac_ausgangsspannung_v": gs.get("ac_ausgangsspannung_v"),
        "ac_ausgangsfrequenz_hz": gs.get("ac_ausgangsfrequenz_hz"),
        "ac_scheinleistung_va": gs.get("ac_scheinleistung_va"),
        "ac_wirkleistung_w": gs.get("ac_wirkleistung_w"),
        "ausgangslast_percent": gs.get("ausgangslast_percent"),
        "batteriespannung_v": gs.get("batteriespannung_v"),
        "batterieentladestrom_a": gs.get("batterieentladestrom_a"),
        "batterieladestrom_a": gs.get("batterieladestrom_a"),
        "batteriekapazitaet_percent": gs.get("batteriekapazitaet_percent"),
        "temperatur_c": gs.get("temperatur_c"),
        "mppt1_temperatur_c": gs.get("mppt1_temperatur_c"),
        "mppt2_temperatur_c": gs.get("mppt2_temperatur_c"),
        "solarleistung1_w": gs.get("solarleistung1_w"),
        "solarleistung2_w": gs.get("solarleistung2_w"),
        "solarleistung_gesamt_w": gs.get("solarleistung_gesamt_w"),
        "solarspannung1_v": gs.get("solarspannung1_v"),
        "solarspannung2_v": gs.get("solarspannung2_v"),
        "ladestatus1": gs.get("ladestatus1"),
        "ladestatus2": gs.get("ladestatus2"),
        "batteriestromrichtung": gs.get("batteriestromrichtung"),
        "wr_stromrichtung": gs.get("wr_stromrichtung"),
        "netzstromrichtung": gs.get("netzstromrichtung"),
        "status_raw_20": gs.get("status_raw_20"),
        "status_raw_26": gs.get("status_raw_26"),
        "status_raw_27": gs.get("status_raw_27"),
        "wh_today": wh_today,
        "field_count": gs.get("field_count"),
    }


def read_cycle_once(dev: str, device_name: str) -> Dict[str, Any]:
    fd = None
    try:
        fd = os.open(dev, os.O_RDWR | os.O_NONBLOCK)
        sleep_abortable(0.20)

        gs = send_cmd_retry(fd, "^P005GS", GS_RETRIES, validator=looks_like_gs_data)
        time.sleep(READ_GAP_SEC)

        gs_rx = gs["rx"]
        if gs_rx["kind"] != "DATA" or not looks_like_gs_data(gs_rx.get("data")):
            raise RuntimeError(f"GS invalid: kind={gs_rx['kind']} data={gs_rx.get('data')!r}")

        pi = send_cmd_retry(fd, "^P005PI", AUX_RETRIES)
        time.sleep(READ_GAP_SEC)

        mod = send_cmd_retry(fd, "^P006MOD", AUX_RETRIES)
        time.sleep(READ_GAP_SEC)

        fws = send_cmd_retry(fd, "^P006FWS", AUX_RETRIES)

        pi_rx = pi["rx"]
        mod_rx = mod["rx"]
        fws_rx = fws["rx"]

        data = build_payload(
            device_name=device_name,
            dev=dev,
            gs_data=gs_rx["data"],
            pi_data=pi_rx["data"] if pi_rx["kind"] == "DATA" else None,
            mod_data=mod_rx["data"] if mod_rx["kind"] == "DATA" else None,
            fws_data=fws_rx["data"] if fws_rx["kind"] == "DATA" else None,
        )
        return data
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass


def mqtt_connect() -> mqtt.Client:
    client = mqtt.Client(client_id=f"{DEVICE_NAME}_infini_loop")
    if BROKER_USERNAME:
        client.username_pw_set(BROKER_USERNAME, BROKER_PASSWORD)
    client.connect(BROKER_HOST, BROKER_PORT, MQTT_KEEPALIVE)
    client.loop_start()
    time.sleep(0.2)
    return client


def publish_value(client: mqtt.Client, topic: str, value: Any) -> None:
    if client is None:
        return
    if isinstance(value, bool):
        payload = "true" if value else "false"
    elif value is None:
        payload = ""
    elif isinstance(value, (dict, list)):
        payload = json.dumps(value, ensure_ascii=False)
    else:
        payload = str(value)

    info = client.publish(topic, payload=payload, qos=MQTT_QOS, retain=MQTT_RETAIN)
    info.wait_for_publish()


def publish_payload(client: mqtt.Client, topic_root: str, data: Dict[str, Any]) -> None:
    publish_value(client, f"{topic_root}/json", data)
    publish_value(client, f"{topic_root}/online", True)
    publish_value(client, f"{topic_root}/last_update", data.get("timestamp"))
    publish_value(client, f"{topic_root}/error", data.get("stale_reason", "") if data.get("stale") else "")

    scalar_keys = [
        "ok",
        "firmware_raw",
        "modus_raw",
        "fehlercode",
        "warnungen_anzahl",
        "netzspannung_v",
        "netzfrequenz_hz",
        "ac_ausgangsspannung_v",
        "ac_ausgangsfrequenz_hz",
        "ac_scheinleistung_va",
        "ac_wirkleistung_w",
        "ausgangslast_percent",
        "batteriespannung_v",
        "batterieentladestrom_a",
        "batterieladestrom_a",
        "batteriekapazitaet_percent",
        "temperatur_c",
        "mppt1_temperatur_c",
        "mppt2_temperatur_c",
        "solarleistung1_w",
        "solarleistung2_w",
        "solarleistung_gesamt_w",
        "solarspannung1_v",
        "solarspannung2_v",
        "ladestatus1",
        "ladestatus2",
        "batteriestromrichtung",
        "wr_stromrichtung",
        "netzstromrichtung",
        "status_raw_20",
        "status_raw_26",
        "status_raw_27",
        "wh_today",
        "stale",
        "read_ok",
        "data_age_sec",
        "field_count",
    ]

    for key in scalar_keys:
        publish_value(client, f"{topic_root}/{key}", data.get(key))

    publish_value(client, f"{topic_root}/warnungen_aktiv", data.get("warnungen_aktiv"))


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


def wr2_laststatus_value(data: Dict[str, Any]) -> float:
    return 1.0 if float(data.get("ac_wirkleistung_w", 0) or 0) > 0 else 0.0


def wr2_modus_numeric_value(data: Dict[str, Any]) -> float:
    raw = str(data.get("modus_raw", "0")).strip()
    try:
        return float(int(raw))
    except Exception:
        return 0.0


def online_to_status_value(data: Dict[str, Any]) -> float:
    return 1.0 if bool(data.get("ok", False)) else 0.0


def build_influx_lines_wr2(data: Dict[str, Any]) -> list:
    pv = {
        "Leistung": data.get("solarleistung_gesamt_w", 0) or 0,
        "Leistung1": data.get("solarleistung1_w", 0) or 0,
        "Leistung2": data.get("solarleistung2_w", 0) or 0,
        "MPPT1_Leistung": data.get("solarleistung1_w", 0) or 0,
        "MPPT2_Leistung": data.get("solarleistung2_w", 0) or 0,
        "MPPT3_Leistung": 0.0,
        "MPPT4_Leistung": 0.0,
        "Spannung": data.get("solarspannung1_v", 0) or 0,
        "Spannung1": data.get("solarspannung1_v", 0) or 0,
        "Spannung2": data.get("solarspannung2_v", 0) or 0,
        "Spannung3": 0.0,
        "Spannung4": 0.0,
        "Spannung5": 0.0,
        "Spannung6": 0.0,
        "Spannung7": 0.0,
        "Spannung8": 0.0,
        "Strom1": 0.0,
        "Strom2": 0.0,
        "Strom3": 0.0,
        "Strom4": 0.0,
        "Strom5": 0.0,
        "Strom6": 0.0,
        "Strom7": 0.0,
        "Strom8": 0.0,
    }

    ac = {
        "Ausgangslast": data.get("ausgangslast_percent", 0) or 0,
        "Frequenz": data.get("ac_ausgangsfrequenz_hz", 0) or 0,
        "Leistung": data.get("ac_wirkleistung_w", 0) or 0,
        "Powerfactor": 0.0,
        "Scheinleistung": data.get("ac_scheinleistung_va", 0) or 0,
        "Spannung": data.get("ac_ausgangsspannung_v", 0) or 0,
        "Spannung_R": 0.0,
        "Spannung_S": 0.0,
        "Spannung_T": 0.0,
        "Strom_R": 0.0,
        "Strom_S": 0.0,
        "Strom_T": 0.0,
        "Wirkleistung": data.get("ac_wirkleistung_w", 0) or 0,
    }

    batterie = {
        "Entladestrom": data.get("batterieentladestrom_a", 0) or 0,
        "Kapazitaet": data.get("batteriekapazitaet_percent", 0) or 0,
        "Ladestrom": data.get("batterieladestrom_a", 0) or 0,
        "Spannung": data.get("batteriespannung_v", 0) or 0,
        "Spannung_WR": data.get("batteriespannung_v", 0) or 0,
    }

    service = {
        "Device_Status": 0.0,
        "Effizienz": "",
        "Fehlercode": data.get("fehlercode", 0) or 0,
        "Ladestatus": data.get("ladestatus1", 0) or 0,
        "Ladestatus2": data.get("ladestatus2", 0) or 0,
        "Laststatus": wr2_laststatus_value(data),
        "MPPT1_Temperatur": data.get("mppt1_temperatur_c", 0) or 0,
        "MPPT2_Temperatur": data.get("mppt2_temperatur_c", 0) or 0,
        "Modell": 0.0,
        "Modus": wr2_modus_numeric_value(data),
        "Status": online_to_status_value(data),
        "Stromrichtung_Batt": data.get("batteriestromrichtung", 0) or 0,
        "Stromrichtung_Netz": data.get("netzstromrichtung", 0) or 0,
        "Stromrichtung_WR": data.get("wr_stromrichtung", 0) or 0,
        "Temperatur": data.get("temperatur_c", 0) or 0,
        "WR_Fehler": "",
        "Warnungen": data.get("warnungen_anzahl", 0) or 0,
        "Stale": 1.0 if data.get("stale") else 0.0,
        "Read_OK": 1.0 if data.get("read_ok", True) else 0.0,
        "DataAgeSec": data.get("data_age_sec", 0) or 0,
    }

    netz = {
        "Spannung": data.get("netzspannung_v", 0) or 0,
        "Frequenz": data.get("netzfrequenz_hz", 0) or 0,
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


def write_influx_wr2(data: Dict[str, Any]) -> None:
    if not INFLUX_ENABLED:
        return
    lines = build_influx_lines_wr2(data)
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
                write_influx_wr2(data)
                log(
                    "ok "
                    f"mode={data.get('modus_raw')} "
                    f"bat={data.get('batteriespannung_v')}V "
                    f"load={data.get('ac_wirkleistung_w')}W "
                    f"pv={data.get('solarleistung_gesamt_w')}W "
                    f"wh_today={data.get('wh_today')}"
                )
            except Exception as e:
                stale_data = build_stale_data(str(e))
                if stale_data is not None:
                    publish_payload(client, TOPIC_ROOT, stale_data)
                    write_influx_wr2(stale_data)
                    save_error_snapshot(device_name, str(e))
                    log(
                        "warn stale-write "
                        f"age={stale_data.get('data_age_sec')}s "
                        f"mode={stale_data.get('modus_raw')} "
                        f"bat={stale_data.get('batteriespannung_v')}V "
                        f"load={stale_data.get('ac_wirkleistung_w')}W "
                        f"pv={stale_data.get('solarleistung_gesamt_w')}W "
                        f"wh_today={stale_data.get('wh_today')} "
                        f"reason={e}"
                    )
                else:
                    publish_error(client, str(e))
                    save_error_snapshot(device_name, str(e))
                    log(f"error {e}")
                if is_broken_pipe_error(e):
                    sleep_abortable(BROKEN_PIPE_BACKOFF_SEC)
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

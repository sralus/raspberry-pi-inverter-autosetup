#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
from pathlib import Path
import re
from datetime import datetime

SOURCE_FILE = "/home/pi/wr-logs/WR2_latest.json"
OVERRIDE_FILE = os.environ.get("OVERRIDE_FILE", str(Path(__file__).resolve().parent / "wr2_ui_state_override.json"))
OUTPUT_JSON = os.environ.get("OUTPUT_JSON", str(Path(__file__).resolve().parent.parent / "ui" / "wr2_state.json"))

PSP_MAP = {
    "PSP0": "Batterie → Last → Netz",
    "PSP1": "Last → Batterie → Netz",
    "PSP2": "Unbekannt / reserviert",
}
PCP_MAP = {
    "PCP0": "Solar zuerst",
    "PCP1": "Solar und Netz",
    "PCP2": "Nur Solar",
}
POP_MAP = {
    "POP0": "Solar → Netz → Batterie",
    "POP1": "Solar → Batterie → Netz",
}

SWITCH_LABELS = {
    "A": "Summer stummschalten",
    "B": "Überlast-Bypass",
    "C": "LCD nach 1 Minute auf Standardseite",
    "D": "Automatischer Neustart nach Überlast",
    "E": "Automatischer Neustart nach Übertemperatur",
    "F": "Hintergrundbeleuchtung",
    "G": "Alarm bei Ausfall der Hauptquelle",
    "H": "Fehlercode-Protokoll speichern",
    "I": "Betriebsart Gerät (Grid-Tie / Off-Grid Tie)",
}
LOCKED_SWITCHES = {"B", "D", "E", "I"}

MODUS_MAP = {
    "00": "Power On",
    "01": "Standby",
    "02": "Netzbetrieb",
    "03": "Batteriebetrieb",
    "04": "Fehler",
    "05": "Hybrid / Line",
}

def parse_float(v, digits=1):
    if v is None or v == "":
        return None
    try:
        return round(float(v), digits)
    except Exception:
        return None

def parse_int(v):
    if v is None or v == "":
        return None
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return None

def parse_boolish(v):
    if isinstance(v, bool):
        return v
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "on", "online", "connected"):
        return True
    if s in ("0", "false", "no", "off", "offline", "disconnected"):
        return False
    return None

def normalize_code(v, prefix):
    if v is None:
        return None
    s = str(v).strip().upper()
    m = re.search(rf"{prefix}\d+", s)
    if m:
        return m.group(0)
    if s.isdigit():
        return f"{prefix}{s}"
    if s.startswith(prefix):
        return s
    return None

def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def load_override():
    return load_json(OVERRIDE_FILE, {})

def load_existing_state():
    return load_json(OUTPUT_JSON, {})

def get_override_code(ovr, key, prefix):
    try:
        return normalize_code(ovr.get("priority", {}).get(key, {}).get("code"), prefix)
    except Exception:
        return None

def get_override_setting(ovr, key, fallback=None):
    try:
        val = ovr.get("settings", {}).get(key, fallback)
        return val if val is not None else fallback
    except Exception:
        return fallback

def existing_setting(existing, key, fallback=None):
    try:
        val = existing.get("settings", {}).get(key, fallback)
        return val if val is not None else fallback
    except Exception:
        return fallback

def existing_switches(existing):
    try:
        sw = existing.get("switches", [])
        return sw if isinstance(sw, list) else []
    except Exception:
        return []

def existing_raw_debug(existing):
    try:
        rd = existing.get("raw_debug", {})
        return rd if isinstance(rd, dict) else {}
    except Exception:
        return {}

def tenths_to_v(s):
    i = parse_int(s)
    if i is None:
        return None
    return round(i / 10.0, 1)

def parse_piri(data_text):
    if not data_text:
        return {}
    parts = [p.strip() for p in str(data_text).split(",")]
    if len(parts) < 26:
        return {"raw_parts": parts, "error": f"too_few_parts:{len(parts)}"}

    return {
        "raw_parts": parts,
        "battery_recharge_voltage_v": tenths_to_v(parts[7]),
        "battery_redischarge_voltage_v": tenths_to_v(parts[9]),
        "battery_cutoff_voltage_v": tenths_to_v(parts[8]),
        "bulk_voltage_v": tenths_to_v(parts[11]),
        "floating_voltage_v": tenths_to_v(parts[12]),
        "piri_idx_07_v": tenths_to_v(parts[7]),
        "piri_idx_08_v": tenths_to_v(parts[8]),
        "piri_idx_09_v": tenths_to_v(parts[9]),
        "piri_idx_10_v": tenths_to_v(parts[10]),
        "piri_idx_11_v": tenths_to_v(parts[11]),
        "piri_idx_12_v": tenths_to_v(parts[12]),
        "battery_type_raw": parts[13],
        "max_ac_charge_current_a": parse_int(parts[14]),
        "max_charge_current_a": parse_int(parts[15]),
        "pop_raw": parts[18],
        "pcp_raw": parts[19],
        "psp_raw": parts[24],
        "machine_tail_raw": parts[25],
    }

def map_pop(raw):
    if raw is None:
        return None
    mapping = {
        "0": "POP0",
        "1": "POP1",
        "2": "POP1",
    }
    return mapping.get(str(raw).strip())

def map_pcp(raw):
    if raw is None:
        return None
    mapping = {
        "0": "PCP0",
        "1": "PCP1",
        "2": "PCP2",
        "9": "PCP0",
    }
    return mapping.get(str(raw).strip())

def map_psp(raw):
    if raw is None:
        return None
    mapping = {
        "0": "PSP0",
        "1": "PSP1",
        "2": "PSP2",
    }
    return mapping.get(str(raw).strip())

def parse_flag(data_text):
    if not data_text:
        return []
    parts = [p.strip() for p in str(data_text).split(",")]
    keys = list("ABCDEFGHI")
    out = []
    for idx, key in enumerate(keys):
        raw = parts[idx] if idx < len(parts) else None
        enabled = None
        if raw in ("0", "1"):
            enabled = (raw == "1")
        out.append({
            "key": key,
            "label": SWITCH_LABELS[key],
            "enabled": enabled,
            "locked": (key in LOCKED_SWITCHES),
            "raw": raw,
        })
    return out

def build_default_switches():
    return [{
        "key": letter,
        "label": SWITCH_LABELS[letter],
        "enabled": None,
        "locked": (letter in LOCKED_SWITCHES),
    } for letter in "ABCDEFGHI"]

def main():
    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(f"Quelle nicht gefunden: {SOURCE_FILE}")

    raw = load_json(SOURCE_FILE, {})
    data = raw.get("data", {}) if isinstance(raw, dict) else {}
    override = load_override()
    existing = load_existing_state()
    existing_rd = existing_raw_debug(existing)

    piri_raw = existing_rd.get("piri_raw")
    flag_raw = existing_rd.get("flag_raw")

    piri = parse_piri(piri_raw)
    flags = parse_flag(flag_raw)
    if not flags:
        flags = existing_switches(existing) or build_default_switches()

    timestamp = data.get("timestamp") or raw.get("saved_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ok1 = parse_boolish(data.get("ok"))
    ok2 = parse_boolish(data.get("read_ok"))
    stale = parse_boolish(data.get("stale"))
    if ok1 is True or ok2 is True:
        online = True
    elif stale is True:
        online = False
    else:
        online = None

    modus_raw = str(data.get("modus_raw", "")).strip()
    mode_text = MODUS_MAP.get(modus_raw, "unbekannt")
    if modus_raw:
        mode_text = f"{mode_text} (raw {modus_raw})"

    psp = get_override_code(override, "psp", "PSP") or map_psp(piri.get("psp_raw")) or existing.get("priority", {}).get("psp", {}).get("code")
    pcp = get_override_code(override, "pcp", "PCP") or map_pcp(piri.get("pcp_raw")) or existing.get("priority", {}).get("pcp", {}).get("code")
    pop = get_override_code(override, "pop", "POP") or map_pop(piri.get("pop_raw")) or existing.get("priority", {}).get("pop", {}).get("code")

    battery_type = {
        "0": "AGM",
        "1": "Flooded",
        "2": "User",
        "3": "Lithium",
    }.get(str(piri.get("battery_type_raw")), existing_setting(existing, "battery_type", "Unbekannt"))

    settings = {
        "battery_type": {
            "0": "AGM",
            "1": "Flooded",
            "2": "User",
            "3": "Lithium",
        }.get(str(piri.get("battery_type_raw")), "Unbekannt"),
        "max_charge_current_a": piri.get("max_charge_current_a") or get_override_setting(override, "max_charge_current_a", "10.0"),
        "max_ac_charge_current_a": piri.get("max_ac_charge_current_a") or get_override_setting(override, "max_ac_charge_current_a", "10.0"),

        # Safe-Mode: bei setzbaren Spannungswerten die zuletzt bestätigten
        # Override-Werte bevorzugen, damit das WI den gesetzten Stand zeigt.
        "bulk_voltage_v": get_override_setting(override, "bulk_voltage_v", piri.get("bulk_voltage_v") or "56.4"),
        "floating_voltage_v": get_override_setting(override, "floating_voltage_v", piri.get("floating_voltage_v") or "54.0"),
        "battery_recharge_voltage_v": get_override_setting(override, "battery_recharge_voltage_v", piri.get("battery_recharge_voltage_v") or "46.0"),
        "battery_redischarge_voltage_v": get_override_setting(override, "battery_redischarge_voltage_v", piri.get("battery_redischarge_voltage_v") or "54.0"),
        "battery_cutoff_voltage_v": get_override_setting(override, "battery_cutoff_voltage_v", piri.get("battery_cutoff_voltage_v") or "48.0"),
    }

    state = {
        "meta": {
            "source_file": SOURCE_FILE,
            "override_file": OVERRIDE_FILE,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "builder_mode": "safe_no_direct_wr_access",
        },
        "device": {
            "name": data.get("device_name") or raw.get("device_name") or "WR2",
            "online": online,
            "timestamp": timestamp,
            "mode_text": mode_text,
        },
        "metrics": {
            "pv_power_w": parse_float(data.get("solarleistung_gesamt_w"), 0),
            "load_power_w": parse_float(data.get("ac_wirkleistung_w"), 0),
            "battery_voltage_v": parse_float(data.get("batteriespannung_v"), 1),
            "charge_current_a": parse_float(data.get("batterieladestrom_a"), 1),
            "soc_percent": parse_float(data.get("batteriekapazitaet_percent"), 0),
            "wr_temp_c": parse_float(data.get("temperatur_c"), 1),
        },
        "priority": {
            "psp": {"code": psp, "label": PSP_MAP.get(psp, "Unbekannt"), "recommended": (psp == "PSP0")},
            "pcp": {"code": pcp, "label": PCP_MAP.get(pcp, "Unbekannt"), "recommended": (pcp == "PCP0")},
            "pop": {"code": pop, "label": POP_MAP.get(pop, "Unbekannt"), "recommended": (pop == "POP1")},
        },
        "settings": settings,
        "switches": flags,
        "recommendation": {
            "text": "Empfehlung für WR2 aktuell: PSP0 + PCP0",
            "psp": "PSP0",
            "pcp": "PCP0",
        },
        "raw_debug": {
            "modus_raw": data.get("modus_raw"),
            "firmware_raw": data.get("firmware_raw"),
            "override_loaded": bool(override),
            "piri_raw": piri_raw,
            "flag_raw": flag_raw,
            "piri_parse": piri,
            "note": "Keine Direktzugriffe auf WR2; PIRI/FLAG nur aus vorhandenem State/Cache, Overrides als Fallback",
        },
    }

    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    tmp = OUTPUT_JSON + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, OUTPUT_JSON)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
from pathlib import Path
from datetime import datetime

SOURCE_FILE = os.environ.get("LATEST_JSON", "/home/pi/wr-logs/WR1_latest.json")
OUTPUT_JSON = os.environ.get("OUTPUT_JSON", str(Path(__file__).resolve().parent.parent / "ui" / "wr1_state.json"))


def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


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


def mode_text_from_code(code, fallback=None):
    mapping = {
        "P": "Power On",
        "S": "Standby",
        "L": "Line",
        "B": "Battery",
        "F": "Fault",
        "H": "Power Saving",
    }
    if fallback:
        return fallback
    if code is None:
        return "Unknown"
    return mapping.get(str(code).strip().upper(), "Unknown")


def safe_status_flag(status, key):
    if not isinstance(status, dict):
        return None
    return parse_boolish(status.get(key))


def yes_no_unknown(v):
    if v is True:
        return "Ein"
    if v is False:
        return "Aus"
    return "Unbekannt"


def fmt_val(v, suffix=""):
    if v is None or v == "":
        return "--"
    return f"{v}{suffix}"


def output_priority_label(raw):
    mapping = {
        "0": "Netzbetrieb",
        "1": "Solar zuerst",
        "2": "SBU",
    }
    if raw is None:
        return "--"
    return mapping.get(str(raw), f"Unbekannt ({raw})")


def charger_priority_label(raw):
    mapping = {
        "0": "Netz zuerst",
        "1": "Solar zuerst (nicht freigegeben)",
        "2": "Solar + Netz",
        "3": "Nur Solar",
    }
    if raw is None:
        return "--"
    return mapping.get(str(raw), f"Unbekannt ({raw})")


def main():
    raw = load_json(SOURCE_FILE, {})
    data = raw.get("data", {}) if isinstance(raw, dict) else {}
    existing = load_json(OUTPUT_JSON, {})

    def _norm_prev(v):
        if v in (None, "", "--"):
            return None
        s = str(v).strip()
        for suf in [" V", " VA", " W", " A", " %", " Wh", " s"]:
            if s.endswith(suf):
                s = s[:-len(suf)].strip()
        return s if s not in ("", "--") else None

    def _existing_editable_raw(key):
        try:
            return _norm_prev((((existing.get("editable") or {}).get(key) or {}).get("current_raw")))
        except Exception:
            return None

    def _existing_editable_text(key):
        try:
            return _norm_prev((((existing.get("editable") or {}).get(key) or {}).get("current_text")))
        except Exception:
            return None

    def _existing_current(key):
        try:
            return _norm_prev((existing.get("current_values") or {}).get(key))
        except Exception:
            return None

    def _existing_raw(key):
        try:
            return _norm_prev((existing.get("raw_data") or {}).get(key))
        except Exception:
            return None

    def _existing_extra(label):
        try:
            for item in (existing.get("extra_info") or []):
                if item.get("label") == label:
                    return item.get("value")
        except Exception:
            pass
        return None

    timestamp = (
        data.get("timestamp")
        or raw.get("saved_at")
        or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

    ok1 = parse_boolish(data.get("ok"))
    ok2 = parse_boolish(data.get("read_ok"))
    stale = parse_boolish(data.get("stale"))

    if stale is True:
        online = False
        online_text = "WR1 offline / stale"
    elif ok1 is True or ok2 is True:
        online = True
        online_text = "WR1 online"
    elif ok1 is False or ok2 is False:
        online = False
        online_text = "WR1 offline"
    else:
        online = None
        online_text = "WR1 Status unbekannt"

    device_status = data.get("device_status", {})
    mode_code = data.get("mode_code")
    mode_text = mode_text_from_code(mode_code, data.get("mode_text"))

    metrics = {
        "pv_input_power_w": parse_int(data.get("pv_input_power_w")),
        "load_watt": parse_int(data.get("load_watt")),
        "load_va": parse_int(data.get("load_va")),
        "load_percent": parse_int(data.get("load_percent")),
        "battery_voltage_v": parse_float(data.get("battery_voltage_v"), 1),
        "battery_charge_current_a": parse_int(data.get("battery_charge_current_a")),
        "battery_discharge_current_a": parse_int(data.get("battery_discharge_current_a")),
        "battery_capacity_percent": parse_int(data.get("battery_capacity_percent")),
        "heatsink_temperature_c": parse_int(data.get("heatsink_temperature_c")),
        "pv_input_current_a": parse_float(data.get("pv_input_current_a"), 1),
        "pv_input_voltage_v": parse_float(data.get("pv_input_voltage_v"), 1),
        "ac_grid_voltage_v": parse_float(data.get("ac_grid_voltage_v"), 1),
        "ac_grid_frequency_hz": parse_float(data.get("ac_grid_frequency_hz"), 1),
        "ac_output_voltage_v": parse_float(data.get("ac_output_voltage_v"), 1),
        "ac_output_frequency_hz": parse_float(data.get("ac_output_frequency_hz"), 1),
        "bus_voltage_v": parse_int(data.get("bus_voltage_v")),
        "scc_voltage_v": parse_float(data.get("scc_voltage_v"), 1),
        "wh_today": parse_float(data.get("wh_today"), 3),
    }

    warnings_raw = data.get("warnings_raw") or ""
    warnings_all_zero = bool(data.get("warnings", {}).get("all_zero", False))
    if warnings_raw:
        warning_text = "Keine Warnungen" if warnings_all_zero else f"Warnbits aktiv: {warnings_raw}"
    else:
        warning_text = "--"

    output_raw = str(data.get("piri_output_source_priority_raw")).strip() if data.get("piri_output_source_priority_raw") is not None else None
    charger_raw = str(data.get("piri_charger_source_priority_raw")).strip() if data.get("piri_charger_source_priority_raw") is not None else None

    if output_raw in (None, "", "--"):
        output_raw = _existing_editable_raw("output_priority")

    if charger_raw in (None, "", "--"):
        charger_raw = _existing_editable_raw("charger_priority")

    recharge_v = (
        _norm_prev(data.get("piri_battery_recharge_voltage_v"))
        or _existing_current("battery_recharge_voltage_v")
        or _existing_raw("piri_battery_recharge_voltage_v")
        or _existing_editable_raw("recharge_voltage")
        or _existing_editable_text("recharge_voltage")
    )

    redischarge_v = (
        _norm_prev(data.get("piri_battery_redischarge_voltage_2_v"))
        or _norm_prev(data.get("piri_battery_redischarge_voltage_v"))
        or _existing_current("battery_redischarge_voltage_2_v")
        or _existing_current("battery_redischarge_voltage_v")
        or _existing_raw("piri_battery_redischarge_voltage_2_v")
        or _existing_raw("piri_battery_redischarge_voltage_v")
        or _existing_editable_raw("redischarge_voltage")
        or _existing_editable_text("redischarge_voltage")
    )

    cutoff_v = (
        _norm_prev(data.get("piri_battery_cutoff_voltage_v"))
        or _existing_current("battery_cutoff_voltage_v")
        or _existing_current("piri_battery_cutoff_voltage_v")
        or _existing_raw("piri_battery_cutoff_voltage_v")
        or _existing_editable_raw("cutoff_voltage")
        or _existing_editable_text("cutoff_voltage")
    )

    bulk_v = (
        _norm_prev(data.get("piri_bulk_voltage_v"))
        or _existing_current("bulk_voltage_v")
        or _existing_raw("piri_bulk_voltage_v")
        or _existing_editable_raw("bulk_voltage")
        or _existing_editable_text("bulk_voltage")
    )

    float_v = (
        _norm_prev(data.get("piri_float_voltage_v"))
        or _existing_current("float_voltage_v")
        or _existing_raw("piri_float_voltage_v")
        or _existing_editable_raw("float_voltage")
        or _existing_editable_text("float_voltage")
    )

    nennleistung = _existing_extra("Nennleistung")
    if data.get("piri_ac_output_rating_apparent_power_va") not in (None, "", "--") or data.get("piri_ac_output_rating_active_power_w") not in (None, "", "--"):
        nennleistung = f"{data.get('piri_ac_output_rating_apparent_power_va', '--')} VA / {data.get('piri_ac_output_rating_active_power_w', '--')} W"

    max_charge_info = _existing_extra("Max. AC / Gesamt Ladestrom")
    if data.get("piri_max_ac_charge_current_a") not in (None, "", "--") or data.get("piri_max_charge_current_a") not in (None, "", "--"):
        max_charge_info = f"{fmt_val(data.get('piri_max_ac_charge_current_a'), ' A')} / {fmt_val(data.get('piri_max_charge_current_a'), ' A')}"

    top_status = [
        {"label": "Last aktiv", "value": yes_no_unknown(safe_status_flag(device_status, "load_on")), "state": safe_status_flag(device_status, "load_on")},
        {"label": "Solar-Ladung", "value": yes_no_unknown(safe_status_flag(device_status, "scc_charging")), "state": safe_status_flag(device_status, "scc_charging")},
        {"label": "Netz-Ladung", "value": yes_no_unknown(safe_status_flag(device_status, "ac_charging")), "state": safe_status_flag(device_status, "ac_charging")},
        {"label": "Lädt", "value": yes_no_unknown(safe_status_flag(device_status, "charging")), "state": safe_status_flag(device_status, "charging")},
        {"label": "Konfig geändert", "value": yes_no_unknown(safe_status_flag(device_status, "config_changed")), "state": safe_status_flag(device_status, "config_changed")},
        {"label": "SCC Firmware", "value": yes_no_unknown(safe_status_flag(device_status, "scc_firmware_updated")), "state": safe_status_flag(device_status, "scc_firmware_updated")},
        {"label": "Spannung stabil", "value": yes_no_unknown(safe_status_flag(device_status, "battery_voltage_to_steady")), "state": safe_status_flag(device_status, "battery_voltage_to_steady")},
        {"label": "Warnstatus", "value": "OK" if warnings_all_zero else ("Warnung" if warnings_raw else "Unbekannt"), "state": True if warnings_all_zero else (False if warnings_raw else None)},
    ]

    settings_left = [
        {"label": "Protokoll", "value": data.get("protocol_id") or "PI30"},
        {"label": "Gerät", "value": data.get("device_name") or raw.get("device_name") or "WR1"},
        {"label": "Modus", "value": mode_text},
        {"label": "Output priority", "value": output_priority_label(output_raw)},
        {"label": "Charger priority", "value": charger_priority_label(charger_raw)},
        {"label": "Seriennummer", "value": data.get("device_serial") or "--"},
        {"label": "Statusbits roh", "value": data.get("device_status_bits") or "--"},
        {"label": "Datenalter", "value": f"{parse_float(data.get('data_age_sec'), 1)} s" if parse_float(data.get('data_age_sec'), 1) is not None else "--"},
    ]

    settings_right = [
        {"label": "PV-Spannung", "value": fmt_val(metrics["pv_input_voltage_v"], " V")},
        {"label": "PV-Strom", "value": fmt_val(metrics["pv_input_current_a"], " A")},
        {"label": "Last %", "value": fmt_val(metrics["load_percent"], " %")},
        {"label": "Last VA", "value": fmt_val(metrics["load_va"], " VA")},
        {"label": "Wh heute", "value": fmt_val(metrics["wh_today"], " Wh")},
        {"label": "Flags roh", "value": data.get("flags_raw") or "--"},
    ]

    extra_info = [
        {"label": "Warnungen", "value": warning_text},
        {"label": "Nennleistung", "value": nennleistung or "--"},
        {"label": "Cutoff / Back to grid", "value": f"{fmt_val(cutoff_v, ' V')} / {fmt_val(recharge_v, ' V')}"},
        {"label": "Back to discharge", "value": fmt_val(redischarge_v, ' V')},
        {"label": "Bulk / Float", "value": f"{fmt_val(bulk_v, ' V')} / {fmt_val(float_v, ' V')}"},
        {"label": "Max. AC / Gesamt Ladestrom", "value": max_charge_info or "--"},
    ]

    editable = {
        "output_priority": {
            "id": "output_priority",
            "label": "Output source priority",
            "current_raw": output_raw or "--",
            "current_text": output_priority_label(output_raw),
            "options": [
                {"value": "0", "label": "Netzbetrieb", "enabled": True},
                {"value": "1", "label": "Solar zuerst", "enabled": True},
                {"value": "2", "label": "SBU", "enabled": True},
            ],
        },
        "charger_priority": {
            "id": "charger_priority",
            "label": "Charger source priority",
            "current_raw": charger_raw or "--",
            "current_text": charger_priority_label(charger_raw),
            "options": [
                {"value": "0", "label": "Netz zuerst", "enabled": True},
                {"value": "1", "label": "Solar zuerst (gesperrt)", "enabled": False},
                {"value": "2", "label": "Solar + Netz", "enabled": True},
                {"value": "3", "label": "Nur Solar", "enabled": True},
            ],
        },
        "recharge_voltage": {
            "id": "recharge_voltage",
            "label": "Back to grid voltage",
            "current_raw": str(recharge_v or "--"),
            "current_text": fmt_val(recharge_v, " V"),
            "options": [{"value": f"{v/10:.1f}", "label": f"{v/10:.1f} V", "enabled": True} for v in range(440, 581)]
        },
        "redischarge_voltage": {
            "id": "redischarge_voltage",
            "label": "Back to discharge voltage",
            "current_raw": str(redischarge_v or "--"),
            "current_text": fmt_val(redischarge_v, " V"),
            "options": [
                {"value": "48.0", "label": "48.0 V", "enabled": True},
                {"value": "49.0", "label": "49.0 V", "enabled": True},
                {"value": "50.0", "label": "50.0 V", "enabled": True},
                {"value": "51.0", "label": "51.0 V", "enabled": True},
                {"value": "52.0", "label": "52.0 V", "enabled": True},
                {"value": "53.0", "label": "53.0 V", "enabled": True},
                {"value": "54.0", "label": "54.0 V", "enabled": True},
                {"value": "55.0", "label": "55.0 V", "enabled": True}
            ]
        },
        "cutoff_voltage": {
            "id": "cutoff_voltage",
            "label": "Cut-off voltage",
            "current_raw": str(cutoff_v or "--"),
            "current_text": fmt_val(cutoff_v, " V"),
            "options": [
                {"value": "44.0", "label": "44.0 V", "enabled": True},
                {"value": "44.5", "label": "44.5 V", "enabled": True},
                {"value": "45.0", "label": "45.0 V", "enabled": True},
                {"value": "45.5", "label": "45.5 V", "enabled": True},
                {"value": "46.0", "label": "46.0 V", "enabled": True},
                {"value": "46.5", "label": "46.5 V", "enabled": True},
                {"value": "47.0", "label": "47.0 V", "enabled": True},
                {"value": "47.5", "label": "47.5 V", "enabled": True},
                {"value": "48.0", "label": "48.0 V", "enabled": True},
                {"value": "48.5", "label": "48.5 V", "enabled": True},
                {"value": "49.0", "label": "49.0 V", "enabled": True},
                {"value": "49.5", "label": "49.5 V", "enabled": True},
                {"value": "50.0", "label": "50.0 V", "enabled": True}
            ]
        },
        "bulk_voltage": {
            "id": "bulk_voltage",
            "label": "Bulk voltage",
            "current_raw": str(bulk_v or "--"),
            "current_text": fmt_val(bulk_v, " V"),
            "options": [
                {"value": "54.0", "label": "54.0 V", "enabled": True},
                {"value": "54.1", "label": "54.1 V", "enabled": True},
                {"value": "54.2", "label": "54.2 V", "enabled": True},
                {"value": "54.3", "label": "54.3 V", "enabled": True},
                {"value": "54.4", "label": "54.4 V", "enabled": True},
                {"value": "54.5", "label": "54.5 V", "enabled": True},
                {"value": "54.6", "label": "54.6 V", "enabled": True},
                {"value": "54.7", "label": "54.7 V", "enabled": True},
                {"value": "54.8", "label": "54.8 V", "enabled": True},
                {"value": "54.9", "label": "54.9 V", "enabled": True},
                {"value": "55.0", "label": "55.0 V", "enabled": True},
                {"value": "55.1", "label": "55.1 V", "enabled": True},
                {"value": "55.2", "label": "55.2 V", "enabled": True},
                {"value": "55.3", "label": "55.3 V", "enabled": True},
                {"value": "55.4", "label": "55.4 V", "enabled": True},
                {"value": "55.5", "label": "55.5 V", "enabled": True}
            ]
        },
        "float_voltage": {
            "id": "float_voltage",
            "label": "Float voltage",
            "current_raw": str(float_v or "--"),
            "current_text": fmt_val(float_v, " V"),
            "options": [{"value": f"{v/10:.1f}", "label": f"{v/10:.1f} V", "enabled": True} for v in range(480, 561)]
        },
    }

    recommendation = f"Aktueller WR1 Status: Modus {mode_text}"
    if metrics["pv_input_power_w"] is not None and metrics["load_watt"] is not None:
        recommendation += f" | PV {metrics['pv_input_power_w']} W | Last {metrics['load_watt']} W"
    if warnings_all_zero:
        recommendation += " | Keine Warnungen"
    if output_raw is not None:
        recommendation += f" | Output: {output_priority_label(output_raw)}"
    if charger_raw is not None:
        recommendation += f" | Charger: {charger_priority_label(charger_raw)}"

    footer_parts = []
    footer_parts.append(f"Modus: {mode_text}")
    footer_parts.append("Write verifiziert: POP00, POP01, POP02, PCP00, PCP02, PCP03, PBCV, PBDV, PSDV, PCVV, PBFT")
    footer_parts.append("Lesen verifiziert: QMOD, QPIGS, QID, QPIRI, QPIWS, QFLAG")
    if parse_float(data.get("data_age_sec"), 1) is not None:
        footer_parts.append(f"Datenalter: {parse_float(data.get('data_age_sec'), 1)} s")
    if parse_int(data.get("field_count")) is not None:
        footer_parts.append(f"Felder: {parse_int(data.get('field_count'))}")

    state = {
        "device_name": data.get("device_name") or raw.get("device_name") or "WR1",
        "current_values": {
            "battery_cutoff_voltage_v": parse_float(data.get("battery_cutoff_voltage_v"), 1),
            "piri_battery_cutoff_voltage_v": parse_float(data.get("piri_battery_cutoff_voltage_v"), 1),
            "battery_recharge_voltage_v": parse_float(data.get("piri_battery_recharge_voltage_v"), 1),
            "battery_redischarge_voltage_v": parse_float(data.get("piri_battery_redischarge_voltage_v"), 1),
            "battery_redischarge_voltage_2_v": parse_float(data.get("piri_battery_redischarge_voltage_2_v"), 1),
            "bulk_voltage_v": parse_float(data.get("piri_bulk_voltage_v"), 1),
            "float_voltage_v": parse_float(data.get("piri_float_voltage_v"), 1),
        },
        "raw_data": {
            "piri_battery_cutoff_voltage_v": data.get("piri_battery_cutoff_voltage_v"),
            "piri_battery_recharge_voltage_v": data.get("piri_battery_recharge_voltage_v"),
            "piri_battery_redischarge_voltage_v": data.get("piri_battery_redischarge_voltage_v"),
            "piri_battery_redischarge_voltage_2_v": data.get("piri_battery_redischarge_voltage_2_v"),
            "piri_bulk_voltage_v": data.get("piri_bulk_voltage_v"),
            "piri_float_voltage_v": data.get("piri_float_voltage_v"),
        },
        "timestamp": timestamp,
        "online": online,
        "online_text": online_text,
        "protocol_id": data.get("protocol_id") or "PI30",
        "mode_code": mode_code,
        "mode_text": mode_text,
        "ok": ok1,
        "read_ok": ok2,
        "stale": stale,
        "stale_reason": data.get("stale_reason") or "",
        "field_count": parse_int(data.get("field_count")),
        "metrics": metrics,
        "top_status": top_status,
        "settings_left": settings_left,
        "settings_right": settings_right,
        "extra_info": extra_info,
        "editable": editable,
        "recommendation_text": recommendation,
        "footer_info": " | ".join(footer_parts),
    }

    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    tmp = OUTPUT_JSON + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, OUTPUT_JSON)


if __name__ == "__main__":
    main()

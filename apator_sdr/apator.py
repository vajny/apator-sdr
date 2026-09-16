#!/usr/bin/env python3
"""Decode Apator Metra E-ITN 30 / E-RM 30, listen, serve a web UI, publish MQTT."""

from __future__ import annotations

import json
import os
import queue
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

IBM = bytes(
    [
        0xFF, 0xE1, 0x1D, 0x9A, 0xED, 0x85, 0x33, 0x24, 0xEA, 0x7A,
        0xD2, 0x39, 0x70, 0x97, 0x57, 0x0A, 0x54, 0x7D, 0x2D, 0xD8,
        0x6D, 0x0D,
    ]
)
NIBBLE = bytes([0x0, 0x7, 0xF, 0x9, 0xE, 0xD, 0x3, 0x4, 0x2, 0x6, 0xC, 0xB, 0x1, 0x8, 0xA, 0x5])
NIBBLE_INV = bytes([NIBBLE.index(i) for i in range(16)])

HERE = Path(__file__).resolve().parent
LOG = HERE / "readings.jsonl"
if Path("/data").is_dir():
    LOG = Path("/data/readings.jsonl")

DEVICES: dict[int, dict] = {}
STATE: dict[int, dict] = {}
STATE_LOCK = threading.Lock()
SSE: list[queue.Queue] = []
SETTINGS: dict = {}
MQTT_SOCK = None
MQTT_LOCK = threading.Lock()
HA_ANNOUNCED: set[int] = set()
SEEN: dict[int, tuple] = {}
LOG_LEVEL: dict[str, float] = {}
RADIO: dict = {}
EVENTS: deque = deque()
RADIO_MQTT_AT = 0.0
HA_RADIO = False
WINDOW_S = 900


def on_air_id(raw_id: int, meta: dict) -> int:
    # E-RM potisk (3018…) vs ID ve vzduchu (7044…): totéž pole XOR 0x38000000.
    if meta.get("model") == "E-RM30" and raw_id < 0x20000000:
        return raw_id ^ 0x38000000
    return raw_id


def parse_serial(text) -> int | None:
    s = str(text or "").strip().replace(" ", "")
    if not s:
        return None
    if "/" in s:
        s = s.split("/", 1)[0]
    if not s.isdigit():
        return None
    return int(s, 10)


def _model_of(item: dict) -> str:
    raw = str(item.get("model") or item.get("kind") or item.get("type") or "")
    return {
        "topení": "E-ITN30",
        "topeni": "E-ITN30",
        "vodoměr": "E-RM30",
        "vodomer": "E-RM30",
        "itn": "E-ITN30",
        "erm": "E-RM30",
        "E-ITN 30.2": "E-ITN30",
        "E-RM 30": "E-RM30",
        "E-ITN30": "E-ITN30",
        "E-RM30": "E-RM30",
    }.get(raw, "E-ITN30" if raw not in ("E-RM30",) else raw)


def meta_from_row(serial: int, item: dict) -> dict:
    model = _model_of(item)
    name = str(item.get("name") or "").strip() or str(serial)
    print_s = str(item.get("print") or item.get("serial") or serial).strip()
    return {"model": model, "name": name, "print": print_s}


def printed_serial(rec: dict) -> str:
    if rec.get("print"):
        return str(rec["print"])
    ident = rec.get("id")
    if ident is None:
        return ""
    ident = int(ident)
    if rec.get("model") == "E-RM30" and ident >= 0x20000000:
        return str(ident ^ 0x38000000)
    return str(ident)


def load_devices() -> None:
    DEVICES.clear()
    merged: dict[str, dict] = {}
    opts = Path("/data/options.json")
    if opts.exists():
        try:
            raw = json.loads(opts.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            raw = {}
        if isinstance(raw, dict) and isinstance(raw.get("devices"), list):
            for item in raw["devices"]:
                if not isinstance(item, dict):
                    continue
                serial = parse_serial(item.get("serial") or item.get("id") or "")
                if serial is None:
                    continue
                merged[str(serial)] = meta_from_row(serial, item)
    for p in (HERE / "devices.json", Path("/data/devices.json"), Path("/config/devices.json")):
        if not p.exists():
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            merged.update(data)
    for k, v in merged.items():
        serial = parse_serial(k)
        if serial is None or not isinstance(v, dict):
            continue
        meta = meta_from_row(serial, v)
        DEVICES[on_air_id(serial, meta)] = meta


def devices_as_options() -> list[dict]:
    rows = []
    for ident, meta in DEVICES.items():
        serial = str(meta.get("print") or ident)
        rows.append({
            "serial": serial,
            "name": meta.get("name") or serial,
            "model": meta.get("model") or "E-ITN30",
        })
    return rows


def _opt(opts: dict, key: str, fallback):
    v = opts.get(key)
    if v is None or v == "":
        v = fallback
    return v


def persist_devices() -> None:
    rows: dict[str, dict] = {}
    for ident, meta in DEVICES.items():
        serial = str(parse_serial(meta.get("print")) or ident)
        rows[serial] = {
            "model": meta.get("model") or "E-ITN30",
            "name": meta.get("name") or serial,
            "print": str(meta.get("print") or serial),
        }
    blob = json.dumps(rows, ensure_ascii=False, indent=2) + "\n"
    targets = [HERE / "devices.json"]
    if Path("/config").is_dir():
        targets = [Path("/config/devices.json")]
    for p in targets:
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(blob, encoding="utf-8")
        except OSError as e:
            print(f"devices.json: {e}", flush=True)
    tok = os.environ.get("SUPERVISOR_TOKEN")
    if not tok:
        return
    opts_path = Path("/data/options.json")
    opts: dict = {}
    if opts_path.exists():
        try:
            opts = json.loads(opts_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            opts = {}
    inner = opts.get("options")
    if isinstance(inner, dict):
        opts = {**opts, **inner}
    body = json.dumps({
        "options": {
            "frequency": _opt(opts, "frequency", SETTINGS.get("frequency") or "868.95M"),
            "gain": _opt(opts, "gain", SETTINGS.get("gain") or "19.2"),
            "sample_rate": _opt(opts, "sample_rate", SETTINGS.get("sample_rate") or "1024k"),
            "devices": devices_as_options(),
        }
    }).encode()
    req = urllib.request.Request(
        "http://supervisor/addons/self/options",
        data=body,
        method="POST",
        headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            r.read()
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"HA options: {e}", flush=True)


def register_device(item: dict) -> dict | None:
    serial = parse_serial(item.get("serial") or item.get("id") or item.get("print") or "")
    if serial is None:
        return None
    meta = meta_from_row(serial, item)
    ident = on_air_id(serial, meta)
    DEVICES[ident] = meta
    with STATE_LOCK:
        if ident in STATE:
            STATE[ident] = annotate(dict(STATE[ident]))
    persist_devices()
    rec = STATE.get(ident)
    if rec:
        mqtt_send(rec)
    return {"id": ident, **meta}


def crc16(data: bytes, poly: int = 0x8005, init: int = 0xFFFF) -> int:
    c = init
    for b in data:
        c ^= b << 8
        for _ in range(8):
            c = ((c << 1) ^ poly) & 0xFFFF if c & 0x8000 else (c << 1) & 0xFFFF
    return c


def _sub_nibbles(src: bytes, table: bytes) -> bytes:
    out = bytearray(len(src))
    for i, b in enumerate(src):
        out[i] = (table[b >> 4] << 4) | table[b & 0x0F]
    return bytes(out)


def _unwhiten(frame: bytes) -> bytes:
    return bytes(b ^ IBM[i] for i, b in enumerate(frame[: len(IBM)]))


def _parse_date(u16: int) -> str:
    day = u16 & 0x1F
    month = (u16 >> 5) & 0x0F
    year = 2000 + ((u16 >> 9) & 0x7F)
    return f"{year:04d}-{month:02d}-{day:02d}"


def decode_frame(frame: bytes) -> dict | None:
    if len(frame) < 20:
        return None
    uw = _unwhiten(frame)
    length = uw[0]
    if length not in (0x11, 0x13) or length + 3 > len(uw):
        return None
    got = (uw[length + 1] << 8) | uw[length + 2]
    calc = crc16(uw[: length + 1])
    payload = _sub_nibbles(uw[1 : length + 1], NIBBLE)
    rec = {
        "len": length,
        "crc_ok": got == calc,
        "raw": frame[: length + 3].hex(),
        "payload": payload.hex(),
    }
    if length == 0x11:
        rec.update(
            {
                "model": "E-ITN30",
                "id": (payload[3] << 24 | payload[2] << 16 | payload[1] << 8 | payload[0]) ^ 0x38000000,
                "current": payload[11] << 8 | payload[10],
                "last_year": payload[5] << 8 | payload[4],
                "date": _parse_date(payload[13] << 8 | payload[12]),
                "extra": payload[6:10].hex() + payload[14:].hex(),
            }
        )
    else:
        vol_raw = ((payload[7] << 24 | payload[6] << 16 | payload[5] << 8 | payload[4]) & 0x0FFFFFFF) >> 3
        rec.update(
            {
                "model": "E-RM30",
                "id": (payload[3] << 24 | payload[2] << 16 | payload[1] << 8 | payload[0]) ^ 0x30000000,
                "volume_m3": round(vol_raw / 1000.0, 3),
                "date": _parse_date(payload[16] << 8 | payload[15]),
            }
        )
    return rec


def repair_known(frame: bytes) -> dict | None:
    nbits = min(len(frame), 22) * 8
    known = set(DEVICES)

    def apply(bits: tuple[int, ...]) -> dict | None:
        buf = bytearray(frame)
        for bit in bits:
            buf[bit // 8] ^= 1 << (7 - (bit % 8))
        rec = decode_frame(bytes(buf))
        if rec and rec["crc_ok"] and rec["id"] in known:
            if DEVICES[rec["id"]].get("model") in (None, rec["model"]):
                rec["repaired_bits"] = list(bits)
                return rec
        return None

    for a in range(nbits):
        hit = apply((a,))
        if hit:
            return hit
    for a in range(nbits):
        for b in range(a + 1, nbits):
            hit = apply((a, b))
            if hit:
                return hit
    return None


def annotate(rec: dict) -> dict:
    ident = int(rec["id"])
    info = DEVICES.get(ident)
    if info:
        rec["code"] = info.get("code")
        rec["print"] = info.get("print")
        rec["name"] = info.get("name")
        rec["configured"] = True
    else:
        rec["configured"] = False
        rec.setdefault("print", printed_serial(rec))
    return rec


def encode_frame(model: str, **fields) -> bytes:
    if model == "E-ITN30":
        length = 0x11
        y, m, d = (int(x) for x in fields["date"].split("-"))
        date = ((y - 2000) << 9) | (m << 5) | d
        ident = fields["id"] ^ 0x38000000
        payload = bytearray(length)
        payload[0:4] = ident.to_bytes(4, "little")
        payload[4:6] = int(fields["last_year"]).to_bytes(2, "little")
        payload[10:12] = int(fields["current"]).to_bytes(2, "little")
        payload[12:14] = date.to_bytes(2, "little")
    elif model == "E-RM30":
        length = 0x13
        y, m, d = (int(x) for x in fields["date"].split("-"))
        date = ((y - 2000) << 9) | (m << 5) | d
        ident = fields["id"] ^ 0x30000000
        vol = int(round(float(fields["volume_m3"]) * 1000)) << 3
        payload = bytearray(length)
        payload[0:4] = ident.to_bytes(4, "little")
        payload[4:8] = vol.to_bytes(4, "little")
        payload[15:17] = date.to_bytes(2, "little")
    else:
        raise ValueError(model)
    coded = _sub_nibbles(payload, NIBBLE_INV)
    inner = bytes([length]) + coded
    c = crc16(inner)
    uw = inner + bytes([c >> 8, c & 0xFF])
    return bytes(b ^ IBM[i] for i, b in enumerate(uw))


def codes_text(msg: dict) -> str | None:
    codes = msg.get("codes")
    if isinstance(codes, list) and codes:
        codes = codes[0]
    if isinstance(codes, str) and "{" in codes:
        return codes
    rows = msg.get("rows") or []
    if rows and rows[0].get("data"):
        n = rows[0].get("len")
        data = rows[0]["data"]
        return f"{{{n}}}{data}" if n else str(data)
    if isinstance(codes, str) and codes:
        return codes
    data = msg.get("data")
    return str(data) if data else None


def bits_from_codes(codes: str) -> str:
    s = str(codes).strip().strip("[]'\"")
    nbits = None
    if "}" in s:
        pre, s = s.split("}", 1)
        digits = "".join(ch for ch in pre if ch.isdigit())
        if digits:
            nbits = int(digits)
    s = "".join(ch for ch in s if ch in "0123456789abcdefABCDEF")
    if not s:
        return ""
    bits = bin(int(s, 16))[2:].zfill(len(s) * 4)
    return bits[:nbits] if nbits is not None else bits


def decode_bits(bits: str) -> dict | None:
    best = None
    if len(bits) < 176:
        bits = bits + "0" * (176 - len(bits))
    max_bytes = min(22, len(bits) // 8)
    if max_bytes < 20:
        return None
    for off in range(min(8, len(bits) - 160 + 1)):
        for nbytes in (20, 22):
            if off + nbytes * 8 > len(bits):
                continue
            chunk = bits[off : off + nbytes * 8]
            if any(c not in "01" for c in chunk):
                continue
            frame = int(chunk, 2).to_bytes(nbytes, "big")
            rec = decode_frame(frame)
            if not rec:
                continue
            rec["bit_offset"] = off
            if rec["crc_ok"]:
                return annotate(rec)
            fixed = repair_known(frame)
            if fixed:
                fixed["bit_offset"] = off
                return annotate(fixed)
            if best is None:
                best = rec
    return annotate(best) if best else None


def _num(msg: dict, *keys):
    for k in keys:
        v = msg.get(k)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return None


def apply_level(msg: dict, rec: dict) -> None:
    rssi = _num(msg, "rssi", "RSSI", "rssi_db")
    snr = _num(msg, "snr", "SNR", "snr_db")
    noise = _num(msg, "noise", "noise_db")
    if rssi is None:
        rssi = LOG_LEVEL.get("rssi")
    if snr is None:
        snr = LOG_LEVEL.get("snr")
    if rssi is not None:
        rec["rssi"] = rssi
    if snr is not None:
        rec["snr"] = snr
    if noise is not None:
        rec["noise"] = noise
    f1 = _num(msg, "freq1")
    f2 = _num(msg, "freq2")
    if f1 is not None:
        rec["freq1"] = f1
    if f2 is not None:
        rec["freq2"] = f2


def note_log_level(line: str) -> None:
    s = line.strip()
    m = re.match(r"(rssi|snr)\s*[:=]\s*(-?[\d.]+)", s, re.I)
    if m:
        LOG_LEVEL[m.group(1).lower()] = float(m.group(2))
    m = re.search(r"Found (.+?) tuner", s, re.I)
    if m:
        RADIO["tuner"] = m.group(1).strip()
    m = re.search(r"Tuner gain set to\s*(.+)", s, re.I)
    if m:
        txt = m.group(1).rstrip(".")
        print("aktivní zisk:", txt, flush=True)
        RADIO["gain_text"] = txt
        gm = re.search(r"(-?[\d.]+)", txt)
        if gm:
            RADIO["gain_db"] = float(gm.group(1))
        return
    m = re.search(r"Set initial gain for FC0012 to\s*(.+)", s, re.I)
    if m:
        print("aktivní zisk (FC0012 start):", m.group(1).rstrip("."), flush=True)
    note_auto_level(s)


def note_auto_level(s: str) -> None:
    m = re.search(
        r"Current noise level\s*(-?[\d.]+)\s*dB.*?estimated noise\s*(-?[\d.]+)\s*dB",
        s,
        re.I,
    )
    if m:
        RADIO["level"] = float(m.group(1))
        RADIO["noise"] = float(m.group(2))
        mqtt_radio_maybe()
        return
    m = re.search(
        r"Estimated noise level is\s*(-?[\d.]+)\s*dB.*?minimum detection level to\s*(-?[\d.]+)\s*dB",
        s,
        re.I,
    )
    if m:
        RADIO["noise"] = float(m.group(1))
        RADIO["threshold"] = float(m.group(2))
        mqtt_radio_maybe()


def radio_hit(kind: str, ident: int | None = None, snr=None) -> None:
    EVENTS.append((time.time(), kind, ident, snr))
    now = time.time()
    while EVENTS and now - EVENTS[0][0] > WINDOW_S:
        EVENTS.popleft()
    mqtt_radio_maybe()


def radio_snapshot() -> dict:
    now = time.time()
    while EVENTS and now - EVENTS[0][0] > WINDOW_S:
        EVENTS.popleft()
    ok = fail = junk = 0
    ids: set[int] = set()
    snrs: list[float] = []
    for _ts, kind, ident, snr in EVENTS:
        if kind == "ok":
            ok += 1
            if ident is not None:
                ids.add(ident)
        elif kind == "fail":
            fail += 1
        else:
            junk += 1
        if snr is not None:
            try:
                snrs.append(float(snr))
            except (TypeError, ValueError):
                pass
    n = ok + fail
    fail_pct = round(100.0 * fail / n, 1) if n else 0.0
    total = ok + fail + junk
    ppm = round(total / (WINDOW_S / 60.0), 2)
    noise = RADIO.get("noise")
    if noise is None:
        band = "—"
    elif noise >= -20:
        band = "přebuzené"
    elif noise >= -30 or fail_pct >= 40 or len(ids) >= 8:
        band = "rušné"
    else:
        band = "klidné"
    return {
        "noise_db": RADIO.get("noise"),
        "threshold_db": RADIO.get("threshold"),
        "level_db": RADIO.get("level"),
        "gain_db": RADIO.get("gain_db"),
        "gain_asked": RADIO.get("gain_asked"),
        "gain_text": RADIO.get("gain_text"),
        "tuner": RADIO.get("tuner"),
        "band": band,
        "ppm": ppm,
        "crc_ok": ok,
        "crc_fail": fail,
        "undecoded": junk,
        "fail_pct": fail_pct,
        "unique": len(ids),
        "snr_avg": round(sum(snrs) / len(snrs), 1) if snrs else None,
        "window_min": WINDOW_S // 60,
    }


def decode_native(msg: dict) -> dict | None:
    model = msg.get("model", "")
    if model == "ApatorMetra-ERM30":
        return {
            "model": "E-RM30",
            "id": int(msg["id"]),
            "volume_m3": float(msg["volume_m3"]),
            "date": msg["date"],
            "crc_ok": True,
            "src": "rtl_433",
        }
    if model == "ApatorMetra-EITN30":
        return {
            "model": "E-ITN30",
            "id": int(msg["id"]),
            "current": int(msg.get("current_heating", msg.get("current", 0))),
            "last_year": int(msg.get("last_yr_heating", msg.get("last_year", 0))),
            "date": msg["date"],
            "crc_ok": True,
            "src": "rtl_433",
        }
    return None


def handle_rtl_line(line: str) -> dict | None:
    line = line.strip()
    if not line.startswith("{"):
        return None
    try:
        msg = json.loads(line)
    except json.JSONDecodeError:
        return None
    native = decode_native(msg)
    if native:
        apply_level(msg, native)
        native["time"] = msg.get("time")
        return annotate(native)
    if msg.get("model") not in ("Apator", "Apator_flex", "ApatorMetra-ERM30", "ApatorMetra-EITN30"):
        return None
    codes = codes_text(msg)
    if not codes:
        return None
    rec = decode_bits(bits_from_codes(codes))
    if not rec:
        return None
    apply_level(msg, rec)
    rec["time"] = msg.get("time")
    rec["src"] = "flex"
    rec["codes"] = codes
    return rec


def _fmt_bits(rec: dict) -> str:
    parts = []
    if rec.get("snr") is not None:
        parts.append(f"SNR {rec['snr']}")
    if rec.get("rssi") is not None:
        parts.append(f"RSSI {rec['rssi']}")
    if rec.get("noise") is not None:
        parts.append(f"noise {rec['noise']}")
    if rec.get("freq1") is not None or rec.get("freq2") is not None:
        parts.append(f"f={rec.get('freq1') or '?'}|{rec.get('freq2') or '?'}")
    if rec.get("src"):
        parts.append(str(rec["src"]))
    if rec.get("bit_offset"):
        parts.append(f"off={rec['bit_offset']}")
    if rec.get("repaired_bits"):
        parts.append(f"oprava {len(rec['repaired_bits'])}b")
    if rec.get("dup"):
        parts.append("dup")
    if rec.get("codes") and not rec.get("crc_ok"):
        raw = str(rec["codes"]).replace("\n", "")
        parts.append(f"raw={raw[:52]}")
    return ("  " + "  ".join(parts)) if parts else ""


def fmt(rec: dict) -> str:
    t = rec.get("time") or datetime.now().strftime("%H:%M:%S")
    if rec.get("repaired_bits"):
        crc = "CRC opraven"
    elif rec.get("crc_ok"):
        crc = "CRC ok"
    else:
        crc = "CRC fail"
    ident = rec.get("id")
    printed = rec.get("print") or printed_serial(rec)
    ids = str(ident)
    if printed and str(printed) not in ("", str(ident)):
        ids = f"{ident}  {printed}"
    name = rec.get("name") or rec.get("code") or ""
    label = f"  {name}" if name else ""
    extra = _fmt_bits(rec)
    if rec["model"] == "E-ITN30":
        return (
            f"{t}  E-ITN {ids}{label}  "
            f"náměr {rec.get('current')}  (loni {rec.get('last_year')})  {rec.get('date')}  {crc}{extra}"
        )
    return f"{t}  E-RM {ids}{label}  {rec.get('volume_m3')} m3  {rec.get('date')}  {crc}{extra}"


def fmt_undecoded(line: str) -> str | None:
    try:
        msg = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(msg, dict):
        return None
    codes = codes_text(msg) or ""
    snip = codes[:52] + ("…" if len(codes) > 52 else "")
    rssi = _num(msg, "rssi", "RSSI")
    snr = _num(msg, "snr", "SNR")
    t = msg.get("time") or ""
    model = msg.get("model") or "?"
    bits = [f"json {model}"]
    if snip:
        bits.append(snip)
    if snr is not None:
        bits.append(f"SNR {snr}")
    if rssi is not None:
        bits.append(f"RSSI {rssi}")
    bits.append("nešlo dekódovat")
    return f"{t}  " + "  ".join(bits) if t else "  ".join(bits)


def append_log(rec: dict) -> None:
    rec = dict(rec)
    rec.setdefault("heard", datetime.now(timezone.utc).isoformat())
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def load_latest() -> None:
    if not LOG.exists():
        return
    for line in LOG.read_text(encoding="utf-8").splitlines():
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if rec.get("crc_ok") and rec.get("id") is not None:
            STATE[int(rec["id"])] = annotate(rec)


def snapshot() -> dict:
    with STATE_LOCK:
        devices = {}
        for ident, meta in DEVICES.items():
            rec = STATE.get(ident)
            if rec:
                devices[str(ident)] = rec
            else:
                devices[str(ident)] = annotate(
                    {"id": ident, "model": meta.get("model"), "crc_ok": None}
                )
        discovery = {}
        for ident, rec in STATE.items():
            if ident in DEVICES:
                continue
            extra = dict(rec)
            extra["configured"] = False
            extra.setdefault("print", printed_serial(extra))
            discovery[str(ident)] = extra
    updated = None
    for rec in list(devices.values()) + list(discovery.values()):
        t = rec.get("time") or rec.get("heard")
        if t and (updated is None or t > updated):
            updated = t
    return {
        "updated": updated,
        "devices": devices,
        "discovery": discovery,
        "configured": len(DEVICES),
        "radio": radio_snapshot(),
    }


def update_state(rec: dict) -> None:
    if not rec.get("crc_ok"):
        return
    rec = dict(rec)
    ident = int(rec["id"])
    with STATE_LOCK:
        prev = STATE.get(ident)
        if prev:
            if rec.get("rssi") is None:
                rec["rssi"] = prev.get("rssi")
            if rec.get("snr") is None:
                rec["snr"] = prev.get("snr")
        STATE[ident] = rec
    for q in list(SSE):
        try:
            q.put_nowait(rec)
        except queue.Full:
            pass


def _mqtt_len(n: int) -> bytes:
    out = bytearray()
    while True:
        digit = n % 128
        n //= 128
        if n:
            digit |= 0x80
        out.append(digit)
        if not n:
            break
    return bytes(out)


def _mqtt_str(s: str) -> bytes:
    b = s.encode()
    return len(b).to_bytes(2, "big") + b


def mqtt_connect() -> socket.socket | None:
    host = SETTINGS.get("mqtt_host") or ""
    if not host:
        return None
    port = int(SETTINGS.get("mqtt_port") or 1883)
    user = SETTINGS.get("mqtt_user") or ""
    password = SETTINGS.get("mqtt_password") or ""
    flags = 0x02
    payload = _mqtt_str("apator-sdr")
    if user:
        flags |= 0x80
        payload += _mqtt_str(user)
        if password:
            flags |= 0x40
            payload += _mqtt_str(password)
    vh = b"\x00\x04MQTT\x04" + bytes([flags]) + (180).to_bytes(2, "big")
    rem = vh + payload
    pkt = bytes([0x10]) + _mqtt_len(len(rem)) + rem
    sock = socket.create_connection((host, port), 5)
    sock.settimeout(5)
    sock.sendall(pkt)
    resp = sock.recv(4)
    if not resp or resp[0] != 0x20 or (len(resp) > 3 and resp[3] != 0):
        sock.close()
        raise OSError(f"MQTT CONNACK {resp!r}")
    return sock


def _mqtt_drop(err: BaseException | None = None) -> None:
    global MQTT_SOCK
    if err is not None:
        print(f"mqtt: {err}", flush=True)
    try:
        if MQTT_SOCK:
            MQTT_SOCK.close()
    except OSError:
        pass
    MQTT_SOCK = None


def mqtt_publish(topic: str, payload: str, retain: bool = True) -> bool:
    global MQTT_SOCK
    if not SETTINGS.get("mqtt_host"):
        return False
    body = _mqtt_str(topic) + payload.encode()
    header = 0x30 | (1 if retain else 0)
    pkt = bytes([header]) + _mqtt_len(len(body)) + body
    with MQTT_LOCK:
        for _ in range(2):
            try:
                if MQTT_SOCK is None:
                    MQTT_SOCK = mqtt_connect()
                if MQTT_SOCK is None:
                    return False
                MQTT_SOCK.sendall(pkt)
                return True
            except OSError as e:
                _mqtt_drop(e)
        return False


def mqtt_keepalive_loop() -> None:
    global MQTT_SOCK
    while True:
        time.sleep(45)
        if not SETTINGS.get("mqtt_host"):
            continue
        with MQTT_LOCK:
            try:
                if MQTT_SOCK is None:
                    MQTT_SOCK = mqtt_connect()
                    continue
                MQTT_SOCK.sendall(b"\xc0\x00")
            except OSError as e:
                _mqtt_drop(e)
        mqtt_radio_maybe()


def ha_announce(rec: dict) -> None:
    ident = int(rec["id"])
    if ident in HA_ANNOUNCED:
        return
    heat = rec["model"] == "E-ITN30"
    name = rec.get("name") or rec.get("code") or str(ident)
    device = {
        "identifiers": [f"apator_{ident}"],
        "name": name,
        "manufacturer": "Apator Metra",
        "model": "E-ITN 30.2" if heat else "E-RM 30",
    }
    state = f"apator/{ident}/state"
    sensors = []
    if heat:
        sensors = [
            ("namer", "Náměr", "{{ value_json.current }}", None, "jedn.", "measurement"),
            ("loni", "Loňský náměr", "{{ value_json.last_year }}", None, "jedn.", "measurement"),
        ]
    else:
        sensors = [
            ("objem", "Objem", "{{ value_json.volume_m3 }}", "water", "m³", "total_increasing"),
        ]
    sensors.append(("rssi", "RSSI", "{{ value_json.rssi }}", "signal_strength", "dB", "measurement"))
    for key, label, tmpl, devclass, unit, sclass in sensors:
        cfg = {
            "name": f"{name} {label}",
            "unique_id": f"apator_{ident}_{key}",
            "state_topic": state,
            "value_template": tmpl,
            "device": device,
            "state_class": sclass,
        }
        if unit:
            cfg["unit_of_measurement"] = unit
        if devclass:
            cfg["device_class"] = devclass
        if not mqtt_publish(f"homeassistant/sensor/apator_{ident}_{key}/config", json.dumps(cfg), True):
            return
    HA_ANNOUNCED.add(ident)


def ha_announce_radio() -> None:
    global HA_RADIO
    if HA_RADIO:
        return
    device = {
        "identifiers": ["apator_sdr_radio"],
        "name": "Apator SDR rádio",
        "manufacturer": "rtl_433",
        "model": "RTL-SDR",
    }
    state = "apator/radio/state"
    sensors = [
        ("sum", "Šum", "{{ value_json.noise_db }}", "dB", "mdi:waveform"),
        ("prah", "Práh detekce", "{{ value_json.threshold_db }}", "dB", "mdi:tune"),
        ("zisk", "Zisk tuneru", "{{ value_json.gain_db }}", "dB", "mdi:volume-equal"),
        ("zisk_config", "Zisk z konfigurace", "{{ value_json.gain_asked }}", "dB", "mdi:cog"),
        ("crc_fail", "CRC fail", "{{ value_json.fail_pct }}", "%", "mdi:alert"),
        ("slyseno", "Slyšených ID", "{{ value_json.unique }}", None, "mdi:access-point"),
        ("telegramy", "Telegramy/min", "{{ value_json.ppm }}", "1/min", "mdi:pulse"),
        ("pasmo", "Pásmo 868", "{{ value_json.band }}", None, "mdi:radio-tower"),
    ]
    for key, label, tmpl, unit, icon in sensors:
        cfg = {
            "name": label,
            "unique_id": f"apator_radio_{key}",
            "state_topic": state,
            "value_template": tmpl,
            "device": device,
            "entity_category": "diagnostic",
            "icon": icon,
        }
        if unit:
            cfg["unit_of_measurement"] = unit
            cfg["state_class"] = "measurement"
        if not mqtt_publish(f"homeassistant/sensor/apator_radio_{key}/config", json.dumps(cfg), True):
            return
    HA_RADIO = True


def mqtt_radio_maybe(force: bool = False) -> None:
    global RADIO_MQTT_AT
    if not SETTINGS.get("mqtt_host"):
        return
    now = time.time()
    if not force and now - RADIO_MQTT_AT < 15:
        return
    ha_announce_radio()
    if not HA_RADIO:
        return
    RADIO_MQTT_AT = now
    mqtt_publish("apator/radio/state", json.dumps(radio_snapshot()), True)


def mqtt_send(rec: dict) -> None:
    if not rec.get("crc_ok"):
        return
    if int(rec["id"]) not in DEVICES:
        return
    ha_announce(rec)
    slim = {
        k: rec.get(k)
        for k in ("id", "model", "name", "code", "print", "current", "last_year", "volume_m3", "date", "rssi", "snr", "time", "crc_ok")
        if rec.get(k) is not None
    }
    mqtt_publish(f"apator/{rec['id']}/state", json.dumps(slim, ensure_ascii=False), True)


def already_seen(rec: dict) -> bool:
    ident = int(rec["id"])
    sig = (
        str(rec.get("time") or "")[:19],
        rec.get("volume_m3"),
        rec.get("current"),
        rec.get("date"),
        rec.get("crc_ok"),
    )
    if SEEN.get(ident) == sig:
        return True
    SEEN[ident] = sig
    return False


def on_packet(rec: dict) -> None:
    rec = dict(rec)
    dup = already_seen(rec)
    if dup:
        rec["dup"] = True
        print(fmt(rec), flush=True)
        if rec.get("crc_ok") and (rec.get("snr") is not None or rec.get("rssi") is not None):
            update_state(rec)
        return
    print(fmt(rec), flush=True)
    radio_hit(
        "ok" if rec.get("crc_ok") else "fail",
        rec.get("id"),
        rec.get("snr"),
    )
    known = int(rec["id"]) in DEVICES
    if rec.get("crc_ok") or known:
        append_log(rec)
    if rec.get("crc_ok"):
        update_state(rec)
        mqtt_send(rec)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:
        req = str(args[0]) if args else ""
        if any(x in req for x in ("/api/state", "/api/stream", "/health", "GET / HTTP", "GET // HTTP")):
            return
        super().log_message(fmt, *args)

    def _json(self, obj: dict, code: int = 200) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        path = "/" + "/".join(p for p in self.path.split("?", 1)[0].split("/") if p)
        if path in ("/", "/index.html"):
            html = (HERE / "web" / "index.html").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html)))
            self.end_headers()
            self.wfile.write(html)
            return
        if path == "/api/state":
            self._json(snapshot())
            return
        if path == "/api/discovery":
            snap = snapshot()
            self._json({"updated": snap["updated"], "devices": snap["discovery"]})
            return
        if path == "/api/devices":
            self._json({str(k): v for k, v in DEVICES.items()})
            return
        if path == "/health":
            self._json({"ok": True, "devices": len(STATE)})
            return
        if path == "/api/stream":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            q: queue.Queue = queue.Queue(maxsize=8)
            SSE.append(q)
            try:
                self.wfile.write(b"data: {\"hello\":true}\n\n")
                self.wfile.flush()
                while True:
                    try:
                        rec = q.get(timeout=20)
                        self.wfile.write(f"data: {json.dumps({'id': rec.get('id')})}\n\n".encode())
                        self.wfile.flush()
                    except queue.Empty:
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
            except OSError:
                pass
            finally:
                if q in SSE:
                    SSE.remove(q)
            return
        self.send_error(404)

    def do_POST(self) -> None:
        path = "/" + "/".join(p for p in self.path.split("?", 1)[0].split("/") if p)
        if path != "/api/devices":
            self.send_error(404)
            return
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if 0 < n < 65536 else b"{}"
        try:
            item = json.loads(raw.decode())
        except json.JSONDecodeError:
            item = {}
        if not isinstance(item, dict):
            item = {}
        added = register_device(item)
        if not added:
            self._json({"ok": False, "error": "špatné sériové číslo"}, 400)
            return
        out = snapshot()
        out["ok"] = True
        out["device"] = added
        self._json(out)


def start_http() -> ThreadingHTTPServer:
    port = int(SETTINGS.get("web_port") or 8099)
    httpd = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    print(f"web http://127.0.0.1:{port}/", flush=True)
    return httpd


def _mqtt_from_supervisor() -> dict:
    tok = os.environ.get("SUPERVISOR_TOKEN")
    if not tok:
        return {}
    try:
        req = urllib.request.Request(
            "http://supervisor/services/mqtt",
            headers={"Authorization": f"Bearer {tok}"},
        )
        with urllib.request.urlopen(req, timeout=2) as r:
            data = json.loads(r.read().decode()).get("data") or {}
        return {
            "mqtt_host": data.get("host") or "",
            "mqtt_port": int(data.get("port") or 1883),
            "mqtt_user": data.get("username") or "",
            "mqtt_password": data.get("password") or "",
        }
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
        return {}


def load_settings() -> dict:
    s = {
        "frequency": os.environ.get("FREQUENCY", "868.95M"),
        "gain": os.environ.get("GAIN", "19.2"),
        "sample_rate": os.environ.get("SAMPLE_RATE", "1024k"),
        "web_port": int(os.environ.get("WEB_PORT", "8099")),
        "mqtt_host": os.environ.get("MQTT_HOST", ""),
        "mqtt_port": int(os.environ.get("MQTT_PORT", "1883")),
        "mqtt_user": os.environ.get("MQTT_USER", ""),
        "mqtt_password": os.environ.get("MQTT_PASSWORD", ""),
    }
    merge_radio_options(s)
    sup = _mqtt_from_supervisor()
    if not s.get("mqtt_host") and sup.get("mqtt_host"):
        s.update(sup)
    return s


def read_addon_options() -> dict:
    merged: dict = {}
    for p in (Path("/data/options.json"), HERE / "options.json"):
        if not p.exists():
            continue
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if not isinstance(raw, dict):
            continue
        inner = raw.get("options")
        if isinstance(inner, dict):
            merged.update(inner)
        for k, v in raw.items():
            if k != "options":
                merged[k] = v
    return merged


def merge_radio_options(s: dict) -> None:
    raw = read_addon_options()
    for k in ("frequency", "gain", "sample_rate"):
        if k not in raw:
            continue
        v = raw[k]
        if v is None or v == "":
            continue
        s[k] = str(v).strip()


def rtl_gain() -> str:
    g = SETTINGS.get("gain")
    if g is None or g == "":
        return "19.2"
    return str(g).strip()


def gain_is_auto(g: str | None = None) -> bool:
    s = str(g if g is not None else rtl_gain()).strip().lower()
    return s == "auto"


def rtl_gain_arg() -> str | None:
    # rtl_433: atof(g)*10 == 0 → Auto. 0 in HA therefore cannot mean 0 dB.
    # -9.9 is FC0012 minimum; R820T snaps it up to its own floor.
    g = rtl_gain()
    if gain_is_auto(g):
        return None
    s = g.replace(",", ".")
    try:
        if abs(float(s)) < 0.05:
            return "-9.9"
    except ValueError:
        pass
    return g


def rtl_cmd() -> list[str]:
    cmd = [
        "rtl_433",
        "-d", "0",
        "-v",
        "-f", str(SETTINGS.get("frequency") or "868.95M"),
        "-s", str(SETTINGS.get("sample_rate") or "1024k"),
        "-Y", "minmax",
        "-Y", "autolevel",
        "-M", "level",
        "-M", "noise",
        "-R", "0",
        "-R", "277",
        "-X", "n=Apator,m=FSK_PCM,s=25,l=25,r=5000,preamble=aaaa699a",
        "-F", "log",
        "-F", "json",
    ]
    g = rtl_gain_arg()
    if g is not None:
        cmd[4:4] = ["-g", g]
    if shutil.which("stdbuf"):
        cmd = ["stdbuf", "-oL", *cmd]
    return cmd


def self_check() -> None:
    heat = {
        "model": "E-ITN30",
        "id": 31975929,
        "current": 517,
        "last_year": 2605,
        "date": "2026-01-26",
    }
    water = {
        "model": "E-RM30",
        "id": 12345678,
        "volume_m3": 12.345,
        "date": "2026-09-16",
    }
    for src in (heat, water):
        frame = encode_frame(**src)
        got = decode_frame(frame)
        assert got and got["crc_ok"], got
        for k, v in src.items():
            assert got[k] == v, (k, got[k], v)
        bits = bin(int(frame.hex(), 16))[2:].zfill(len(frame) * 8)
        aligned = decode_bits("000" + bits + "1111")
        assert aligned and aligned["crc_ok"] and aligned["id"] == src["id"], aligned
    sample = bytes.fromhex("eec25edb8e003d1584cadf3678f930c1f7bdc6ec")
    pub = decode_frame(sample)
    assert pub and pub["id"] == 31975929 and pub["current"] == 517 and pub["last_year"] == 2605, pub
    prev = dict(DEVICES)
    DEVICES.clear()
    DEVICES[30731042] = {"model": "E-ITN30", "name": "check"}
    try:
        mine = encode_frame(model="E-ITN30", id=30731042, current=263, last_year=258, date="2026-09-16")
        broken = bytearray(mine)
        broken[13 // 8] ^= 1 << (7 - (13 % 8))
        broken[41 // 8] ^= 1 << (7 - (41 % 8))
        fixed = repair_known(bytes(broken))
        assert fixed and fixed["id"] == 30731042 and fixed["crc_ok"], fixed
        live = bytes.fromhex("ee6d56cd8ecd3fca2aa752387cf738c8f0bdf60f")
        live_fix = repair_known(live)
        assert live_fix and live_fix["id"] == 30731042 and live_fix["date"] == "2026-09-16", live_fix
        json_line = (
            '{"time":"2026-09-16T17:43:55","model":"Apator","codes":'
            '["{158}ee6956cd8ef13cbc87b15e0577f7380561bd4bd8"],"rssi":-11.4,"snr":30.8}'
        )
        from_json = handle_rtl_line(json_line)
        assert from_json and from_json["id"] == 30731042, from_json
    finally:
        DEVICES.clear()
        DEVICES.update(prev)
    assert parse_serial("301835244/1022") == 301835244
    water_meta = meta_from_row(301835244, {"serial": "301835244/1022", "name": "Studená", "model": "E-RM30"})
    assert on_air_id(301835244, water_meta) == 704488428 and water_meta["name"] == "Studená"
    assert 301835238 ^ 0x38000000 == 704488422
    assert 301835244 ^ 0x38000000 == 704488428
    assert on_air_id(301835238, {"model": "E-RM30"}) == 704488422
    assert on_air_id(704488422, {"model": "E-RM30"}) == 704488422
    prev_state = dict(STATE)
    STATE.clear()
    held = dict(DEVICES)
    DEVICES.clear()
    STATE[704488604] = {"id": 704488604, "model": "E-RM30", "crc_ok": True, "volume_m3": 1.0}
    snap = snapshot()
    assert "704488604" in snap["discovery"] and "704488604" not in snap["devices"]
    STATE.clear()
    STATE.update(prev_state)
    DEVICES.update(held)
    rec = {"id": 1, "model": "E-RM30", "crc_ok": True, "volume_m3": 1.0}
    update_state(rec)
    update_state({"id": 1, "model": "E-RM30", "crc_ok": True, "volume_m3": 1.0, "snr": 28.4, "rssi": -12.0})
    assert STATE[1]["snr"] == 28.4
    note_log_level("snr      : 21.5 dB")
    note_log_level("rssi     : -18.2 dB")
    tagged = {}
    apply_level({}, tagged)
    assert tagged["snr"] == 21.5 and tagged["rssi"] == -18.2
    LOG_LEVEL.clear()
    STATE.pop(1, None)
    line = fmt(
        {
            "model": "E-RM30",
            "id": 704488422,
            "print": "301835238",
            "name": "Teplá",
            "volume_m3": 90.235,
            "date": "2026-09-16",
            "crc_ok": False,
            "snr": 38.4,
            "rssi": -1.2,
            "src": "flex",
            "codes": "{161}ee6d",
        }
    )
    assert "RSSI -1.2" in line and "CRC fail" in line and "flex" in line and "301835238" in line, line
    miss = fmt_undecoded('{"time":"t","model":"Apator","codes":["{10}abcd"],"rssi":-9}')
    assert miss and "nešlo dekódovat" in miss and "abcd" in miss, miss
    prev_g = SETTINGS.get("gain")
    SETTINGS["gain"] = "0"
    assert rtl_gain_arg() == "-9.9" and "-9.9" in rtl_cmd()
    SETTINGS["gain"] = "auto"
    assert gain_is_auto() and "-g" not in rtl_cmd()
    SETTINGS["gain"] = 18
    assert rtl_gain() == "18" and rtl_gain_arg() == "18"
    note_log_level("Found Fitipower FC0012 tuner")
    note_log_level("SDR: Tuner gain set to 19.200000 dB.")
    assert RADIO.get("tuner", "").startswith("Fitipower") and RADIO.get("gain_db") == 19.2
    if prev_g is None:
        SETTINGS.pop("gain", None)
    else:
        SETTINGS["gain"] = prev_g
    EVENTS.clear()
    RADIO.clear()
    note_log_level("Auto Level: Current noise level -15.5 dB, estimated noise -15.4 dB")
    assert RADIO.get("noise") == -15.4 and RADIO.get("level") == -15.5
    note_log_level("Auto Level: Estimated noise level is -38.7 dB, adjusting minimum detection level to -35.7 dB")
    assert RADIO.get("noise") == -38.7 and RADIO.get("threshold") == -35.7
    EVENTS.clear()
    radio_hit("ok", 704488422, 30.0)
    st = radio_snapshot()
    assert st["crc_ok"] == 1 and st["band"] == "klidné" and st["unique"] == 1
    RADIO["noise"] = -15.0
    assert radio_snapshot()["band"] == "přebuzené"
    assert "radio" in snapshot()
    EVENTS.clear()
    RADIO.clear()
    print("self-check ok", flush=True)


def listen() -> int:
    seen = 0
    if Path("/data").is_dir() and not Path("/dev/bus/usb").exists():
        print("USB v kontejneru není — vypni Protection mode, zkus USB 2.", flush=True)
    try:
        while True:
            merge_radio_options(SETTINGS)
            cmd = rtl_cmd()
            asked = rtl_gain()
            got = rtl_gain_arg()
            try:
                RADIO["gain_asked"] = float(str(asked).replace(",", "."))
            except ValueError:
                RADIO["gain_asked"] = None
            print(f"config gain={asked!r}  options={read_addon_options().get('gain')!r}", flush=True)
            if got is None:
                print("aktivní zisk: Auto", flush=True)
            elif got != asked:
                print(f"aktivní zisk: žádám {got} dB  (0 u rtl_433 je Auto, proto minimum)", flush=True)
            else:
                print(f"aktivní zisk: žádám {got} dB", flush=True)
            print("poslouchám", SETTINGS.get("frequency"), flush=True)
            print(" ", " ".join(cmd), flush=True)
            try:
                proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
                )
            except FileNotFoundError:
                print("rtl_433 chybí v image, čekám 10s", flush=True)
                time.sleep(10)
                continue
            assert proc.stdout is not None
            try:
                while True:
                    line = proc.stdout.readline()
                    if line == "" and proc.poll() is not None:
                        break
                    if not line:
                        continue
                    if not line.lstrip().startswith("{"):
                        note_log_level(line)
                    try:
                        rec = handle_rtl_line(line)
                    except Exception as e:
                        print(f"skip: {e}  {line.strip()[:160]}", flush=True)
                        continue
                    if rec:
                        seen += 1
                        on_packet(rec)
                    elif line.lstrip().startswith("{"):
                        miss = fmt_undecoded(line)
                        if miss:
                            print(miss, flush=True)
                            radio_hit("junk")
                    elif line.strip():
                        print(line.rstrip(), flush=True)
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        proc.kill()
            rc = proc.returncode
            print(f"rtl_433 skončil ({rc}), telegramů {seen}, další pokus za 5s", flush=True)
            time.sleep(5)
    except KeyboardInterrupt:
        print(f"\nstop  telegramů: {seen}  log: {LOG}", flush=True)
        return 0


def main(argv: list[str]) -> int:
    global SETTINGS
    SETTINGS = load_settings()
    load_devices()
    if argv[1:] == ["check"] or argv[1:] == ["--check"]:
        self_check()
        return 0
    if argv[1:]:
        print("usage: apator.py           # listen + web", file=sys.stderr)
        print("       apator.py check     # self-check", file=sys.stderr)
        return 2
    self_check()
    load_latest()
    start_http()
    asked = rtl_gain()
    got = rtl_gain_arg()
    if got is None:
        gain_s = "auto"
    elif got != asked:
        gain_s = f"{asked}→{got} dB"
    else:
        gain_s = f"{got} dB"
    print(
        f"měřáků v konfiguraci: {len(DEVICES)}  gain={gain_s}  "
        f"{SETTINGS.get('frequency')}  {SETTINGS.get('sample_rate')}",
        flush=True,
    )
    for ident, meta in DEVICES.items():
        print(
            f"  {meta.get('print') or ident}  {meta.get('model')}  "
            f"{meta.get('name') or '-'}  id {ident}",
            flush=True,
        )
    if SETTINGS.get("mqtt_host"):
        print(f"mqtt {SETTINGS['mqtt_host']}:{SETTINGS['mqtt_port']}", flush=True)
        threading.Thread(target=mqtt_keepalive_loop, daemon=True).start()
        for rec in list(STATE.values()):
            mqtt_send(rec)
        mqtt_radio_maybe(True)
    else:
        print("mqtt vypnuté (nastav MQTT_HOST nebo HA mosquitto)", flush=True)
    return listen()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

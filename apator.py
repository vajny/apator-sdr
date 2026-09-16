#!/usr/bin/env python3
"""Decode Apator Metra E-ITN 30 / E-RM 30, listen, serve a web UI, publish MQTT."""

from __future__ import annotations

import json
import os
import queue
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
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


def load_devices() -> None:
    DEVICES.clear()
    paths = [HERE / "devices.json", Path("/data/devices.json")]
    merged: dict[str, dict] = {}
    for p in paths:
        if p.exists():
            merged.update(json.loads(p.read_text(encoding="utf-8")))
    for k, v in merged.items():
        DEVICES[int(k)] = v


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
    if info is None:
        best_d, best_id = 99, None
        for kid, meta in DEVICES.items():
            if meta.get("model") and meta["model"] != rec.get("model"):
                continue
            d = (ident ^ kid).bit_count()
            if d < best_d:
                best_d, best_id = d, kid
        rec["id_hamming"] = best_d
        rec["id_guess"] = best_id
        if best_id is not None and best_d <= 2:
            info = DEVICES[best_id]
    else:
        rec["id_hamming"] = 0
    if info:
        rec["code"] = info.get("code")
        rec["print"] = info.get("print")
        rec["name"] = info.get("name")
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
        native["rssi"] = msg.get("rssi")
        native["snr"] = msg.get("snr")
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
    rec["rssi"] = msg.get("rssi")
    rec["snr"] = msg.get("snr")
    rec["time"] = msg.get("time")
    rec["src"] = "flex"
    rec["codes"] = codes
    return rec


def fmt(rec: dict) -> str:
    t = rec.get("time") or datetime.now().strftime("%H:%M:%S")
    if rec.get("repaired_bits"):
        crc = "CRC opraven"
    elif rec.get("crc_ok"):
        crc = "CRC ok"
    else:
        crc = "CRC fail"
    snr = rec.get("snr")
    snr_s = f"  SNR {snr}" if snr is not None else ""
    label = rec.get("name") or rec.get("code") or rec.get("print") or ""
    label_s = f"  {label}" if label else ""
    if rec["model"] == "E-ITN30":
        return (
            f"{t}  E-ITN {rec['id']}{label_s}  "
            f"náměr {rec.get('current')}  (loni {rec.get('last_year')})  {rec.get('date')}  {crc}{snr_s}"
        )
    return f"{t}  E-RM {rec['id']}{label_s}  {rec.get('volume_m3')} m3  {rec.get('date')}  {crc}{snr_s}"


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
        devices = {str(k): v for k, v in STATE.items()}
    updated = None
    for rec in devices.values():
        t = rec.get("time") or rec.get("heard")
        if t and (updated is None or t > updated):
            updated = t
    return {"updated": updated, "devices": devices}


def update_state(rec: dict) -> None:
    if not rec.get("crc_ok"):
        return
    with STATE_LOCK:
        STATE[int(rec["id"])] = rec
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
    vh = b"\x00\x04MQTT\x04" + bytes([flags, 0x00, 0x3C])
    rem = vh + payload
    pkt = bytes([0x10]) + _mqtt_len(len(rem)) + rem
    sock = socket.create_connection((host, port), 5)
    sock.sendall(pkt)
    resp = sock.recv(4)
    if not resp or resp[0] != 0x20 or (len(resp) > 3 and resp[3] != 0):
        sock.close()
        raise OSError(f"MQTT CONNACK {resp!r}")
    return sock


def mqtt_publish(topic: str, payload: str, retain: bool = True) -> None:
    global MQTT_SOCK
    if not SETTINGS.get("mqtt_host"):
        return
    body = _mqtt_str(topic) + payload.encode()
    header = 0x30 | (1 if retain else 0)
    pkt = bytes([header]) + _mqtt_len(len(body)) + body
    with MQTT_LOCK:
        try:
            if MQTT_SOCK is None:
                MQTT_SOCK = mqtt_connect()
            if MQTT_SOCK is None:
                return
            MQTT_SOCK.sendall(pkt)
        except OSError as e:
            print(f"mqtt: {e}", flush=True)
            try:
                if MQTT_SOCK:
                    MQTT_SOCK.close()
            except OSError:
                pass
            MQTT_SOCK = None


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
        mqtt_publish(f"homeassistant/sensor/apator_{ident}_{key}/config", json.dumps(cfg), True)
    HA_ANNOUNCED.add(ident)


def mqtt_send(rec: dict) -> None:
    if not rec.get("crc_ok"):
        return
    ha_announce(rec)
    slim = {
        k: rec.get(k)
        for k in ("id", "model", "name", "code", "print", "current", "last_year", "volume_m3", "date", "rssi", "snr", "time", "crc_ok")
        if rec.get(k) is not None
    }
    mqtt_publish(f"apator/{rec['id']}/state", json.dumps(slim, ensure_ascii=False), True)


def on_packet(rec: dict) -> None:
    print(fmt(rec), flush=True)
    append_log(rec)
    if rec.get("crc_ok"):
        update_state(rec)
        mqtt_send(rec)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:
        if args and str(args[0]).startswith("GET /api/state"):
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
        path = self.path.split("?", 1)[0]
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
    for p in (Path("/data/options.json"), HERE / "options.json"):
        if p.exists():
            raw = json.loads(p.read_text(encoding="utf-8"))
            for k, v in raw.items():
                if v not in (None, ""):
                    s[k] = v
    sup = _mqtt_from_supervisor()
    if not s.get("mqtt_host") and sup.get("mqtt_host"):
        s.update(sup)
    return s


def rtl_cmd() -> list[str]:
    return [
        "rtl_433",
        "-d", "0",
        "-f", str(SETTINGS.get("frequency") or "868.95M"),
        "-s", str(SETTINGS.get("sample_rate") or "1024k"),
        "-g", str(SETTINGS.get("gain") or "19.2"),
        "-Y", "minmax",
        "-Y", "autolevel",
        "-Y", "minsnr=10",
        "-R", "277",
        "-X", "n=Apator,m=FSK_PCM,s=25,l=25,r=5000,preamble=aaaa699a",
        "-M", "level",
        "-M", "time:iso",
        "-F", "json",
    ]


def self_check() -> None:
    load_devices()
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
    print("self-check ok", flush=True)


def listen() -> int:
    json_path = HERE / "rtl.jsonl"
    if Path("/data").is_dir():
        json_path = Path("/data/rtl.jsonl")
    json_path.write_text("")
    cmd = rtl_cmd()
    cmd[cmd.index("-F") + 1] = f"json:{json_path}"
    print("poslouchám", SETTINGS.get("frequency"), flush=True)
    print(" ", " ".join(cmd), flush=True)
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    seen = 0
    try:
        with json_path.open(encoding="utf-8", errors="replace") as f:
            while True:
                if proc.poll() is not None:
                    rest = f.read()
                    for line in rest.splitlines():
                        rec = handle_rtl_line(line)
                        if rec:
                            seen += 1
                            on_packet(rec)
                    break
                line = f.readline()
                if not line:
                    time.sleep(0.25)
                    continue
                try:
                    rec = handle_rtl_line(line)
                except Exception as e:
                    print(f"skip: {e}", flush=True)
                    continue
                if rec:
                    seen += 1
                    on_packet(rec)
    except KeyboardInterrupt:
        print("\nstop", flush=True)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
    print(f"telegramů: {seen}  log: {LOG}", flush=True)
    return 0


def main(argv: list[str]) -> int:
    global SETTINGS
    load_devices()
    SETTINGS = load_settings()
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
    if SETTINGS.get("mqtt_host"):
        print(f"mqtt {SETTINGS['mqtt_host']}:{SETTINGS['mqtt_port']}", flush=True)
        for rec in list(STATE.values()):
            mqtt_send(rec)
    else:
        print("mqtt vypnuté (nastav MQTT_HOST nebo HA mosquitto)", flush=True)
    return listen()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

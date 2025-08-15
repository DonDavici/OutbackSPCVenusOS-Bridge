#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Outback SPC III → Victron Venus OS Bridge (Ein-Datei-Implementierung, v3++ Stabilisierung)
=========================================================================================

Kernprinzipien (unverändert):
- PV-Anteil auf AC/L1 wird **nur** über com.victronenergy.pvinverter.* publiziert.
- Multi (com.victronenergy.vebus.*) publiziert auf **L1 nur den Batterie-Rest** (L1 − PV(L1)),
  plus optionalen Nacht-Eigenverbrauch.
- Battery-Monitor (com.victronenergy.battery.*) hat **Vorrang** für DC (V/I/P/SOC).
- ET112 / ACLoad (com.victronenergy.acload.*) liefern L2/L3.
- **Keine** künstliche Solar-Charger-Doppelzählung (AC-PV NICHT als DC-PV nochmal melden).
- Ein Gerät = eine Rolle (PV→pvinverter, Batterie→vebus, L2/L3→acload).

Erweiterungen in dieser Version:
- Stabile Visualisierung: PV-Forward-Zähler (/Ac/L1/Energy/Forward, /Ac/Energy/Forward), täglicher Reset
  (Mitternacht, GX-Zeit) und Persistenz unter /data/outback_spc/state.json.
- PV-Service flapped nicht (Connected=1 auch nachts; Power=0).
- UI-Leistungsgrenzen (PowerLimit) pro Phase: L1/L2=3000W, L3=1500W (konfigurierbar).
- Optionaler Generator-Passthrough-Service (com.victronenergy.grid.generator_tuya) mit Hysterese & Mindestlaufzeit.
- Testmodus mit Heartbeat (1s): Automatik (konsistent) und Override (direkt V/I/P/SOC setzen).
- CLI & (optionale) Settings-Integration; strukturierte, deduplizierte Logs (Text/JSON), Summenzeile.

Autor: Bridge-Implementierung für Outback / PowLand / Voltronic via BLE
Version: 3.1
"""

from __future__ import annotations

# ───────────────── D-Bus Mainloop früh setzen ─────────────────
from dbus.mainloop.glib import DBusGMainLoop
DBusGMainLoop(set_as_default=True)

# ───────────────── Imports ────────────────────────────────────
import argparse, atexit, json, logging, math, os, platform, random, signal, struct, sys, time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import dbus
from dbus.bus import BusConnection
import gi
gi.require_version("GLib", "2.0")
from gi.repository import GLib

# Victron velib-python (auf Venus OS vorhanden)
sys.path.insert(1, "/opt/victronenergy/dbus-systemcalc-py/ext/velib_python")
from vedbus import VeDbusService, VeDbusItemImport  # type: ignore

# SettingsDevice (optional – wenn verfügbar nutzen, ansonsten weicher Fallback)
try:
    from settingsdevice import SettingsDevice  # type: ignore
except Exception:
    SettingsDevice = None  # Fallback weiter unten

# BLE (nur live nötig)
try:
    from bluepy.btle import Peripheral, BTLEException  # type: ignore
except Exception:
    Peripheral = None
    BTLEException = Exception

# ───────────────── Version/Globale Defaults ──────────────────
VERSION = "3.1"
APPNAME = "outback_venus_v3"
STATE_DIR = "/data/outback_spc"
STATE_FILE = os.path.join(STATE_DIR, "state.json")

# ───────────────── Hilfen: DBus Verbindungen ──────────────────
class SystemBus(dbus.bus.BusConnection):
    def __new__(cls): return super().__new__(cls, dbus.bus.BusConnection.TYPE_SYSTEM)
class SessionBus(dbus.bus.BusConnection):
    def __new__(cls): return super().__new__(cls, dbus.bus.BusConnection.TYPE_SESSION)

def dbusconnection():
    return SessionBus() if 'DBUS_SESSION_BUS_ADDRESS' in os.environ else SystemBus()

def new_system_bus_connection() -> BusConnection:
    last_err = None
    for addr in ("unix:path=/run/dbus/system_bus_socket",
                 "unix:path=/var/run/dbus/system_bus_socket"):
        try:
            return BusConnection(addr)
        except Exception as e:
            last_err = e
    logging.getLogger("Core").warning("private system bus failed (%s), fallback to default", last_err)
    return dbusconnection()

# ───────────────── Utilities ──────────────────────────────────
def clamp(v: float, lo: float, hi: float) -> float:
    return lo if v < lo else hi if v > hi else v

def str2bool(v):
    if isinstance(v, bool): return v
    if v is None: return False
    return str(v).strip().lower() in ("1","true","t","yes","y","on")

def read_btaddr(cli_val: Optional[str]) -> str:
    if cli_val: return cli_val
    try:
        import utils  # optional
        return getattr(utils, "OUTBACK_ADDRESS", "00:35:FF:02:95:99")
    except Exception:
        return "00:35:FF:02:95:99"

def now_local_date_str() -> str:
    return time.strftime("%Y-%m-%d", time.localtime())

T0 = time.monotonic()

# ───────────────── Logging (Module/Levels & Dedupe) ──────────
class RateLimitedHandler(logging.Handler):
    """Einfache Dedupe/Rate-Limitierung pro (logger, level, msg) innerhalb eines Zeitfensters."""
    def __init__(self, base: logging.Handler, rate_limit_ms: int = 400):
        super().__init__(base.level)
        self.base = base
        self.rate_limit = rate_limit_ms / 1000.0
        self._last: Dict[Tuple[int,int,str], float] = {}

    def emit(self, record: logging.LogRecord) -> None:
        key = (hash(record.name), record.levelno, record.getMessage())
        t = time.monotonic()
        last = self._last.get(key, 0.0)
        if (t - last) >= self.rate_limit:
            self._last[key] = t
            self.base.emit(record)

def setup_logging(fmt: str = "text", debug: bool = False, rate_limit_ms: int = 400):
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.DEBUG if debug else logging.INFO)

    class TextFormatter(logging.Formatter):
        def format(self, r: logging.LogRecord) -> str:
            ts = time.monotonic() - T0
            module = (r.name[:5]).ljust(5)
            level = r.levelname.ljust(5)
            return f"[T+{ts:0.3f}s] {module} {level} {r.getMessage()}"

    class JsonFormatter(logging.Formatter):
        def format(self, r: logging.LogRecord) -> str:
            ts = time.monotonic() - T0
            obj = {
                "t_plus_s": round(ts, 3),
                "module": r.name,
                "level": r.levelname,
                "msg": r.getMessage(),
            }
            return json.dumps(obj, separators=(",",":"))

    stream = logging.StreamHandler(sys.stdout)
    stream.setLevel(logging.DEBUG if debug else logging.INFO)
    stream.setFormatter(TextFormatter() if fmt=="text" else JsonFormatter())
    rl = RateLimitedHandler(stream, rate_limit_ms=rate_limit_ms)
    root.addHandler(rl)

    # Teil-Logger
    for name in ("Core","Outbk","Battery","PV","Gen","ET112","Test"):
        logging.getLogger(name).setLevel(logging.DEBUG if debug else logging.INFO)

# ───────────────── Persistenz: PV-Forward-Zähler ──────────────
class PvForwardStore:
    """PV-Forward Energie (kWh) – Tageszähler (Reset um Mitternacht), Lifetime persistent."""
    def __init__(self, path: str = STATE_FILE):
        self.path = path
        self.total_kwh = 0.0
        self.day_kwh = 0.0
        self.day_date = now_local_date_str()
        self._dirty = False
        self._last_save = 0.0
        self._load()

    def _ensure_dir(self):
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
        except Exception:
            pass

    def _load(self):
        try:
            with open(self.path, "r") as f:
                d = json.load(f)
            self.total_kwh = float(d.get("pv_forward_total_kwh", 0.0))
            self.day_kwh = float(d.get("pv_forward_day_kwh", 0.0))
            self.day_date = str(d.get("pv_forward_day_date", self.day_date))
        except Exception:
            pass

    def save_if_needed(self, force: bool=False):
        now = time.monotonic()
        if not force and not self._dirty:
            return
        if not force and (now - self._last_save) < 15.0:
            return
        self._ensure_dir()
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump({
                    "pv_forward_total_kwh": round(self.total_kwh, 6),
                    "pv_forward_day_kwh": round(self.day_kwh, 6),
                    "pv_forward_day_date": self.day_date
                }, f)
            os.replace(tmp, self.path)
            self._dirty = False
            self._last_save = now
        except Exception as e:
            logging.getLogger("Core").warning("persist save failed: %s", e)

    def integrate(self, p_w: float, dt_s: float):
        # Tageswechsel prüfen
        cur_date = now_local_date_str()
        if cur_date != self.day_date:
            self.day_date = cur_date
            self.day_kwh = 0.0
            self._dirty = True
        inc_kwh = (p_w * (dt_s/3600.0)) / 1000.0
        if inc_kwh > 0:
            self.total_kwh += inc_kwh
            self.day_kwh += inc_kwh
            self._dirty = True

    def snapshot(self) -> Tuple[float,float,str]:
        return (self.total_kwh, self.day_kwh, self.day_date)

PV_STORE = PvForwardStore()
atexit.register(lambda: PV_STORE.save_if_needed(force=True))

# ───────────────── D-Bus Dummy-Service (Dry-Run) ──────────────
class DummyVeDbusService:
    """Emuliert das API von VeDbusService für --dry-run/CLI-Tests ohne D-Bus."""
    def __init__(self, name: str, *_args, **_kwargs):
        self.name = name
        self.paths: Dict[str, dict] = {}
        self._registered = False
        logging.getLogger("Core").info("DummyService %s", name)

    def add_path(self, path, value=None, writeable=False, description=None, gettextcallback=None):
        self.paths[path] = {"value": value, "writeable": writeable, "cb": gettextcallback}
        return path

    def __setitem__(self, path, value):
        if path not in self.paths:
            self.add_path(path, value=value)
        else:
            self.paths[path]["value"] = value

    def __getitem__(self, path):
        if path not in self.paths:
            raise KeyError(path)
        return self.paths[path]["value"]

    def register(self):
        self._registered = True
        logging.getLogger("Core").info("DummyService registered: %s (%d paths)", self.name, len(self.paths))

# ───────────────── Test-Szenarien (konsistent) ────────────────
# Eingaben (intuitiv): pv_tot, L1, L2, L3, dcV, mode
# mode: "charge" (pv_rest→batt), "discharge" (0→batt), "balanced" (batt≈0)
SCENARIOS: Dict[str, Dict[str, float | str]] = {
    "day_charge":      {"pv":1800, "L1":500, "L2":200, "L3":100, "dcV":26.8, "mode":"charge"},
    "day_cover_l1":    {"pv":700,  "L1":600, "L2":400, "L3":150, "dcV":26.7, "mode":"discharge"},
    "balanced":        {"pv":1000, "L1":600, "L2":300, "L3":100, "dcV":26.8, "mode":"balanced"},
    "evening_l23":     {"pv":0,    "L1":80,  "L2":900, "L3":700, "dcV":26.5, "mode":"discharge"},
    "night_idle":      {"pv":0,    "L1":10,  "L2":5,   "L3":5,   "dcV":26.5, "mode":"discharge"},
    "cloud_bursts":    {"pv":1200, "L1":800, "L2":300, "L3":100, "dcV":26.8, "mode":"balanced"},
    "day_plus_batt":   {"pv":1500, "L1":300, "L2":200, "L3":100, "dcV":27.0, "mode":"charge"},
    "day_surplus":     {"pv":2200, "L1":400, "L2":100, "L3":50,  "dcV":27.2, "mode":"charge"},
    "gen":             {"pv":0,    "L1":50,  "L2":0,   "L3":0,   "dcV":26.6, "mode":"discharge"},
}

# ───────────────── OutbackReader (Round Snapshot) ─────────────
class OutbackReader:
    """
    Liest Outback-SPC-III Messwerte via BLE oder liefert konsistente Testdaten.
    Round-Modell: A03 + A11 werden pro Runde erfasst (min. Intervall, Backoff).
    """

    # GATT UUIDs (als Platzhalter – wie v3)
    _SRV_1810 = '00001810-0000-1000-8000-00805f9b34fb'  # enthält A03
    _SRV_1811 = '00001811-0000-1000-8000-00805f9b34fb'  # enthält A11
    _A03      = '00002a03-0000-1000-8000-00805f9b34fb'
    _A11      = '00002a11-0000-1000-8000-00805f9b34fb'

    BASE_MIN_INTERVAL    = 1.8
    BACKOFF_MAX_DEFAULT  = 15.0
    BACKOFF_MAX          = BACKOFF_MAX_DEFAULT

    # Eigenverbrauch des Outback (AC-Seite) – nur nachts relevant
    SELF_CONS_W          = 35.0

    def __init__(self, hci: str, mac: str, *,
                 test: bool=False, scene: str="day_charge", debug: bool=False,
                 min_interval_s: float=1.8, backoff_max_s: float=15.0, seed: Optional[int]=None):
        self.log = logging.getLogger("Outbk")
        self.hci, self.mac = hci, mac
        self.test, self.scene, self.debug = bool(test), scene, bool(debug)

        self.min_interval_s = float(min_interval_s or self.BASE_MIN_INTERVAL)
        self.backoff_max    = float(backoff_max_s  or self.BACKOFF_MAX_DEFAULT)

        # BLE Handles
        self._p = None; self._c03 = None; self._c11 = None

        # Round/Timing
        self._busy = False
        self._next_round_at = 0.0
        self._last_snapshot_at = 0.0
        self._consec_fails = 0
        self._round_id = 0
        self._last_throttle_log = 0.0

        # Metriken
        self._ok_count = 0; self._fail_count = 0
        self._acc_read_ms = 0.0; self._acc_skew_ms = 0.0
        self._last_metrics_ts = 0.0

        # Letzte Skalierungswerte (SI)
        self.pvP = 0.0; self.pvV = 0.0; self.pvI = 0.0
        self.acV = 230.0; self.acF = 50.0
        self.acP_active = 0.0; self.acS_apparent = 0.0; self.loadPct = 0.0
        self.dcV = 26.6; self.dcI = 0.0

        if seed is not None:
            random.seed(seed)

        if self.test:
            self._schedule_next(success=True)
        else:
            try:
                self._connect()
                self._schedule_next(success=True)
            except Exception as e:
                if self.debug: self.log.debug("First connect failed: %s", e)
                self._schedule_next(success=False)

    # ─── BLE Connect/Disconnect ───
    def _connect(self):
        if Peripheral is None:
            raise RuntimeError("bluepy nicht verfügbar")
        iface = int(self.hci[3:]) if self.hci.startswith("hci") else 0
        self._p = Peripheral(self.mac, iface=iface)
        s10 = self._p.getServiceByUUID(self._SRV_1810)
        s11 = self._p.getServiceByUUID(self._SRV_1811)
        self._c03 = s10.getCharacteristics(self._A03)[0]
        self._c11 = s11.getCharacteristics(self._A11)[0]
        self._consec_fails = 0
        if self.debug: self.log.debug("BLE connected %s on %s", self.mac, self.hci)

    def _disconnect(self):
        try:
            if self._p: self._p.disconnect()
        except Exception:
            pass
        self._p = self._c03 = self._c11 = None
        if self.debug: self.log.debug("BLE disconnected")

    # ─── Utils ───
    @staticmethod
    def _swap_decode(buf: bytes) -> tuple[int, ...]:
        shorts = struct.unpack('>' + 'h' * (len(buf)//2), buf)
        return tuple(((v >> 8) & 255) | ((v & 255) << 8) for v in shorts)

    def _schedule_next(self, *, success: bool):
        now = time.time()
        if success:
            delay = self.min_interval_s
            self._consec_fails = 0
        else:
            ladder = [1.0, 2.0, 4.0, 8.0, 12.0]
            idx = min(max(self._consec_fails-1, 0), len(ladder)-1)
            delay = min(ladder[idx], self.backoff_max)
        delay += random.uniform(0.0, 0.2)
        self._next_round_at = now + delay

    def _report_metrics(self):
        now = time.time()
        if self._last_metrics_ts and (now - self._last_metrics_ts) < 30.0:
            return
        self._last_metrics_ts = now
        avg_read = self._acc_read_ms/self._ok_count if self._ok_count else 0.0
        avg_skew = self._acc_skew_ms/self._ok_count if self._ok_count else 0.0
        interval = max(0.0, self._next_round_at - now)
        self.log.info("BLE stats: ok=%d fail=%d avg_read=%.1fms avg_skew=%.1fms interval=%.2fs",
                 self._ok_count, self._fail_count, avg_read, avg_skew, interval)

    # ─── Testdaten: konsistente Bilanz ───
    def _gen_consistent(self):
        base = SCENARIOS.get(self.scene, SCENARIOS["day_charge"])
        pv = float(base["pv"]); L1=float(base["L1"]); L2=float(base["L2"]); L3=float(base["L3"])
        dcV = float(base["dcV"]); mode=str(base["mode"])

        # leichte Variation
        jitter = lambda v,p=0.05: v*(1+p*math.sin(time.time()/7+random.random()))
        pv = max(0.0, jitter(pv)); L1 = max(0.0, jitter(L1,0.02))
        L2 = max(0.0, jitter(L2,0.02)); L3 = max(0.0, jitter(L3,0.02))

        # PV→L1
        pv_to_L1 = min(L1, pv)
        batt_to_L1 = max(0.0, L1 - pv_to_L1)
        batt_to_L23 = L2 + L3
        batt_to_AC = batt_to_L1 + batt_to_L23
        pv_rest = max(0.0, pv - pv_to_L1)

        if mode == "charge":
            pv_to_batt = pv_rest
        elif mode == "discharge":
            pv_to_batt = 0.0
        else:  # balanced
            pv_to_batt = min(batt_to_AC, pv_rest)

        dcP = pv_to_batt - batt_to_AC                       # + = lädt, - = entlädt
        dcI = dcP / dcV if dcV else 0.0

        # Werte auf Reader-Felder mappen
        self.pvP = pv
        self.pvV = 120.0 if pv > 0 else 0.0
        self.pvI = (pv/self.pvV) if self.pvV else 0.0

        self.acV = 230.0; self.acF = 50.0
        self.acP_active = L1                                # A03[5] = Gesamt-L1 (PV + ggf. Batt)
        self.acS_apparent = max(L1, L1*1.05)
        self.loadPct = min(100.0, (L1/3000.0)*100.0)
        self.dcV = dcV; self.dcI = dcI

    # ─── Haupt-API ───
    def read(self) -> bool:
        now = time.time()
        if self.test:
            if now < self._next_round_at:
                self.last_status = "throttle"
                if self.debug and (now - self._last_throttle_log > 5.0):
                    self.log.debug("throttle until %.3f (in %.1fs)", self._next_round_at, self._next_round_at - now)
                    self._last_throttle_log = now
                self._report_metrics()
                return True
            self._gen_consistent()
            self._last_snapshot_at = now
            self._ok_count += 1
            self._acc_read_ms += 120.0; self._acc_skew_ms += 10.0
            self._schedule_next(success=True)
            self.last_status = "ok-test"
            self._report_metrics()
            return True

        # Live
        if self._busy:
            self.last_status = "busy"; return True
        if now < self._next_round_at:
            self.last_status = "throttle"
            if self.debug and (now - self._last_throttle_log > 5.0):
                self.log.debug("throttle until %.3f (in %.1fs)", self._next_round_at, self._next_round_at - now)
                self._last_throttle_log = now
            self._report_metrics()
            return True

        self._busy = True; self._round_id += 1; rid = self._round_id
        try:
            if not self._p: self._connect()
            t0 = time.monotonic(); raw_a03 = self._c03.read(); t_mid = time.monotonic(); raw_a11 = self._c11.read(); t1 = time.monotonic()
            a03 = self._swap_decode(raw_a03); a11 = self._swap_decode(raw_a11)

            self.acV = a03[2]*0.1; self.acF = a03[3]*0.1
            self.acS_apparent = float(a03[4]); self.acP_active = float(a03[5]); self.loadPct = float(a03[6])
            self.dcV = a03[8]*0.01; self.dcI = float(a03[9])
            self.pvV = a11[6]*0.1; self.pvP = float(a11[7]); self.pvI = (self.pvP/self.pvV) if self.pvV else 0.0

            self._ok_count += 1
            self._acc_read_ms += (t1-t0)*1000.0; self._acc_skew_ms += (t1-t_mid)*1000.0
            self._last_snapshot_at = time.time()
            self._schedule_next(success=True)
            self.last_status = "ok"
            if self.debug:
                self.log.debug("ROUND %d OK | acV=%.1f P_L1=%.0f pv=%.0f dcV=%.2f dcI=%.2f", rid, self.acV, self.acP_active, self.pvP, self.dcV, self.dcI)
            self._report_metrics()
            return True

        except BTLEException as e:
            hard = any(s in str(e) for s in ("Helper not started","Not connected","Device disconnected"))
            if self.debug: self.log.debug("BLE round %d failed: %s (hard=%s)", rid, e, hard)
            self._fail_count += 1; self._consec_fails += 1
            if hard:
                self._disconnect(); time.sleep(0.2)
                try: self._connect()
                except Exception: pass
                self._next_round_at = time.time() + 1.5
            else:
                if self._consec_fails >= 2:
                    self._disconnect()
                    try: self._connect()
                    except Exception: pass
                self._schedule_next(success=False)
            self.last_status = "exc"; return False
        except Exception as e:
            if self.debug: self.log.debug("BLE round %d unexpected: %s", rid, e)
            self._fail_count += 1; self._consec_fails += 1
            self._schedule_next(success=False)
            self.last_status = "exc"; return False
        finally:
            self._busy = False

# ───────────────── Settings-Facade (sanfter Fallback) ─────────
class DeviceSettings:
    """
    Minimaler Wrapper: liest/aktualisiert gewünschte Settings-Pfade.
    - Wenn SettingsDevice vorhanden: nutzt diese (GX-UI).
    - Sonst: In-Memory mit optionalem JSON-Backup unter /data/outback_spc/settings.json.
    Hinweis: Ohne SettingsDevice sind Settings nicht in der GX-UI sichtbar.
    """
    FILE = os.path.join(STATE_DIR, "settings.json")

    def __init__(self, bus):
        self.bus = bus
        self._mem: Dict[str, float | int | str | bool] = {}
        self._load_file()
        self._sd = None
        if SettingsDevice is not None:
            try:
                # settingsdevice-API variiert leicht; wir kapseln add über self.add()
                self._sd = SettingsDevice(bus, "com.victronenergy.settings")  # type: ignore
            except Exception:
                self._sd = None

    def _load_file(self):
        try:
            with open(self.FILE, "r") as f:
                self._mem.update(json.load(f))
        except Exception:
            pass

    def _save_file(self):
        try:
            os.makedirs(os.path.dirname(self.FILE), exist_ok=True)
            tmp = self.FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(self._mem, f)
            os.replace(tmp, self.FILE)
        except Exception:
            pass

    def add(self, path: str, default, typ: str="i"):
        # typ nur informativ; im Fallback egal
        if path not in self._mem:
            self._mem[path] = default
            self._save_file()
        # Bei vorhandenem SettingsDevice: robust versuchen zu registrieren
        if self._sd is not None:
            try:
                # Viele Implementierungen haben addSetting(path, default, min, max, type)
                # Wir verwenden konservativ: addSetting(path, default)
                add_fn = getattr(self._sd, "addSetting", None)
                if callable(add_fn):
                    try:
                        add_fn(path, default)  # type: ignore
                    except TypeError:
                        # alternativer Signaturversuch
                        try:
                            add_fn(path=path, default=default)  # type: ignore
                        except Exception:
                            pass
            except Exception:
                pass

    def get(self, path: str, default=None):
        if path in self._mem:
            return self._mem[path]
        # Falls SettingsService existiert und Pfad vorhanden ist, lesen:
        try:
            item = VeDbusItemImport(self.bus, "com.victronenergy.settings", f"/{path}")
            val = item.get_value()
            self._mem[path] = val
            self._save_file()
            return val
        except Exception:
            if default is not None:
                self._mem[path] = default
                self._save_file()
                return default
            return None

    def set(self, path: str, value):
        self._mem[path] = value
        self._save_file()
        # Versuchen, über settingsdevice zu schreiben
        if self._sd is not None:
            try:
                set_fn = getattr(self._sd, "set", None)
                if callable(set_fn):
                    set_fn(path, value)  # type: ignore
            except Exception:
                pass

# ───────────────── Generator-Service (optional) ───────────────
class GeneratorService:
    """Grid/Generator-artiger Service, aktiviert bei Passthrough + TuyaPower > Schwelle."""
    def __init__(self, bus, name: str, device_instance: int, power_limit_w: int = 3000, dry_run: bool = False):
        self.log = logging.getLogger("Gen")
        svc_cls = DummyVeDbusService if dry_run else VeDbusService
        self.svc = svc_cls(name, bus, register=False)
        self.add = self.svc.add_path
        self.add("/Mgmt/ProcessName", __file__)
        self.add("/Mgmt/ProcessVersion", "Python " + platform.python_version())
        self.add("/ProductId", 0)
        self.add("/ProductName", "Generator (Tuya)")
        self.add("/DeviceInstance", device_instance)
        self.add("/Connected", 0)
        self.add("/Ac/NumberOfPhases", 1)
        self.add("/Ac/L1/Voltage", 0.0)
        self.add("/Ac/L1/Current", 0.0)
        self.add("/Ac/L1/Power",   0.0)
        self.add("/Ac/L1/PowerLimit", float(power_limit_w))
        self.add("/Status/Running", 0)
        self.add("/UpdateIndex", 0, writeable=True)
        self.svc.register()
        self.running = False
        self._last_change = 0.0

        # Hysterese/Mindestlaufzeit (konfigurierbar extern)
        self.start_w = 120.0
        self.stop_w  = 60.0
        self.min_run_s = 8.0

    def configure(self, start_w: float, stop_w: float, min_run_s: float):
        self.start_w = float(start_w)
        self.stop_w  = float(stop_w)
        self.min_run_s = float(min_run_s)

    def set_connected(self, on: bool):
        self.svc["/Connected"] = 1 if on else 0

    def update(self, passthrough: bool, tuya_power_w: float, voltage: float = 230.0):
        now = time.monotonic()
        prev = self.running
        if not passthrough or tuya_power_w is None:
            self.running = False
        else:
            if not self.running:
                if tuya_power_w >= self.start_w:
                    self.running = True
                    self._last_change = now
            else:
                # Mindestlaufzeit beachten
                if (now - self._last_change) >= self.min_run_s and tuya_power_w <= self.stop_w:
                    self.running = False
                    self._last_change = now

        self.svc["/Status/Running"] = 1 if self.running else 0
        self.svc["/Ac/L1/Power"] = float(tuya_power_w or 0.0) if self.running else 0.0
        self.svc["/Ac/L1/Voltage"] = float(voltage if self.running else 0.0)
        cur = (self.svc["/Ac/L1/Power"] / self.svc["/Ac/L1/Voltage"]) if (self.running and self.svc["/Ac/L1/Voltage"]>0) else 0.0
        self.svc["/Ac/L1/Current"] = cur
        self.svc["/UpdateIndex"] = (self.svc["/UpdateIndex"] + 1) % 256

        if prev != self.running:
            self.log.info("GEN %s | power=%.0fW start>=%.0f stop<=%.0f minRun=%.0fs",
                          "START" if self.running else "STOP", tuya_power_w or 0.0, self.start_w, self.stop_w, self.min_run_s)

# ───────────────── Bridge (Services & Logik) ──────────────────
@dataclass
class BridgeConfig:
    di_vebus: int = 40
    di_pvinv: int = 61
    di_grid: int  = 38
    l1_limit: float = 3000.0
    l2_limit: float = 3000.0
    l3_limit: float = 1500.0
    poll_ms: int = 1000
    summary_period_s: float = 5.0
    dry_run: bool = False
    rate_limit_ms: int = 400
    log_format: str = "text"
    debug: bool = False
    once: bool = False
    balance_check: bool = False
    tuya_enabled: bool = False
    tuya_source: str = "settings"  # settings|cli|off
    tuya_cli_power_w: float = 0.0

class Bridge:
    """
    Services:
      - VE.Bus/Multi:  com.victronenergy.vebus.<hci>  (L1 = *nur* Batterie-Rest, L2/L3 von ET112)
      - PV-Inverter:   com.victronenergy.pvinverter.<hci> (liefert PV→L1; Connected=1)
      - Generator:     com.victronenergy.grid.generator_tuya (optional)

    BMS priorisiert DC. Energiepfade werden integriert. Anti-Doppelzählung gemäß Formel:
      P_pv_ac = clamp( P_L1_out - max(0, -P_batt), 0, P_L1_out )
    """
    def __init__(self, reader: OutbackReader, cfg: BridgeConfig, test_scene: Optional[str], settings: DeviceSettings):
        self.r = reader
        self.cfg = cfg
        self.scene = test_scene or "day_charge"
        self.poll_ms = int(cfg.poll_ms)
        self.bus_main = dbusconnection()
        self.bus_pv   = new_system_bus_connection()
        self.log_core = logging.getLogger("Core")
        self.log_pv   = logging.getLogger("PV")
        self.log_bat  = logging.getLogger("Battery")
        self.log_gen  = logging.getLogger("Gen")
        self.log_et   = logging.getLogger("ET112")
        self.log_test = logging.getLogger("Test")

        self.settings = settings
        self._init_settings_defaults()

        # Externe Quellen erkennen (BMS, ET112)
        self._detect_external()

        # Services anlegen
        svc_cls = DummyVeDbusService if cfg.dry_run else VeDbusService
        self.vebus = self._svc_vebus(reader.hci, svc_cls)
        self.pvinv = self._svc_pvinverter(reader.hci, svc_cls)
        self.gen   = GeneratorService(self.bus_main, "com.victronenergy.grid.generator_tuya",
                                      device_instance=cfg.di_grid, power_limit_w=int(cfg.l1_limit), dry_run=cfg.dry_run)
        # Generator zunächst getrennt; Aktivierung via update()
        self.gen.set_connected(cfg.tuya_enabled)

        # Energiepfade (kWh)
        self._e = {"s2b":0.0, "s2i":0.0, "i2a":0.0, "b2i":0.0}
        self._t_last = time.time()
        self._last_summary = 0.0

        # Sanfte L1-Änderungen (Multi-L1)
        self._l1_prev = 0.0; self._l1_prev_ema = 0.0

        # Watchdog
        self._last_reader_ok = time.time()

        # CLI/Test: Dump-Signal
        signal.signal(signal.SIGUSR1, lambda *_: self._dump_now())

        if not cfg.once:
            GLib.timeout_add(self.poll_ms, self._update)

    # ─── Settings-Defaults ───
    def _init_settings_defaults(self):
        # Allgemein
        self.settings.add("Settings/Devices/OutbackSPC/TestMode", "off", "s")   # off|auto|override|night|day|...
        self.settings.add("Settings/Devices/OutbackSPC/SummaryPeriod", float(self.cfg.summary_period_s), "f")
        self.settings.add("Settings/Devices/OutbackSPC/RateLimitMs", int(self.cfg.rate_limit_ms), "i")

        # Tuya/Generator
        self.settings.add("Settings/Devices/OutbackSPC/Tuya/Enable", int(1 if self.cfg.tuya_enabled else 0), "i")
        self.settings.add("Settings/Devices/OutbackSPC/Tuya/StartW", 120, "i")
        self.settings.add("Settings/Devices/OutbackSPC/Tuya/StopW",  60,  "i")
        self.settings.add("Settings/Devices/OutbackSPC/Tuya/MinRunS", 8,   "i")
        self.settings.add("Settings/Devices/OutbackSPC/Tuya/PowerW",  0,   "i")  # kann extern gesetzt werden

    # ─── externe Quellen suchen ───
    def _detect_external(self):
        names = self.bus_main.list_names()

        # Battery-Monitor (erster Treffer)
        self.bms = None
        for n in names:
            if n.startswith("com.victronenergy.battery."):
                self.bms = {
                    "V":   VeDbusItemImport(self.bus_main, n, "/Dc/0/Voltage"),
                    "I":   VeDbusItemImport(self.bus_main, n, "/Dc/0/Current"),
                    "P":   VeDbusItemImport(self.bus_main, n, "/Dc/0/Power"),
                    "SOC": VeDbusItemImport(self.bus_main, n, "/Soc"),
                }
                self.log_core.info("Battery-Monitor: %s", n)
                break

        # ET112 → L2/L3
        self.ac = [None, None]
        ac_names = sorted(n for n in names if n.startswith("com.victronenergy.acload."))
        for i, n in enumerate(ac_names[:2]):
            self.ac[i] = {
                "P": VeDbusItemImport(self.bus_main, n, "/Ac/L1/Power"),
                "V": VeDbusItemImport(self.bus_main, n, "/Ac/L1/Voltage"),
                "I": VeDbusItemImport(self.bus_main, n, "/Ac/L1/Current"),
            }
            self.log_core.info("ACLoad L%d: %s", i+2, n)

    # ─── VE.Bus Service ───
    def _svc_vebus(self, hci: str, svc_cls) -> VeDbusService:
        s = svc_cls(f"com.victronenergy.vebus.{hci}", self.bus_main, register=False)
        add = s.add_path
        add("/Mgmt/ProcessName", __file__)
        add("/Mgmt/ProcessVersion", "Python " + platform.python_version())
        add("/Mgmt/Connection", "TEST" if self.r.test else f"Bluetooth {hci}")
        add("/DeviceInstance", self.cfg.di_vebus)
        add("/ProductId", 0xFFFF)
        add("/ProductName", "Outback SPC III (Bridge)")
        add("/FirmwareVersion", VERSION)
        add("/Connected", 1)

        fmt = lambda u:(lambda _p,v:f"{v:.1f}{u}")
        _w,_v,_a,_h,_p = fmt(" W"),fmt(" V"),fmt(" A"),fmt(" Hz"),fmt("%")

        for ph, limit in (("L1", self.cfg.l1_limit), ("L2", self.cfg.l2_limit), ("L3", self.cfg.l3_limit)):
            for m,cb in (("P",_w),("V",_v),("I",_a),("F",_h)):
                add(f"/Ac/Out/{ph}/{m}", 0.0, gettextcallback=cb)
            add(f"/Ac/Out/{ph}/PowerLimit", float(limit))

        for p,cb in (("/Dc/0/Voltage",_v),("/Dc/0/Current",_a),("/Dc/0/Power",_w)):
            add(p, 0.0, gettextcallback=cb)
        add("/Soc", 0.0, gettextcallback=_p)

        # Energiepfade (kWh)
        for ep in ("/Energy/SolarToBattery","/Energy/SolarToInverter",
                   "/Energy/InverterToAcOut","/Energy/BatteryToInverter"):
            add(ep, 0.0)

        add("/Ac/NumberOfPhases", 3)
        add("/Mode", 3, writeable=True)
        add("/State", 0, writeable=True)
        add("/UpdateIndex", 0, writeable=True)
        s.register()
        self.log_core.info("registered VE.Bus as com.victronenergy.vebus.%s", hci)
        return s

    # ─── PV-Inverter Service (nur L1) ───
    def _svc_pvinverter(self, hci: str, svc_cls) -> VeDbusService:
        s = svc_cls(f"com.victronenergy.pvinverter.{hci}", self.bus_pv, register=False)
        add = s.add_path
        add("/Mgmt/ProcessName", __file__)
        add("/Mgmt/ProcessVersion", "Python " + platform.python_version())
        add("/Mgmt/Connection", "Virtual PV @ AC-Out (L1)")
        add("/DeviceInstance", self.cfg.di_pvinv)
        add("/ProductId", 0)
        add("/ProductName", "Outback PV (AC L1 share)")
        add("/FirmwareVersion", VERSION)
        add("/Connected", 1)

        _w = (lambda _p,v:f"{v:.1f} W")
        add("/Ac/Power", 0.0, gettextcallback=_w)
        for ph in ("L1","L2","L3"):
            add(f"/Ac/{ph}/Power", 0.0, gettextcallback=_w)
        # Forward-Zähler (kWh)
        add("/Ac/L1/Energy/Forward", 0.0)
        add("/Ac/Energy/Forward",    0.0)  # optionaler Gesamtzähler (hier identisch L1)
        # UI-Skala
        add("/Ac/L1/PowerLimit", float(self.cfg.l1_limit))
        add("/UpdateIndex", 0, writeable=True)
        add("/Position", 1)  # 1 = AC-Out
        s.register()
        self.log_core.info("registered PV-Inverter as com.victronenergy.pvinverter.%s", hci)
        return s

    # ─── Energie-Integrator ───
    @staticmethod
    def _kwh(acc: float, p_w: float, dt_s: float) -> float:
        return acc + (p_w * (dt_s/3600.0)) / 1000.0

    # ─── Hilfen ───
    def _acload(self, idx: int, key: str) -> float:
        try:
            if self.ac[idx]:
                return float(self.ac[idx][key].get_value())
        except Exception:
            pass
        return 0.0

    def _read_bms(self):
        """Bevorzugt BMS; liefert (dcV, dcI, dcP, soc, source)"""
        used_dc = "Outback"
        soc = None
        if getattr(self, "bms", None):
            try:
                dcV = float(self.bms["V"].get_value())
                dcI = float(self.bms["I"].get_value())
                dcP = float(self.bms["P"].get_value())
                soc = float(self.bms["SOC"].get_value())
                used_dc = "BMS"
                return dcV, dcI, dcP, soc, used_dc
            except Exception:
                pass
        # Fallback Outback (nur wenn BMS nicht greifbar)
        dcV = float(self.r.dcV); dcI = float(self.r.dcI); dcP = dcV*dcI
        return dcV, dcI, dcP, soc, used_dc

    def _pv_write(self, p_l1_w: float, dt_s: float):
        """Schreibt PV-Leistung & integriert Forward-Zähler."""
        self.pvinv["/Ac/L1/Power"] = p_l1_w
        self.pvinv["/Ac/Power"]    = p_l1_w
        self.pvinv["/Ac/L2/Power"] = 0.0
        self.pvinv["/Ac/L3/Power"] = 0.0
        PV_STORE.integrate(p_l1_w, dt_s)
        total, day, date = PV_STORE.snapshot()
        self.pvinv["/Ac/L1/Energy/Forward"] = total
        self.pvinv["/Ac/Energy/Forward"]    = total
        self.pvinv["/UpdateIndex"] = (self.pvinv["/UpdateIndex"] + 1) % 256
        self.log_pv.info("l1_pv=%dW → pvinverter:/Ac/L1/Power | fwd_total=%.3fkWh (day %.3f @ %s)",
                         int(round(p_l1_w)), total, day, date)

    def _dump_now(self):
        total, day, date = PV_STORE.snapshot()
        self.log_core.info("DUMP  PV_forward_total=%.3fkWh  PV_day=%.3fkWh (%s)  E: %s",
                           total, day, date, json.dumps(self._e, sort_keys=True))

    # ─── Haupt-Tick ───
    def tick(self) -> bool:
        now = time.time()
        dt  = max(0.0, now - self._t_last)
        self._t_last = now

        ok = self.r.read()
        if ok:
            self._last_reader_ok = now
        else:
            if (now - self._last_reader_ok) > 10.0:
                self.log_core.warning("Reader stalled > 10s (status=%s)", getattr(self.r,"last_status","n/a"))

        # DC vom BMS bevorzugen
        dcV, dcI, dcP, soc, used_dc = self._read_bms()

        # Testmodus ggf. überschreiben/erzwingen
        test_mode = str(self.settings.get("Settings/Devices/OutbackSPC/TestMode", "off"))
        if test_mode in ("override", "custom"):
            # Override-Werte aus Settings (falls gesetzt)
            dcV = float(self.settings.get("Settings/Devices/OutbackSPC/Test/DCV", dcV))
            dcI = float(self.settings.get("Settings/Devices/OutbackSPC/Test/DCI", dcI))
            dcP = float(self.settings.get("Settings/Devices/OutbackSPC/Test/DCP", dcV*dcI))
            soc = float(self.settings.get("Settings/Devices/OutbackSPC/Test/SOC", soc if soc is not None else 50.0))
        # Heartbeat sicherstellen: wir schreiben ohnehin jede Sekunde

        # L2/L3 (ET112)
        L2P, L2V, L2I = self._acload(0,"P"), self._acload(0,"V"), self._acload(0,"I")
        L3P, L3V, L3I = self._acload(1,"P"), self._acload(1,"V"), self._acload(1,"I")

        # Outback Snapshot
        L1_meas= max(0.0, float(self.r.acP_active or 0.0))
        acV    = max(0.0, float(self.r.acV)); acF = max(0.0, float(self.r.acF))

        # ── Anti-Doppelzählung: PV-AC-Anteil exakt nach Formel ───────────────
        # P_pv_ac = clamp( P_L1_out - max(0, -P_batt), 0, P_L1_out )
        pv_ac_l1 = clamp(L1_meas - max(0.0, -dcP), 0.0, L1_meas)

        # Nacht-Eigenverbrauch (nur wenn PV≈0 & Entladung) addiert sich zum Batterieanteil auf L1
        add_night = OutbackReader.SELF_CONS_W if (pv_ac_l1 < 5.0 and dcP < 0.0) else 0.0

        # Multi-L1 zeigt nur Batterie-Rest (sanft glätten)
        batt_to_L1_target = max(0.0, L1_meas - pv_ac_l1 + add_night)
        RATE_W_PER_S = 700.0     # max Ramp/sek
        EMA_ALPHA    = 0.35      # EMA-Glättung
        max_step     = RATE_W_PER_S * dt
        l1_ramped    = self._l1_prev + clamp(batt_to_L1_target - self._l1_prev, -max_step, max_step)
        l1_smoothed  = EMA_ALPHA*l1_ramped + (1.0-EMA_ALPHA)*self._l1_prev_ema
        self._l1_prev, self._l1_prev_ema = l1_ramped, l1_smoothed
        L1_multi = max(0.0, l1_smoothed)

        # ── Energiepfade integrieren (kWh) ───────────────────────
        e0 = self._e.copy()
        # Solar→Inverter = PV-AC-Anteil auf L1
        self._e["s2i"] = self._kwh(self._e["s2i"], pv_ac_l1, dt)
        # Solar→Battery ~ pos. DC-Leistung (vereinfachte Zuordnung)
        if dcP > 0.0:
            self._e["s2b"] = self._kwh(self._e["s2b"], dcP, dt)
        # Inverter→AC-Out (nur Multi-L1-Anteil)
        self._e["i2a"] = self._kwh(self._e["i2a"], L1_multi, dt)
        # Battery→Inverter (Entladung)
        if dcP < 0.0:
            self._e["b2i"] = self._kwh(self._e["b2i"], abs(dcP), dt)

        # ── Schreiben: PV-Inverter (L1) + Forward-Zähler ─────────
        self._pv_write(pv_ac_l1, dt)

        # ── Schreiben: Multi / VE.Bus ────────────────────────────
        m = self.vebus
        # DC
        m["/Dc/0/Voltage"], m["/Dc/0/Current"], m["/Dc/0/Power"] = dcV, dcI, dcP
        if soc is not None:
            m["/Soc"] = clamp(float(soc), 0.0, 100.0)

        # AC-Out
        m["/Ac/Out/L1/P"] = L1_multi
        m["/Ac/Out/L1/V"] = acV
        m["/Ac/Out/L1/I"] = (L1_multi/acV) if acV else 0.0
        m["/Ac/Out/L1/F"] = acF

        m["/Ac/Out/L2/P"], m["/Ac/Out/L2/V"], m["/Ac/Out/L2/I"], m["/Ac/Out/L2/F"] = L2P, L2V, L2I, acF
        m["/Ac/Out/L3/P"], m["/Ac/Out/L3/V"], m["/Ac/Out/L3/I"], m["/Ac/Out/L3/F"] = L3P, L3V, L3I, acF

        # Energiepfade (kWh)
        m["/Energy/SolarToInverter"]   = self._e["s2i"]
        m["/Energy/SolarToBattery"]    = self._e["s2b"]
        m["/Energy/InverterToAcOut"]   = self._e["i2a"]
        m["/Energy/BatteryToInverter"] = self._e["b2i"]

        # Mode/State (heuristisch, wie v3 – leicht justiert)
        total_ac = L1_multi + L2P + L3P + pv_ac_l1
        if pv_ac_l1 > 80 and dcP >= 30:
            mode, state = 3, 5     # Absorption
        elif pv_ac_l1 > 50 and dcP >= 0:
            mode, state = 3, 4     # Bulk/Charging
        elif total_ac > 50 and dcP < 0:
            mode, state = 3, 9     # Inverting
        else:
            mode, state = 3, 11    # Passthru/Stand-by
        m["/Mode"], m["/State"] = mode, state

        m["/UpdateIndex"] = (m["/UpdateIndex"] + 1) % 256

        # ── Generator-Logik ──────────────────────────────────────
        tuya_enabled = bool(int(self.settings.get("Settings/Devices/OutbackSPC/Tuya/Enable", int(self.cfg.tuya_enabled))))
        if tuya_enabled and self.cfg.tuya_source != "off":
            # Quelle bestimmen
            if self.cfg.tuya_source == "cli":
                tuya_power = float(self.cfg.tuya_cli_power_w)
            else:
                tuya_power = float(self.settings.get("Settings/Devices/OutbackSPC/Tuya/PowerW", 0))
            self.gen.configure(
                start_w=float(self.settings.get("Settings/Devices/OutbackSPC/Tuya/StartW", 120)),
                stop_w=float(self.settings.get("Settings/Devices/OutbackSPC/Tuya/StopW", 60)),
                min_run_s=float(self.settings.get("Settings/Devices/OutbackSPC/Tuya/MinRunS", 8))
            )
            self.gen.set_connected(True)
            self.gen.update(passthrough=(state==11), tuya_power_w=tuya_power, voltage=acV or 230.0)
        else:
            self.gen.set_connected(False)
            self.gen.update(False, 0.0, voltage=0.0)

        # ── Logging (kompakt + Debug-Rechenweg) ─────────────────
        self.log_core.info("INV   l1_out=%dW | batt=%+dW | state=%s",
                           int(round(L1_multi)), int(round(dcP)), ("Invert" if state==9 else "Charge" if state in (4,5) else "Standby"))
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            self.log_pv.debug(
                "calc: P_pv_ac=clamp(L1(%d)-max(0,-Batt(%d)),0,%d)=%dW",
                int(round(L1_meas)), int(round(dcP)), int(round(L1_meas)), int(round(pv_ac_l1))
            )

        # Summenzeile periodisch
        sp = float(self.settings.get("Settings/Devices/OutbackSPC/SummaryPeriod", self.cfg.summary_period_s))
        if sp > 0 and (now - self._last_summary) >= sp:
            self._last_summary = now
            self.log_core.info(
                "SUM   L1=%d L2=%d L3=%d | PV_ac=%d | GEN=%d | BATT=%+d%s",
                int(round(L1_multi)), int(round(L2P)), int(round(L3P)),
                int(round(pv_ac_l1)),
                int(round(self.gen.svc["/Ac/L1/Power"])) if self.gen else 0,
                int(round(dcP)),
                (f" (SOC={m['/Soc']:.1f})" if "/Soc" in getattr(self.vebus, "paths", {}) else "")
            )

        # Balance-Check (optional)
        if self.cfg.balance_check:
            # Erwartung: L1_meas ≈ pv_ac_l1 + L1_multi (bis auf Glättung)
            resid = L1_meas - (pv_ac_l1 + L1_multi)
            if abs(resid) > 50.0:  # Toleranz
                self.log_core.warning("balance: L1=%.0f vs pv_ac(%.0f)+multi(%.0f) resid=%.0f",
                                      L1_meas, pv_ac_l1, L1_multi, resid)

        # Persistenz-Fortschreibung ggf. speichern
        PV_STORE.save_if_needed(force=False)

        return True

    # GLib-Timer
    def _update(self) -> bool:
        try:
            return self.tick()
        except Exception as e:
            self.log_core.error("update exception: %s", e)
            return True  # weiterlaufen

# ───────────────── CLI/Start ──────────────────────────────────
def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Outback SPC III → Victron D-Bus Bridge (PV→L1 via PV-Inverter)")
    ap.add_argument("--bt",  help="Bluetooth-MAC (überschreibt utils.OUTBACK_ADDRESS)")
    ap.add_argument("--hci", default="hci0", help="BLE-Adapter (hciX)")
    ap.add_argument("--test", choices=["off"]+list(SCENARIOS.keys()),
                    default="off", help="Test-Szenario; 'off' = Live (Standard)")
    ap.add_argument("--testmode", choices=["off","auto","override","night","day","day_plus_batt","day_surplus","gen","custom"],
                    default="off", help="Testmodus über Settings/CLI (auto=automatik-konsistent, override=Direktwerte)")
    ap.add_argument("--debug", nargs='?', const=True, default=False, type=str2bool,
                    help="ausführliches Logging")
    ap.add_argument("--log-format", choices=["text","json"], default="text")
    ap.add_argument("--summary-period", type=float, default=5.0, help="Summenzeile alle N Sekunden (0=aus)")
    ap.add_argument("--dry-run", nargs='?', const=True, default=False, type=str2bool, help="ohne D-Bus (Dummy-Services)")
    ap.add_argument("--once", nargs='?', const=True, default=False, type=str2bool, help="einmaliger Tick und Ende")
    ap.add_argument("--balance-check", nargs='?', const=True, default=False, type=str2bool, help="Bilanzprüfung aktivieren")
    ap.add_argument("--poll-ms", type=int, default=1000, help="Update-Intervall ms")
    ap.add_argument("--bt-interval", type=float, default=OutbackReader.BASE_MIN_INTERVAL, help="Mindest-Rundenintervall s (Basis)")
    ap.add_argument("--bt-backoff-max", type=float, default=OutbackReader.BACKOFF_MAX, help="Max. BLE-Backoff s")
    ap.add_argument("--rate-limit-ms", type=int, default=400, help="Log-Dedupe-RateLimit in ms")
    ap.add_argument("--seed", type=int, help="Zufalls-Seed für Testjitter")

    # Device Instances & Limits
    ap.add_argument("--di-vebus", type=int, default=40)
    ap.add_argument("--di-pvinverter", type=int, default=61)
    ap.add_argument("--di-grid", type=int, default=38)
    ap.add_argument("--l1-limit", type=float, default=3000.0)
    ap.add_argument("--l2-limit", type=float, default=3000.0)
    ap.add_argument("--l3-limit", type=float, default=1500.0)

    # Tuya/Generator
    ap.add_argument("--tuya-enabled", nargs='?', const=True, default=False, type=str2bool, help="Generator-Service aktivieren")
    ap.add_argument("--tuya-source", choices=["settings","cli","off"], default="settings", help="Tuya-Leistungsquelle")
    ap.add_argument("--tuya-power", type=float, default=0.0, help="Tuya-Leistung (W), wenn --tuya-source=cli")

    # Test-Overrides
    ap.add_argument("--test-dcv", type=float, help="Override: Batterie-Spannung (V)")
    ap.add_argument("--test-dci", type=float, help="Override: Batterie-Strom (A)")
    ap.add_argument("--test-dcp", type=float, help="Override: Batterie-Leistung (W)")
    ap.add_argument("--test-soc", type=float, help="Override: Batterie-SOC (%)")

    # Debug/Tools
    ap.add_argument("--dump-now", nargs='?', const=True, default=False, type=str2bool, help="sofortiger Dump (einmal)")
    return ap

def main():
    ap = build_arg_parser()
    args = ap.parse_args()

    setup_logging(fmt=args.log_format, debug=bool(args.debug), rate_limit_ms=args.rate_limit_ms)
    log = logging.getLogger("Core")
    if args.debug:
        logging.getLogger("bluepy").setLevel(logging.WARNING)
    log.info("Start v%s | %s | poll=%dms | min_round=%.1fs backoff<=%.0fs",
             VERSION, ("TEST:"+args.test) if args.test!="off" else "LIVE", args.poll_ms, args.bt_interval, args.bt_backoff_max)

    mac = read_btaddr(args.bt)

    # Reader
    reader = OutbackReader(
        args.hci, mac,
        test=(args.test!="off"),
        scene=(args.test if args.test!="off" else "day_charge"),
        debug=bool(args.debug),
        min_interval_s=args.bt_interval,
        backoff_max_s=args.bt_backoff_max,
        seed=args.seed
    )

    # Settings-Fassade
    dev_settings = DeviceSettings(dbusconnection())
    # Testmode (per CLI initial setzen)
    if args.testmode and args.testmode != "off":
        dev_settings.set("Settings/Devices/OutbackSPC/TestMode", args.testmode)
    # Test-Overrides initialisieren (falls über CLI)
    if args.test_dcv is not None: dev_settings.set("Settings/Devices/OutbackSPC/Test/DCV", args.test_dcv)
    if args.test_dci is not None: dev_settings.set("Settings/Devices/OutbackSPC/Test/DCI", args.test_dci)
    if args.test_dcp is not None: dev_settings.set("Settings/Devices/OutbackSPC/Test/DCP", args.test_dcp)
    if args.test_soc is not None: dev_settings.set("Settings/Devices/OutbackSPC/Test/SOC", args.test_soc)

    # Bridge-Konfiguration
    cfg = BridgeConfig(
        di_vebus=args.di_vebus,
        di_pvinv=args.di_pvinverter,
        di_grid=args.di_grid,
        l1_limit=args.l1_limit,
        l2_limit=args.l2_limit,
        l3_limit=args.l3_limit,
        poll_ms=args.poll_ms,
        summary_period_s=args.summary_period,
        dry_run=bool(args.dry_run),
        rate_limit_ms=args.rate_limit_ms,
        log_format=args.log_format,
        debug=bool(args.debug),
        once=bool(args.once),
        balance_check=bool(args.balance_check),
        tuya_enabled=bool(args.tuya_enabled),
        tuya_source=args.tuya_source,
        tuya_cli_power_w=args.tuya_power
    )

    br = Bridge(reader, cfg, (args.test if args.test!="off" else None), dev_settings)

    if args.dump_now:
        br._dump_now()

    # Einmaliger Tick?
    if cfg.once:
        br.tick()
        PV_STORE.save_if_needed(force=True)
        log.info("Once-mode done.")
        return

    # Hauptloop
    try:
        GLib.MainLoop().run()
    except KeyboardInterrupt:
        log.info("Exiting…")
    finally:
        PV_STORE.save_if_needed(force=True)

if __name__ == "__main__":
    main()
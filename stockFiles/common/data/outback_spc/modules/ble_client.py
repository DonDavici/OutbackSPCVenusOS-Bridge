# -*- coding: utf-8 -*-
"""
Outback BLE Client (A03/A11 Round-Snapshot) – bluepy-only, robust & getunt
- Keine Bleak-Abhängigkeit (vereinfachte Installation auf Venus OS)
- Harte Timeouts: Connect 3.0 s, Read 1.5 s (blockiert Mainloop nicht)
- Exponential-Backoff mit Jitter
- Auto-Tuning des addrType (public ↔ random) bei Fehlserien
- Persistenz: merkt sich "last_good" addrType in /data/outback_spc/state.json
- Detail-Logging im Debug-Modus
Wichtig: PV (pv_w) diagnostisch; Projekt berechnet PV-AC separat (kein Doppelzählen).
"""

from __future__ import annotations
import time, random, struct, os, re, json, logging, threading
from typing import Optional, Dict, Tuple

# ───────── bluepy Import ─────────
try:
    from bluepy.btle import Peripheral, BTLEException  # type: ignore
except Exception:
    Peripheral = None
    BTLEException = Exception  # Fallback, damit Code nicht crasht

# ───────── Outback Services/Chars (wie per Probe bestätigt) ─────────
_SRV_1810 = '00001810-0000-1000-8000-00805f9b34fb'  # A03
_SRV_1811 = '00001811-0000-1000-8000-00805f9b34fb'  # A11
_A03      = '00002a03-0000-1000-8000-00805f9b34fb'
_A11      = '00002a11-0000-1000-8000-00805f9b34fb'

# ───────── Zustände (Heuristik) ─────────
STATE_OFF = 0
STATE_INVERT = 1
STATE_CHARGE = 2
STATE_PASSTHROUGH = 3

# ───────── MAC-Utils ─────────
_MAC_RE = re.compile(r"^[0-9A-F]{12}$")
def _normalize_mac(s: str) -> str:
    if not s: return ""
    raw = re.sub(r"[^0-9A-Fa-f]", "", s).upper()
    if not _MAC_RE.match(raw): return ""
    return ":".join(raw[i:i+2] for i in range(0, 12, 2))

def _resolve_mac(provided: str) -> Tuple[str, str]:
    m = _normalize_mac(provided)
    if m: return m, "CLI"
    env = _normalize_mac(os.getenv("OUTBACK_BLE_MAC", ""))
    if env: return env, "ENV"
    try:
        import utils  # optionales Legacy-Config-Modul
        legacy = _normalize_mac(getattr(utils, "OUTBACK_ADDRESS", ""))
        if legacy: return legacy, "utils"
    except Exception:
        pass
    return "00:35:FF:02:95:99", "default"

# ───────── State-Persistenz ─────────
_STATE_PATH = "/data/outback_spc/state.json"

def _load_state() -> dict:
    try:
        with open(_STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_state(d: dict) -> None:
    try:
        os.makedirs(os.path.dirname(_STATE_PATH), exist_ok=True)
        tmp = _STATE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, indent=2, sort_keys=True)
        os.replace(tmp, _STATE_PATH)
    except Exception:
        pass

def _swap_decode(buf: bytes) -> tuple:
    shorts = struct.unpack('>' + 'h' * (len(buf)//2), buf)
    return tuple(((v >> 8) & 255) | ((v & 255) << 8) for v in shorts)

# ───────── Timeout-Wrapper (threaded) ─────────
class _Box:
    __slots__ = ("val","err")
    def __init__(self): self.val=None; self.err=None

def _call_with_timeout(fn, timeout_s: float):
    box = _Box()
    def run():
        try: box.val = fn()
        except Exception as e: box.err = e
    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(timeout_s)
    if t.is_alive():
        raise TimeoutError(f"BLE call timed out after {timeout_s:.1f}s")
    if box.err is not None:
        raise box.err
    return box.val

# ───────── Hauptklasse: bluepy-only Client ─────────
class BleOutbackClient:
    """
    Einheitliches Interface für den Daemon:
      - snapshot() -> Dict oder None (atomare Runde A03+A11)
      - get_status() -> Dict (für Logs/UI)
    """

    # Debug-Helfer
    def _d(self, msg: str, *args):
        if not getattr(self, "debug", False): return
        try:
            logging.getLogger("BLE").debug(msg, *args)
        except Exception:
            try: print("[BLE DEBUG] " + (msg % args if args else msg))
            except Exception: print("[BLE DEBUG] " + msg)

    def __init__(self, mac: str = "", hci: str = "hci0",
                 min_interval_s: float = 1.8, backoff_max_s: float = 15.0,
                 debug: bool = False):
        self.mac, self._mac_source = _resolve_mac(mac)
        self.hci = hci
        self.min_interval_s = float(min_interval_s or 1.8)
        self.backoff_max_s = float(backoff_max_s or 15.0)
        self.debug = bool(debug)

        # bluepy Backend-Felder
        self._p = None; self._c03 = None; self._c11 = None

        # Takt/Zustand
        self._busy = False
        self._next_at = 0.0                      # sofort beim Start versuchen
        self._last_throttle_log = 0.0
        self._ok = 0; self._fail = 0
        self._acc_read_ms = 0.0; self._acc_skew_ms = 0.0
        self._last_metrics = 0.0
        self._consec_fails = 0

        # addrType Auto-Tuning & Persistenz
        st = _load_state()
        last_good = (st.get("ble") or {}).get("last_good_addr")
        self.addr_type = last_good if last_good in ("public", "random") else "public"
        self._tuned_once = False  # wurde in dieser Session bereits getoggelt?

        # Öffentlicher Status
        self.last_status = "init"
        self.last_error = ""
        self.backend = "bluepy"

        self._d("init: backend=%s mac=%s (src=%s) hci=%s addr=%s min=%.1fs backoff<=%.1fs",
                self.backend, self.mac, self._mac_source, self.hci, self.addr_type,
                self.min_interval_s, self.backoff_max_s)

    # ───────── interne Helfer ─────────
    def _iface_index(self) -> int:
        try:
            return int(self.hci[3:]) if self.hci.startswith("hci") else 0
        except Exception:
            return 0

    def _connect(self):
        if Peripheral is None or not self.mac:
            raise RuntimeError("bluepy nicht verfügbar oder MAC leer (setze --ble-mac / OUTBACK_BLE_MAC / utils.OUTBACK_ADDRESS)")
        self._d("connect: trying mac=%s on %s (%s)", self.mac, self.hci, self.addr_type)

        def _do_connect():
            p = Peripheral(self.mac, iface=self._iface_index(), addrType=self.addr_type)
            s10 = p.getServiceByUUID(_SRV_1810)
            s11 = p.getServiceByUUID(_SRV_1811)
            c03 = s10.getCharacteristics(_A03)[0]
            c11 = s11.getCharacteristics(_A11)[0]
            return p, c03, c11

        p, c03, c11 = _call_with_timeout(_do_connect, 3.0)
        self._p, self._c03, self._c11 = p, c03, c11
        self._consec_fails = 0
        self.last_status = "connected"
        self._d("connect: OK (services ready)")

    def _disconnect(self):
        self._d("disconnect: requested")
        try:
            if self._p: self._p.disconnect()
        except Exception:
            pass
        self._p = self._c03 = self._c11 = None
        self.last_status = "disconnected"
        self._d("disconnect: done")

    def _schedule_next(self, success: bool):
        now = time.monotonic()
        if success:
            delay = self.min_interval_s; self._consec_fails = 0
        else:
            ladder = [1.0, 2.0, 4.0, 8.0, 12.0]
            idx = min(max(self._consec_fails - 1, 0), len(ladder) - 1)
            delay = min(ladder[idx], self.backoff_max_s)
        delay += random.uniform(0.0, 0.2)
        self._next_at = now + delay
        self._d("schedule: next in %.1fs (success=%s, consec=%d)", delay, success, self._consec_fails)

    def _metrics(self):
        now = time.monotonic()
        if now - self._last_metrics < 30.0: return
        self._last_metrics = now
        avg_read = (self._acc_read_ms/self._ok) if self._ok else 0.0
        avg_skew = (self._acc_skew_ms/self._ok) if self._ok else 0.0
        self._d("stats: ok=%d fail=%d avg_read=%.1fms avg_skew=%.1fms next=%.2fs",
                self._ok, self._fail, avg_read, avg_skew, max(0.0, self._next_at - now))

    def _persist_last_good(self):
        st = _load_state()
        st.setdefault("ble", {})["last_good_addr"] = self.addr_type
        _save_state(st)
        self._d("persist: last_good_addr=%s saved", self.addr_type)

    # ───────── öffentliche API ─────────
    def snapshot(self) -> Optional[Dict]:
        """Atomarer Snapshot (A03 + A11) oder None bei (temporärem) Fehler/Throttle."""
        now = time.monotonic()
        if now < self._next_at:
            self.last_status = "throttle"
            if self.debug and (now - self._last_throttle_log > 5.0):
                self._last_throttle_log = now
                self._d("throttle: until %.3f (in %.1fs)", self._next_at, self._next_at - now)
            self._metrics()
            return None

        if self._busy:
            self.last_status = "busy"; self._d("snapshot: busy, skip"); return None
        self._busy = True

        try:
            if not self._p:
                self._connect()

            t0 = time.monotonic()
            raw_a03 = _call_with_timeout(lambda: self._c03.read(), 1.5)
            t_mid = time.monotonic()
            raw_a11 = _call_with_timeout(lambda: self._c11.read(), 1.5)
            t1 = time.monotonic()

            a03 = _swap_decode(raw_a03)
            a11 = _swap_decode(raw_a11)
            read_ms = (t1 - t0) * 1000.0
            skew_ms = (t1 - t_mid) * 1000.0

            acV = a03[2] * 0.1
            acF = a03[3] * 0.1
            l1_power = float(a03[5])
            dcV = a03[8] * 0.01
            dcI = float(a03[9])

            pvV = a11[6] * 0.1
            pvP = float(a11[7])

            try:
                rssi = int(self._p.getRSSI()) if hasattr(self._p, "getRSSI") else 0
            except Exception:
                rssi = 0

            self._ok += 1
            self._acc_read_ms += read_ms
            self._acc_skew_ms += skew_ms
            self._schedule_next(success=True)
            self._metrics()

            # Erfolg → addrType merken
            self._persist_last_good()
            self._tuned_once = False

            self.last_status = "ok"; self.last_error = ""
            self._d("round OK: acV=%.1fV L1=%0.0fW pv=%0.0fW dc=%.2fV %+0.2fA rssi=%d read=%.1fms skew=%.1fms",
                    acV, l1_power, pvP, dcV, dcI, rssi, read_ms, skew_ms)

            return {
                "power_w": max(0.0, l1_power),
                "state": STATE_INVERT,
                "rssi": rssi,
                "pv_w": max(0.0, pvP),
                "ac_v": max(0.0, acV),
                "dc_v": max(0.0, dcV),
                "dc_i": dcI,
                "ts": int(time.time())
            }

        except (TimeoutError, BTLEException) as e:
            self._fail += 1; self._consec_fails += 1
            self.last_status = "timeout" if isinstance(e, TimeoutError) else "btle_error"
            self.last_error = str(e)
            self._d("round FAIL(%s): %s | consec=%d", self.last_status, str(e), self._consec_fails)

            # Auto-Tuning: bei erster Fehlserie >3 Versuchen addrType toggeln (einmal pro Serie)
            if self._consec_fails >= 3 and not self._tuned_once:
                self._tuned_once = True
                self.addr_type = "random" if self.addr_type == "public" else "public"
                self._d("auto-tune: toggled addr_type -> %s", self.addr_type)

            try: self._disconnect()
            except Exception: pass
            self._schedule_next(success=False)
            return None

        except Exception as e:
            self._fail += 1; self._consec_fails += 1
            self.last_status = "error"; self.last_error = str(e)
            self._d("round FAIL: %s | consec=%d", str(e), self._consec_fails)
            try: self._disconnect()
            except Exception: pass
            self._schedule_next(success=False)
            return None

        finally:
            self._busy = False

    def get_status(self) -> dict:
        now = time.monotonic()
        next_in = max(0.0, self._next_at - now)
        return {
            "status": self.last_status,
            "error": self.last_error,
            "ok": self._ok,
            "fail": self._fail,
            "consec_fails": self._consec_fails,
            "next_in_s": round(next_in, 2),
            "mac": self.mac,
            "hci": self.hci,
            "backend": self.backend,
            "addr_type": self.addr_type,
        }
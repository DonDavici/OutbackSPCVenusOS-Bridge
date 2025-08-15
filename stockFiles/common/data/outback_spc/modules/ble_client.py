# -*- coding: utf-8 -*-
"""
Outback BLE Client (A03/A11 Round-Snapshot) – robuste Implementierung:
- Atomare Messung pro Runde (A03 + A11)
- Backoff/Throttle mit Exponentialleiter
- Metriken (ok/fail, avg_read, avg_skew)
- Fällt harmlos auf None zurück, wenn bluepy fehlt oder nicht verbunden
Wichtig: PV-Wert (pv_w) dient nur zur Diagnose; PV-AC im Projekt wird NICHT daraus abgeleitet.
"""

import time, random, struct, os, re, logging
from typing import Optional, Dict

try:
    from bluepy.btle import Peripheral, BTLEException  # type: ignore
except Exception:
    Peripheral = None
    BTLEException = Exception

# Outback Services/Chars (platzhalterisch – typischer Aufbau gemäß v3.0)
_SRV_1810 = '00001810-0000-1000-8000-00805f9b34fb'  # A03
_SRV_1811 = '00001811-0000-1000-8000-00805f9b34fb'  # A11
_A03      = '00002a03-0000-1000-8000-00805f9b34fb'
_A11      = '00002a11-0000-1000-8000-00805f9b34fb'

# Zustände laut Projektvorgabe (Heuristik)
STATE_OFF = 0
STATE_INVERT = 1
STATE_CHARGE = 2
STATE_PASSTHROUGH = 3

# ——— MAC-Utils ———
_MAC_RE = re.compile(r"^[0-9A-F]{12}$")

def _normalize_mac(s: str) -> str:
    """Nimmt beliebiges MAC-Format, gibt AA:BB:CC:DD:EE:FF zurück oder leeren String."""
    if not s:
        return ""
    raw = re.sub(r"[^0-9A-Fa-f]", "", s).upper()
    if not _MAC_RE.match(raw):
        return ""
    return ":".join(raw[i:i+2] for i in range(0, 12, 2))

def _pick_mac(provided: str) -> str:
    """Auflösereihenfolge: CLI > ENV(OUTBACK_BLE_MAC) > utils.OUTBACK_ADDRESS > ''"""
    m = _normalize_mac(provided)
    if m:
        return m
    env = _normalize_mac(os.getenv("OUTBACK_BLE_MAC", ""))
    if env:
        return env
    try:
        import utils  # optionales Legacy-Config-Modul
        legacy = _normalize_mac(getattr(utils, "OUTBACK_ADDRESS", ""))
        if legacy:
            return legacy
    except Exception:
        pass
    # Fallback: Standard-MAC (Dummy, falls kein BLE verfügbar)
    return "00:35:FF:02:95:99"

def _swap_decode(buf: bytes) -> tuple:
    """Outback-Frames (A03/A11) sind big-endian; Bytes vertauschen und als 16-bit vorzeichenbehaftet deuten."""
    shorts = struct.unpack('>' + 'h' * (len(buf)//2), buf)
    return tuple(((v >> 8) & 255) | ((v & 255) << 8) for v in shorts)


class BleOutbackClient:
    """
    BLE-Reader mit rundenweiser Erfassung (A03 & A11) + Backoff.
    snapshot() liefert bei Erfolg ein Dict, sonst None.
    """

    def _d(self, msg: str, *args):
        """Debug-Ausgaben: bevorzugt Python-Logging, fällt auf print zurück."""
        if not getattr(self, "debug", False):
            return
        try:
            logging.getLogger("BLE").debug(msg, *args)
        except Exception:
            try:
                print("[BLE DEBUG] " + (msg % args if args else msg))
            except Exception:
                print("[BLE DEBUG] " + msg)

    def __init__(self, mac: str = "", hci: str = "hci0",
                 min_interval_s: float = 1.8, backoff_max_s: float = 15.0, debug: bool = False):
        self.mac = _pick_mac(mac)
        self.hci = hci
        self.min_interval_s = float(min_interval_s or 1.8)
        self.backoff_max_s = float(backoff_max_s or 15.0)
        self.debug = bool(debug)

        self._p = None
        self._c03 = None
        self._c11 = None
        self._busy = False
        self._next_at = 0.0
        self._last_throttle_log = 0.0
        self._ok = 0
        self._fail = 0
        self._acc_read_ms = 0.0
        self._acc_skew_ms = 0.0
        self._last_metrics = 0.0
        self._consec_fails = 0

        # Öffentliche Statusfelder für externe Logs
        self.last_status = "init"
        self.last_error = ""
        self._d("init: mac=%s (source=%s) hci=%s min=%.1fs backoff<=%.1fs", self.mac, getattr(self, "_mac_source", "?"), self.hci, self.min_interval_s, self.backoff_max_s)

    # ——— intern ———
    def _connect(self):
        if Peripheral is None or not self.mac:
            raise RuntimeError("BLE nicht verfügbar oder MAC leer (setze --ble-mac oder ENV OUTBACK_BLE_MAC oder utils.OUTBACK_ADDRESS)")
        self._d("connect: trying mac=%s on %s", self.mac, self.hci)
        iface = int(self.hci[3:]) if self.hci.startswith("hci") else 0
        self._p = Peripheral(self.mac, iface=iface)
        s10 = self._p.getServiceByUUID(_SRV_1810)
        s11 = self._p.getServiceByUUID(_SRV_1811)
        self._c03 = s10.getCharacteristics(_A03)[0]
        self._c11 = s11.getCharacteristics(_A11)[0]
        self._consec_fails = 0
        self._d("connect: OK (services ready), consec_fails reset")
        self.last_status = "connected"

    def _disconnect(self):
        self._d("disconnect: requested")
        try:
            if self._p:
                self._p.disconnect()
        except Exception:
            pass
        self._p = self._c03 = self._c11 = None
        self._d("disconnect: done")
        self.last_status = "disconnected"

    def _schedule_next(self, success: bool):
        now = time.monotonic()
        if success:
            delay = self.min_interval_s
            self._consec_fails = 0
        else:
            ladder = [1.0, 2.0, 4.0, 8.0, 12.0]
            idx = min(max(self._consec_fails - 1, 0), len(ladder) - 1)
            delay = min(ladder[idx], self.backoff_max_s)
        delay += random.uniform(0.0, 0.2)
        self._next_at = now + delay
        self._d("schedule: next in %.1fs (success=%s, consec_fails=%d)", delay, success, self._consec_fails)

    def _metrics(self):
        now = time.monotonic()
        if now - self._last_metrics < 30.0:
            return
        self._last_metrics = now
        # Ausgabe übernimmt das Hauptlogging; hier nur Rückgabe möglich
        avg_read = (self._acc_read_ms / self._ok) if self._ok else 0.0
        avg_skew = (self._acc_skew_ms / self._ok) if self._ok else 0.0
        return {"ok": self._ok, "fail": self._fail, "avg_read_ms": round(avg_read, 1),
                "avg_skew_ms": round(avg_skew, 1),
                "interval_s": max(0.0, self._next_at - now)}

    # ——— API ———
    def snapshot(self) -> Optional[Dict]:
        """Gibt atomar L1-Power etc. zurück oder None bei (temporärem) Fehler/Throttle."""
        now = time.monotonic()
        # Throttle
        if now < self._next_at:
            self.last_status = "throttle"
            self._d("throttle: until %.3f (in %.1fs)", self._next_at, self._next_at - now)
            if self.debug and (now - self._last_throttle_log > 5.0):
                self._last_throttle_log = now
            self._metrics()
            return None

        if self._busy:
            self.last_status = "busy"
            self._d("snapshot: busy, skipping")
            return None
        self._busy = True

        try:
            if not self._p:
                self._connect()

            t0 = time.monotonic()
            raw_a03 = self._c03.read()
            t_mid = time.monotonic()
            raw_a11 = self._c11.read()
            t1 = time.monotonic()

            a03 = _swap_decode(raw_a03)
            a11 = _swap_decode(raw_a11)

            read_ms = (t1 - t0) * 1000.0
            skew_ms = (t1 - t_mid) * 1000.0

            # A03 typische Indizes (aus v3.0 abgeleitet)
            acV = a03[2] * 0.1
            acF = a03[3] * 0.1
            l1_power = float(a03[5])  # aktive Leistung L1
            dcV = a03[8] * 0.01
            dcI = float(a03[9])

            # A11: PV
            pvV = a11[6] * 0.1
            pvP = float(a11[7])

            # Optionales RSSI (nicht immer verfügbar)
            try:
                rssi = int(self._p.getRSSI()) if hasattr(self._p, "getRSSI") else 0
            except Exception:
                rssi = 0

            self._ok += 1
            self._acc_read_ms += read_ms
            self._acc_skew_ms += skew_ms
            self._schedule_next(success=True)
            self._metrics()

            self.last_status = "ok"
            self.last_error = ""
            self._d("round OK: acV=%.1fV L1=%0.0fW pv=%0.0fW dc=%.2fV %+0.2fA rssi=%d read=%.1fms skew=%.1fms", acV, l1_power, pvP, dcV, dcI, rssi, read_ms, skew_ms)

            # Hinweis: /State kann über BLE nicht sauber gelesen werden → Heuristik „Invert“
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

        except BTLEException as e:
            self._fail += 1
            self._consec_fails += 1
            self.last_status = "btle_error"
            self.last_error = str(e)
            self._d("round FAIL(BTLE): %s | consec=%d", str(e), self._consec_fails)
            # harte Fehler: Disconnect & kurz warten
            self._disconnect()
            self._schedule_next(success=False)
            return None
        except Exception as e:
            self._fail += 1
            self._consec_fails += 1
            self.last_status = "error"
            self.last_error = str(e)
            self._d("round FAIL: %s | consec=%d", str(e), self._consec_fails)
            self._schedule_next(success=False)
            return None
        finally:
            self._busy = False
            # no-op, status bereits gesetzt
            pass

    def get_status(self) -> dict:
        """Kompakter Status für externe Logs/UI."""
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
        }
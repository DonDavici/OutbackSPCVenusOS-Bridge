# -*- coding: utf-8 -*-
"""
BLE-Client (v3-stabil, bluepy/public, minimal)
----------------------------------------------
- Verwendet denselben Connect/Read-Flow wie deine funktionierende v3.0
- Kein Thread-Timeout, kein HCI-Fallback, kein Auto-AddrType
- Deterministisch: Adapter = --hci (Standard hci0), addrType = public
- Ausgabe-Format kompatibel zur neuen Bridge (snapshot/get_status)

Hinweis:
- PV-Werte aus BLE sind diagnostisch; PV→AC-Anteil berechnet die Bridge.
"""

from __future__ import annotations
import time, struct, os, re, logging
from typing import Optional, Dict, Tuple

# bluepy
try:
    from bluepy.btle import Peripheral, BTLEException  # type: ignore
except Exception:
    Peripheral = None
    BTLEException = Exception

log_ble = logging.getLogger("BLE")

# GATT UUIDs (A03/A11)
_SRV_1810 = '00001810-0000-1000-8000-00805f9b34fb'
_SRV_1811 = '00001811-0000-1000-8000-00805f9b34fb'
_A03      = '00002a03-0000-1000-8000-00805f9b34fb'
_A11      = '00002a11-0000-1000-8000-00805f9b34fb'

# Heuristik-State (für Feld "state")
STATE_INVERT = 1

# MAC-Utils
_MAC_RE = re.compile(r"^[0-9A-F]{12}$")
def _normalize_mac(s: str) -> str:
    if not s: return ""
    raw = re.sub(r"[^0-9A-Fa-f]", "", s).upper()
    if not _MAC_RE.match(raw): return ""
    return ":".join(raw[i:i+2] for i in range(0, 12, 2))

def _resolve_mac(cli: Optional[str]) -> Tuple[str, str]:
    m = _normalize_mac(cli or "")
    if m: return m, "CLI"
    env = _normalize_mac(os.getenv("OUTBACK_BLE_MAC", ""))
    if env: return env, "ENV"
    try:
        import utils
        legacy = _normalize_mac(getattr(utils, "OUTBACK_ADDRESS", ""))
        if legacy: return legacy, "utils"
    except Exception:
        pass

    return "", "none"

# Swap/Decode wie v3
def _swap_decode(buf: bytes) -> tuple:
    shorts = struct.unpack('>' + 'h' * (len(buf)//2), buf)
    return tuple(((v >> 8) & 255) | ((v & 255) << 8) for v in shorts)

class BleOutbackClient:
    """
    v3-stabiler BLE-Client:
      - snapshot() -> Dict oder None (A03+A11 in einer Runde)
      - get_status() -> Dict (für Logs)
    """

    BASE_MIN_INTERVAL = 1.8   # exakt wie v3
    BACKOFF_MAX       = 15.0  # v3-Backoff-Leiter

    def __init__(self, mac: str = "", hci: str = "hci0",
                 min_interval_s: float = BASE_MIN_INTERVAL, backoff_max_s: float = BACKOFF_MAX,
                 debug: bool = False):
        self.mac, self._mac_src = _resolve_mac(mac)
        self.hci = hci
        self.min_interval_s = float(min_interval_s or self.BASE_MIN_INTERVAL)
        self.backoff_max_s  = float(backoff_max_s  or self.BACKOFF_MAX)
        self.debug = bool(debug)

        # bluepy Verbindungsobjekte
        self._p = None; self._c03 = None; self._c11 = None

        # Takt/Backoff
        self._busy = False
        self._next_at = 0.0
        self._ok = 0; self._fail = 0; self._consec_fails = 0
        self._last_metrics_ts = 0.0
        self._acc_read_ms = 0.0; self._acc_skew_ms = 0.0

        # Status
        self.last_status = "init"
        self.last_error = ""
        self.backend = "bluepy-v3"
        self.addr_type = "public"  # v3 nutzte public fix

        if self.debug:
            log_ble.debug("init: backend=%s mac=%s(src=%s) hci=%s addr=%s min=%.1fs backoff<=%.1fs",
                          self.backend, (self.mac or "<EMPTY>"), self._mac_src, self.hci, self.addr_type,
                          self.min_interval_s, self.backoff_max_s)

        if not self.mac:
            raise ValueError("Keine BLE-MAC gesetzt. Übergib --ble-mac oder setze OUTBACK_BLE_MAC.")

        # sofort erste Runde
        self._schedule_next(success=True)

    # Helper
    def _iface_index(self) -> int:
        try:
            return int(self.hci[3:]) if self.hci.startswith("hci") else 0
        except Exception:
            return 0

    def _schedule_next(self, *, success: bool):
        now = time.time()
        if success:
            delay = self.min_interval_s; self._consec_fails = 0
        else:
            ladder = [1.0, 2.0, 4.0, 8.0, 12.0]
            idx = min(max(self._consec_fails-1, 0), len(ladder)-1)
            delay = min(ladder[idx], self.backoff_max_s)
        self._next_at = now + delay

    def _metrics(self):
        now = time.time()
        if now - self._last_metrics_ts < 30.0: return
        self._last_metrics_ts = now
        avg_read = self._acc_read_ms/self._ok if self._ok else 0.0
        avg_skew = self._acc_skew_ms/self._ok if self._ok else 0.0
        if self.debug:
            log_ble.debug("v3: stats ok=%d fail=%d avg_read=%.1fms avg_skew=%.1fms next=%.2fs",
                          self._ok, self._fail, avg_read, avg_skew, max(0.0, self._next_at - now))

    # Connect/Disconnect – identisch zum v3-Verhalten
    def _connect(self):
        if Peripheral is None:
            raise RuntimeError("bluepy nicht verfügbar")
        if not self.mac:
            raise RuntimeError("keine BLE-MAC gesetzt (CLI --ble-mac oder ENV OUTBACK_BLE_MAC)")
        iface = self._iface_index()
        if self.debug:
            log_ble.debug("v3: connect mac=%s hci=%s addr=%s", self.mac, self.hci, self.addr_type)
        self._p = Peripheral(self.mac, iface=iface, addrType=self.addr_type)
        s10 = self._p.getServiceByUUID(_SRV_1810)
        s11 = self._p.getServiceByUUID(_SRV_1811)
        self._c03 = s10.getCharacteristics(_A03)[0]
        self._c11 = s11.getCharacteristics(_A11)[0]
        self._consec_fails = 0
        self.last_status = "connected"
        # v3 wartet nicht künstlich; wir bleiben identisch

    def _disconnect(self):
        try:
            if self._p: self._p.disconnect()
        except Exception:
            pass
        self._p = self._c03 = self._c11 = None
        self.last_status = "disconnected"
        if self.debug:
            log_ble.debug("v3: disconnected")

    # Öffentliche API
    def snapshot(self) -> Optional[Dict]:
        now = time.time()
        if now < self._next_at:
            self.last_status = "throttle"
            self._metrics()
            return None
        if self._busy:
            self.last_status = "busy"; return None
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

            acV = a03[2]*0.1
            acF = a03[3]*0.1
            l1_power = float(a03[5])
            dcV = a03[8]*0.01
            dcI = float(a03[9])

            pvV = a11[6]*0.1
            pvP = float(a11[7])

            self._ok += 1
            self._acc_read_ms += (t1 - t0) * 1000.0
            self._acc_skew_ms += (t1 - t_mid) * 1000.0
            self._schedule_next(success=True)
            self._metrics()

            self.last_status = "ok"; self.last_error = ""
            if self.debug:
                log_ble.debug("v3: round OK acV=%.1fV L1=%0.0fW pv=%0.0fW dc=%.2fV %+0.2fA",
                              acV, l1_power, pvP, dcV, dcI)

            return {
                "power_w": max(0.0, l1_power),
                "state": STATE_INVERT,
                "rssi": 0,               # v3: RSSI nicht verlässlich
                "pv_w": max(0.0, pvP),
                "ac_v": max(0.0, acV),
                "dc_v": max(0.0, dcV),
                "dc_i": dcI,
                "ts": int(time.time())
            }

        except BTLEException as e:
            self._fail += 1; self._consec_fails += 1
            self.last_status = "btle_error"; self.last_error = str(e)
            if self.debug: log_ble.debug("v3: FAIL %s", e)
            try: self._disconnect()
            except Exception: pass
            self._schedule_next(success=False)
            return None

        except Exception as e:
            self._fail += 1; self._consec_fails += 1
            self.last_status = "error"; self.last_error = str(e)
            if self.debug: log_ble.debug("v3: FAIL unexpected %s", e)
            try: self._disconnect()
            except Exception: pass
            self._schedule_next(success=False)
            return None

        finally:
            self._busy = False

    def get_status(self) -> dict:
        nxt = max(0.0, self._next_at - time.time())
        return {
            "status": self.last_status,
            "error": self.last_error,
            "ok": self._ok,
            "fail": self._fail,
            "consec_fails": self._consec_fails,
            "next_in_s": round(nxt, 2),
            "mac": self.mac or "<EMPTY>",
            "hci": self.hci,
            "backend": self.backend,
            "addr_type": self.addr_type,
        }
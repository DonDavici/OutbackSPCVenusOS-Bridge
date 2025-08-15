# -*- coding: utf-8 -*-
"""
Outback BLE Client (A03/A11 Round-Snapshot), Bleak-first mit bluepy-Fallback.
- Bevorzugt Bleak (asyncio, BlueZ/DBus), harte Timeouts (Connect 3.0 s, Read 1.5 s)
- Fällt automatisch auf bluepy mit Thread-Timeouts zurück, falls Bleak nicht verfügbar
- Atomare Messung pro Runde (A03 + A11), Backoff/Throttle, Metriken
Wichtig: PV (pv_w) nur diagnostisch; PV-AC wird projektseitig NICHT daraus abgeleitet.
"""

import time, random, struct, os, re, logging, threading, asyncio
from typing import Optional, Dict, Tuple

# ── Versuche zuerst Bleak ─────────────────────────────────────
try:
    from bleak import BleakClient  # type: ignore
    _HAVE_BLEAK = True
except Exception:
    _HAVE_BLEAK = False
    BleakClient = None  # type: ignore

# ── bluepy-Fallback ───────────────────────────────────────────
try:
    from bluepy.btle import Peripheral, BTLEException  # type: ignore
    _HAVE_BLUEPY = True
except Exception:
    _HAVE_BLUEPY = False
    Peripheral = None  # type: ignore
    BTLEException = Exception  # type: ignore

# Outback Services/Chars (Platzhalter – wie bisher)
_SRV_1810 = '00001810-0000-1000-8000-00805f9b34fb'  # A03
_SRV_1811 = '00001811-0000-1000-8000-00805f9b34fb'  # A11
_A03      = '00002a03-0000-1000-8000-00805f9b34fb'
_A11      = '00002a11-0000-1000-8000-00805f9b34fb'

# Zustände laut Projektvorgabe (Heuristik)
STATE_OFF = 0
STATE_INVERT = 1
STATE_CHARGE = 2
STATE_PASSTHROUGH = 3

# ── MAC-Utils ─────────────────────────────────────────────────
import re
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
        import utils  # optional
        legacy = _normalize_mac(getattr(utils, "OUTBACK_ADDRESS", ""))
        if legacy: return legacy, "utils"
    except Exception:
        pass
    return "B0:7E:11:F9:BC:F2", "default"

def _resolve_backend() -> str:
    be = os.getenv("OUTBACK_BLE_BACKEND", "").lower()
    if be in ("bleak", "bluepy"):
        return be
    return ""

def _resolve_addrtype() -> str:
    at = os.getenv("OUTBACK_BLE_ADDRTYPE", "").lower()
    if at in ("public", "random"):
        return at
    return "public"

def _swap_decode(buf: bytes) -> tuple:
    shorts = struct.unpack('>' + 'h' * (len(buf)//2), buf)
    return tuple(((v >> 8) & 255) | ((v & 255) << 8) for v in shorts)

# ── bluepy: Thread-Timeout-Wrapper ───────────────────────────
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

class BleOutbackClient:
    """
    Einheitliches Interface:
      - snapshot() -> Dict oder None
      - get_status() -> Dict für Logs
    Intern: nutzt Bleak oder bluepy (Fallback), jeweils mit harten Timeouts.
    """

    def _d(self, msg: str, *args):
        if not getattr(self, "debug", False): return
        try:
            logging.getLogger("BLE").debug(msg, *args)
        except Exception:
            try: print("[BLE DEBUG] " + (msg % args if args else msg))
            except Exception: print("[BLE DEBUG] " + msg)

    def __init__(self, mac: str = "", hci: str = "hci0",
                 min_interval_s: float = 1.8, backoff_max_s: float = 15.0, debug: bool = False):
        self.mac, self._mac_source = _resolve_mac(mac)
        self.hci = hci
        self.min_interval_s = float(min_interval_s or 1.8)
        self.backoff_max_s = float(backoff_max_s or 15.0)
        self.debug = bool(debug)

        self._next_at = 0.0
        self._busy = False
        self._last_throttle_log = 0.0
        self._ok = 0; self._fail = 0
        self._acc_read_ms = 0.0; self._acc_skew_ms = 0.0
        self._last_metrics = 0.0
        self._consec_fails = 0

        # Backend-spezifisch
        wanted = _resolve_backend()
        if wanted == "bleak":
            self._backend = "bleak" if _HAVE_BLEAK else ("bluepy" if _HAVE_BLUEPY else "none")
        elif wanted == "bluepy":
            self._backend = "bluepy" if _HAVE_BLUEPY else ("bleak" if _HAVE_BLEAK else "none")
        else:
            self._backend = "bleak" if _HAVE_BLEAK else ("bluepy" if _HAVE_BLUEPY else "none")
        self.addr_type = _resolve_addrtype()

        self._client = None      # BleakClient oder bluepy.Peripheral
        self._c03 = None; self._c11 = None

        # Status
        self.last_status = "init"
        self.last_error = ""
        self._d("init: backend=%s addr_type=%s mac=%s (source=%s) hci=%s min=%.1fs backoff<=%.1fs",
                self._backend, self.addr_type, self.mac, self._mac_source, self.hci, self.min_interval_s, self.backoff_max_s)

    # ── Gemeinsame Planung/Stats ───────────────────────────────
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
        self._d("schedule: next in %.1fs (success=%s, consec_fails=%d)", delay, success, self._consec_fails)

    def _metrics(self):
        now = time.monotonic()
        if now - self._last_metrics < 30.0: return
        self._last_metrics = now
        avg_read = (self._acc_read_ms/self._ok) if self._ok else 0.0
        avg_skew = (self._acc_skew_ms/self._ok) if self._ok else 0.0
        self._d("stats: ok=%d fail=%d avg_read=%.1fms avg_skew=%.1fms next=%.2fs",
                self._ok, self._fail, avg_read, avg_skew, max(0.0, self._next_at - now))

    # ── Bleak Backend ─────────────────────────────────────────
    async def _bleak_connect(self):
        self._d("bleak connect: trying %s", self.mac)
        cli = BleakClient(self.mac, timeout=3.0)
        await cli.connect()  # wirft bei Fehler
        # Char-Handles via UUIDs (Bleak nutzt UUID → direkte Reads ok)
        self._client = cli
        self._d("bleak connect: OK")

    async def _bleak_read_pair(self) -> Tuple[bytes, bytes]:
        assert self._client is not None
        cli: BleakClient = self._client  # type: ignore
        # liest beide Characteristics mit eigenem Timeout
        async def read_uuid(u: str, to: float) -> bytes:
            return await asyncio.wait_for(cli.read_gatt_char(u), timeout=to)
        raw_a03 = await read_uuid(_A03, 1.5)
        raw_a11 = await read_uuid(_A11, 1.5)
        return raw_a03, raw_a11

    async def _bleak_disconnect(self):
        try:
            if self._client:
                await asyncio.wait_for(self._client.disconnect(), timeout=1.0)
        except Exception:
            pass
        self._client = None
        self._d("bleak disconnect: done")

    # ── bluepy Backend ────────────────────────────────────────
    def _bluepy_connect(self):
        if not _HAVE_BLUEPY:
            raise RuntimeError("bluepy nicht verfügbar")
        self._d("bluepy connect: trying %s on %s", self.mac, self.hci)
        def _do_connect():
            iface = int(self.hci[3:]) if self.hci.startswith("hci") else 0
            p = Peripheral(self.mac, iface=iface, addrType=self.addr_type)
            s10 = p.getServiceByUUID(_SRV_1810)
            s11 = p.getServiceByUUID(_SRV_1811)
            c03 = s10.getCharacteristics(_A03)[0]
            c11 = s11.getCharacteristics(_A11)[0]
            return p, c03, c11
        p, c03, c11 = _call_with_timeout(_do_connect, 3.0)
        self._client, self._c03, self._c11 = p, c03, c11
        self._d("bluepy connect: OK")

    def _bluepy_read_pair(self) -> Tuple[bytes, bytes]:
        raw_a03 = _call_with_timeout(lambda: self._c03.read(), 1.5)
        raw_a11 = _call_with_timeout(lambda: self._c11.read(), 1.5)
        return raw_a03, raw_a11

    def _bluepy_disconnect(self):
        try:
            if self._client: self._client.disconnect()
        except Exception:
            pass
        self._client = self._c03 = self._c11 = None
        self._d("bluepy disconnect: done")

    # ── Öffentliche API ───────────────────────────────────────
    def snapshot(self) -> Optional[Dict]:
        now = time.monotonic()
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

        t0 = time.monotonic()
        try:
            # Verbindungsaufbau bei Bedarf
            if self._client is None:
                if self._backend == "bleak":
                    if not _HAVE_BLEAK:
                        self._backend = "bluepy"
                    else:
                        asyncio.run(asyncio.wait_for(self._bleak_connect(), timeout=3.5))
                if self._client is None and self._backend == "bluepy":
                    self._bluepy_connect()

            # Lesen (Paar)
            if self._backend == "bleak":
                raw_a03, raw_a11 = asyncio.run(asyncio.wait_for(self._bleak_read_pair(), timeout=3.2))
            else:
                raw_a03, raw_a11 = self._bluepy_read_pair()
            t1 = time.monotonic()

            a03 = _swap_decode(raw_a03)
            a11 = _swap_decode(raw_a11)
            read_ms = (t1 - t0) * 1000.0

            acV = a03[2] * 0.1
            acF = a03[3] * 0.1
            l1_power = float(a03[5])
            dcV = a03[8] * 0.01
            dcI = float(a03[9])

            pvV = a11[6] * 0.1
            pvP = float(a11[7])

            # RSSI (nur bluepy sicher verfügbar; bei Bleak lassen wir 0)
            rssi = 0
            try:
                if _HAVE_BLUEPY and self._backend == "bluepy" and hasattr(self._client, "getRSSI"):
                    rssi = int(self._client.getRSSI())
            except Exception:
                pass

            self._ok += 1
            self._acc_read_ms += read_ms
            self._schedule_next(success=True)
            self._metrics()

            self.last_status = "ok"; self.last_error = ""
            self._d("%s round OK: acV=%.1fV L1=%0.0fW pv=%0.0fW dc=%.2fV %+0.2fA rssi=%d read=%.1fms",
                    self._backend, acV, l1_power, pvP, dcV, dcI, rssi, read_ms)

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

        except (asyncio.TimeoutError, TimeoutError) as e:
            self._fail += 1; self._consec_fails += 1
            self.last_status = "timeout"; self.last_error = str(e)
            self._d("%s TIMEOUT: %s | consec=%d", self._backend, str(e), self._consec_fails)
            try:
                if self._backend == "bleak": asyncio.run(self._bleak_disconnect())
                else: self._bluepy_disconnect()
            except Exception: pass
            self._schedule_next(success=False)
            return None
        except Exception as e:
            self._fail += 1; self._consec_fails += 1
            self.last_status = "error"; self.last_error = str(e)
            self._d("%s FAIL: %s | consec=%d", self._backend, str(e), self._consec_fails)
            try:
                if self._backend == "bleak": asyncio.run(self._bleak_disconnect())
                else: self._bluepy_disconnect()
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
            "backend": self._backend,
            "addr_type": getattr(self, "addr_type", "?"),
        }
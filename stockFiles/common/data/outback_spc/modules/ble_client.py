# -*- coding: utf-8 -*-
"""
Outback BLE Client (A03/A11 Snapshot) – Stub:
- In echter Umgebung sollte hier der Round‑Trip (A03 & A11) mit Backoff/Throttle erfolgen.
- Für diese Paketversion liefern wir None (kein BLE), im Testmodus rechnet testmode.py.
"""

import time
from typing import Optional, Dict


class BleOutbackClient:
    def __init__(self, mac: str = ""):
        self.mac = mac
        self._fail_backoff_s = 1.0
        self._last_try = 0.0

    def snapshot(self) -> Optional[Dict]:
        """
        Liefert dict mit power_w, state, rssi (falls verfügbar).
        Ohne echte BLE‑Implementierung -> None, mit Backoff.
        """
        now = time.monotonic()
        if now - self._last_try < self._fail_backoff_s:
            return None
        self._last_try = now
        self._fail_backoff_s = min(10.0, self._fail_backoff_s * 1.5)
        return None

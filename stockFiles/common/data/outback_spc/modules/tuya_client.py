# -*- coding: utf-8 -*-
"""
Tuya‑Client (Stub):
- Optionaler Zugriff via tinytuya (falls installiert).
- Standardmäßig liefern wir 0 W, Testmodus setzt Werte ohnehin.
"""

from typing import Optional

try:
    import tinytuya  # type: ignore
except Exception:
    tinytuya = None


class TuyaClient:
    def __init__(self, dev_id: str = "", local_key: str = "", address: Optional[str] = None):
        self.dev_id = dev_id
        self.local_key = local_key
        self.address = address
        self._device = None
        if tinytuya and dev_id and local_key:
            try:
                self._device = tinytuya.OutletDevice(dev_id, address, local_key)
                self._device.set_version(3.3)
            except Exception:
                self._device = None

    def read_power(self) -> float:
        if self._device is None:
            return 0.0
        try:
            data = self._device.status()
            for key in ("5", "19", "20", "21"):
                val = data["dps"].get(key)
                if isinstance(val, (int, float)):
                    return float(val)
            return 0.0
        except Exception:
            return 0.0

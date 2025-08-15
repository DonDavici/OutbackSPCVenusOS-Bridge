# -*- coding: utf-8 -*-
"""
ET112-Reader (Stub):
- In echten Setups würden die ET112 als eigene Geräte angebunden (Modbus/DBus).
- Hier liefern wir 0 W; im Testmodus werden L2/L3 über Settings simuliert.
"""

class Et112Reader:
    def __init__(self, source_hint: str = ""):
        self.hint = source_hint

    def read_power(self) -> float:
        return 0.0

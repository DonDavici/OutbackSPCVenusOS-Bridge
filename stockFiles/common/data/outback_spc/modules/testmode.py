# -*- coding: utf-8 -*-
"""
Testmodus: erzeugt konsistente Szenarien (Tag/Nacht/Gen/…)
und kann optional Batterie-Werte übersteuern.
"""

import random
import time
from typing import Dict, Any

from .state_machine import STATE_INVERT, STATE_PASSTHROUGH, compute_pv_ac, clamp


class TestMode:
    def __init__(self, settings, seed: int = 0, scenario: str = "off"):
        self.settings = settings
        self.scenario = scenario
        self.rand = random.Random(seed)
        # Batterie-Modell
        self.capacity_Wh = 5000.0  # Beispiel 48V/100Ah ~ 4.8 kWh
        self.soc = float(self.settings.get("/Settings/Test/Battery/Soc", 75.0))
        self.voltage = float(self.settings.get("/Settings/Test/Battery/Voltage", 52.0))
        self.current = float(self.settings.get("/Settings/Test/Battery/Current", 0.0))
        self.power = float(self.settings.get("/Settings/Test/Battery/Power", 0.0))
        self.override = int(self.settings.get("/Settings/Test/Battery/Override", 0))
        self.last = time.monotonic()

    def _auto_battery(self, dt: float, loads_w: float, pv_ac: float, pv_dc: float, gen_w: float) -> None:
        """
        Modus A: automatische, konsistente Batterie: P_batt = ΣLoads - (PV_ac + PV_dc + Gen).
        SOC-Fortschreibung mit ETA.
        """
        batt_p = loads_w - (pv_ac + pv_dc + gen_w)
        eta = 0.97 if batt_p > 0 else 0.95
        dWh = batt_p * dt / 3600.0 * (1.0 if batt_p < 0 else 1.0/eta)
        d_soc = -dWh / max(1.0, self.capacity_Wh) * 100.0
        self.soc = clamp(self.soc + d_soc, 0.0, 100.0)
        self.power = batt_p
        self.current = batt_p / max(1.0, self.voltage)

    def _scenario_values(self) -> Dict[str, float]:
        L1 = self.settings.get("/Settings/Test/L1", 400.0)
        L2 = self.settings.get("/Settings/Test/L2", 0.0)
        L3 = self.settings.get("/Settings/Test/L3", 0.0)
        PV_AC = self.settings.get("/Settings/Test/PV_AC", 300.0)
        PV_DC = self.settings.get("/Settings/Test/PV_DC", 0.0)
        GEN = self.settings.get("/Settings/Test/GenPower", 0.0)

        sc = self.scenario
        if sc == "night":
            L1 = 400.0 if L1 is None else L1
            PV_AC = 0.0
            GEN = 0.0
        elif sc == "day":
            L1 = 500.0 if L1 is None else L1
            PV_AC = L1
            GEN = 0.0
        elif sc == "day_plus_batt":
            L1 = 800.0 if L1 is None else L1
            PV_AC = 500.0 if self.settings.get("/Settings/Test/PV_AC", None) is None else PV_AC
            GEN = 0.0
        elif sc == "day_surplus":
            L1 = 300.0 if L1 is None else L1
            PV_AC = 600.0 if self.settings.get("/Settings/Test/PV_AC", None) is None else PV_AC
            GEN = 0.0
        elif sc == "gen":
            L1 = 1600.0 if L1 is None else L1
            PV_AC = 500.0 if self.settings.get("/Settings/Test/PV_AC", None) is None else PV_AC
            GEN = 1200.0 if self.settings.get("/Settings/Test/GenPower", None) is None else GEN
        elif sc == "custom":
            L1 = 900.0 if L1 is None else L1
            PV_AC = 620.0 if PV_AC is None else PV_AC
        elif sc == "off":
            pass

        return dict(L1=float(L1), L2=float(L2), L3=float(L3), PV_AC=float(PV_AC), PV_DC=float(PV_DC), GEN=float(GEN))

    def step(self, dt: float) -> Dict[str, Any]:
        v = self._scenario_values()
        loads = v["L1"] + v["L2"] + v["L3"]

        if self.override:
            self.voltage = float(self.settings.get("/Settings/Test/Battery/Voltage", self.voltage))
            self.current = float(self.settings.get("/Settings/Test/Battery/Current", self.current))
            self.power = float(self.settings.get("/Settings/Test/Battery/Power", self.power))
            self.soc = float(self.settings.get("/Settings/Test/Battery/Soc", self.soc))
        else:
            self._auto_battery(dt=dt, loads_w=loads, pv_ac=v["PV_AC"], pv_dc=v["PV_DC"], gen_w=v["GEN"])

        outback_state = STATE_PASSTHROUGH if (self.scenario == "gen" and v["GEN"] > 0.0) else STATE_INVERT

        v["PV_AC"] = compute_pv_ac(v["L1"], self.power)

        return {
            "L1": v["L1"], "L2": v["L2"], "L3": v["L3"],
            "PV_AC": v["PV_AC"], "PV_DC": v["PV_DC"], "GEN": v["GEN"],
            "BATT_V": self.voltage, "BATT_I": self.current, "BATT_P": self.power, "BATT_SOC": self.soc,
            "OUTBACK_STATE": outback_state
        }

    def read_battery_live_fallback(self):
        return {"V": self.voltage, "I": self.current, "P": self.power, "SOC": self.soc}

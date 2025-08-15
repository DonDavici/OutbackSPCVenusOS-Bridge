# -*- coding: utf-8 -*-
"""
Kernlogik:
- PV‑AC‑Berechnung ohne Doppelzählung
- Zustandsklassifikation mit Hysterese
- EMA‑Glättung & Hilfen
"""

from dataclasses import dataclass


# Outback-/Inverter-States (Enum)
STATE_OFF = 0
STATE_INVERT = 1
STATE_CHARGE = 2
STATE_PASSTHROUGH = 3


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def compute_pv_ac(P_L1_out: float, P_batt: float) -> float:
    """
    Zentrale Formel gegen Doppelzählungen:
    P_pv_ac = clamp( P_L1_out - max(0, -P_batt), 0, P_L1_out )
    Hinweis: P_batt > 0 = Laden (Energie in Batterie), P_batt < 0 = Entladen.
    """
    return clamp(P_L1_out - max(0.0, -P_batt), 0.0, P_L1_out)


def classify_state(pv_ac: float, l1_out: float, batt_p: float, outback_state: int, gen_power: float, eps: float = 50.0) -> str:
    """
    State-Machine mit Hysterese (eps):
    - DAY_PV_DIRECT: PV deckt L1 (Batterie ~0)
    - DAY_PV_PLUS_BATT: PV reicht nicht, Batterie liefert
    - DAY_PV_SURPLUS: PV > L1 (DC-Laden nur via MPPT, hier nicht gemeldet)
    - NIGHT_BATT: PV≈0, Batterie versorgt
    - GEN_PASSTHROUGH: Outback Passthrough + Generatorleistung über Schwelle
    """
    # Generator vorziehen
    if outback_state == STATE_PASSTHROUGH and gen_power > eps:
        return "GEN_PASSTHROUGH"

    if pv_ac <= eps and l1_out > eps:
        # Nachtfall: keine AC-PV, Last > 0
        return "NIGHT_BATT"

    # Tagfälle
    diff = pv_ac - l1_out
    if abs(batt_p) <= eps and abs(diff) <= eps:
        return "DAY_PV_DIRECT"
    if diff >= eps:
        return "DAY_PV_SURPLUS"
    if diff <= -eps and batt_p < -eps:
        return "DAY_PV_PLUS_BATT"

    # Fallback
    if pv_ac > eps:
        return "DAY_PV_DIRECT"
    return "NIGHT_BATT"


@dataclass
class EMA:
    """Einfache Exponential Moving Average (α≈0,3)."""
    alpha: float = 0.3
    _y: float = None

    def update(self, x: float) -> float:
        if self._y is None:
            self._y = x
        else:
            self._y = self.alpha * x + (1.0 - self.alpha) * self._y
        return self._y

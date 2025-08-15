# -*- coding: utf-8 -*-
"""
Detailliertere Szenario-Prüfungen auf Logikebene, ohne D-Bus.
"""

import sys
import os
BASE = os.path.dirname(os.path.dirname(__file__))
MODROOT = os.path.join(BASE, "stockFiles", "common", "data", "outback_spc")
sys.path.insert(0, MODROOT)

from modules.state_machine import compute_pv_ac, classify_state, STATE_PASSTHROUGH

def test_cases():
    results = []

    # 1
    results.append(("Case1", compute_pv_ac(500, 0) == 500))
    # 2
    results.append(("Case2", compute_pv_ac(800, -300) == 500))
    # 3
    results.append(("Case3", compute_pv_ac(300, +100) == 300))
    # 4
    results.append(("Case4", compute_pv_ac(400, -400) == 0))
    # 5
    st = classify_state(500, 1600, -100, STATE_PASSTHROUGH, 1200)
    results.append(("Case5", st == "GEN_PASSTHROUGH"))

    for name, ok in results:
        print(name, "PASS" if ok else "FAIL")

if __name__ == "__main__":
    test_cases()

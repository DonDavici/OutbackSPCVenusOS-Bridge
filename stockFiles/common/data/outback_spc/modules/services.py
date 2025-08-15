# -*- coding: utf-8 -*-
"""
Outback SPC Venus Bridge main module
"""

from __future__ import annotations
import os
import sys
import time
import logging

from modules.services import InverterOutbackService, PVInverterService

log_core = logging.getLogger("core")

SVC_INV = None  # type: InverterOutbackService | None
SVC_PV  = None  # type: PVInverterService  | None

def _dbus_publish(pv_l1_power: float, l1_power: float, ac_voltage: float, state: int, last_ble_ts: int, rssi: int, pv_forward_kwh: float = None):
    global SVC_INV, SVC_PV
    try:
        if SVC_PV is not None:
            SVC_PV.update(power=float(pv_l1_power), forward_kwh=float(pv_forward_kwh or 0.0))
    except Exception as e:
        log_core.debug(f"dbus publish (pv) failed: {e}")
    try:
        if SVC_INV is not None:
            i_current = (float(l1_power)/float(ac_voltage)) if ac_voltage else 0.0
            SVC_INV.update(voltage=float(ac_voltage), current=float(i_current), power=float(l1_power), state=int(state), last_ble_update=int(last_ble_ts), rssi=int(rssi))
    except Exception as e:
        log_core.debug(f"dbus publish (inv) failed: {e}")

def main():
    # ... existing initialization code ...

    log_core.info(f"startup: BLE ...")

    # --- D‑Bus Services sofort registrieren ---
    try:
        di_inv = int(getattr(settings, 'get', lambda k, d: d)("/Settings/Devices/OutbackSPC/DeviceInstance/Inverter", args.di_inverter if hasattr(args, 'di_inverter') else 18))
        di_pv  = int(getattr(settings, 'get', lambda k, d: d)("/Settings/Devices/OutbackSPC/DeviceInstance/PvInverter", args.di_pvinverter if hasattr(args, 'di_pvinverter') else 28))
    except Exception:
        di_inv, di_pv = 18, 28
    global SVC_INV, SVC_PV
    log_core.info(f"dbus: registering services di_inv={di_inv} di_pv={di_pv} l1_limit={args.l1_limit}")
    SVC_PV  = PVInverterService("com.victronenergy.pvinverter.outback_l1", device_instance=di_pv,  fw=VERSION, dry=False, power_limit=int(args.l1_limit))
    SVC_INV = InverterOutbackService("com.victronenergy.inverter.outback_l1", device_instance=di_inv, fw=VERSION, dry=False, power_limit=int(args.l1_limit))
    # Sichtprüfung: sind die Services auf dem System‑Bus?
    try:
        import dbus
        names = dbus.SystemBus().list_names()
        log_core.debug(f"dbus present pvinverter.outback_l1={ 'com.victronenergy.pvinverter.outback_l1' in names }")
        log_core.debug(f"dbus present inverter.outback_l1={ 'com.victronenergy.inverter.outback_l1' in names }")
    except Exception:
        pass

    # ... rest of main initialization ...

    while True:
        # ... code that updates values and logs PV INFO, INV INFO, CORE INFO ...

        log_core.info(f"PV INFO: ...")  # existing log
        log_core.info(f"INV INFO: ...")  # existing log
        log_core.info(f"CORE INFO: ...")  # existing log

        # --- D‑Bus Publizieren ---
        _dbus_publish(
            pv_l1_power=float(pv_ac_l1),
            l1_power=float(l1_out),
            ac_voltage=float(ac_v),
            state=int(inv_state),
            last_ble_ts=int(time.time()),
            rssi=int(getattr(reader, 'rssi', 0) or 0),
            pv_forward_kwh=float(pv_forward_kwh)
        )

        # ... rest of loop ...

# Rest of file remains unchanged

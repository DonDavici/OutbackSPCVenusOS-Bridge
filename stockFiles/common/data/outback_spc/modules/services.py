# -*- coding: utf-8 -*-
"""
D-Bus Service-Kapselung für Outback-SPC Venus Bridge
---------------------------------------------------
- Legt die geforderten Services auf dem **System-Bus** an
- Pflegt Pflichtpfade und `/UpdateIndex`
- Liefert `update()`-Methoden für schlankes Publizieren
"""
from __future__ import annotations

import os
import sys
import logging
from typing import Any

from .dbus_helpers import VeDbusServiceWrapper

log = logging.getLogger("services")


def _common_init(svc: VeDbusServiceWrapper, device_instance: int, product_name: str, product_id: int, fw: str):
    """Standard- und Management-Keys setzen."""
    svc.add("/DeviceInstance", int(device_instance))
    svc.add("/ProductName", str(product_name))
    svc.add("/ProductId", int(product_id))
    svc.add("/FirmwareVersion", str(fw))
    svc.add("/Connected", 1)
    svc.add("/Mgmt/ProcessName", os.path.basename(sys.argv[0]))
    svc.add("/Mgmt/ProcessVersion", str(fw))
    svc.add("/Info/TestMode", 0)
    svc.add("/UpdateIndex", 0)
    log.debug("common init: product=%s di=%d", product_name, device_instance)


def _bump_update_index(svc: VeDbusServiceWrapper):
    try:
        cur = int(svc.get("/UpdateIndex", 0))
        log.debug("update-index: current=%d", cur)
        svc.set("/UpdateIndex", (cur + 1) % 256)
        log.debug("update-index: new=%d", (cur + 1) % 256)
    except Exception:
        log.warning("update-index: fallback reset to 1")
        svc.set("/UpdateIndex", 1)


class InverterOutbackService:
    """com.victronenergy.inverter.outback_l1 – reine AC-Abgabe + State"""

    def __init__(self, name: str, device_instance: int, fw: str, dry: bool, power_limit: int):
        log.info("Registering service '%s' (INV) di=%d fw=%s dry=%s limit=%d", name, device_instance, fw, dry, power_limit)
        self.svc = VeDbusServiceWrapper(name, dry=dry, register=False)
        _common_init(self.svc, device_instance, "Outback SPC III (L1)", 0xA001, fw)
        self.svc.add("/Ac/Out/L1/Voltage", 0.0)
        self.svc.add("/Ac/Out/L1/Current", 0.0)
        self.svc.add("/Ac/Out/L1/Power", 0.0)
        self.svc.add("/Ac/Out/L1/PowerLimit", int(power_limit))
        self.svc.add("/State", 0)  # 0=Off, 1=Invert, 2=Charge, 3=Passthrough
        self.svc.add("/Info/LastBleUpdate", 0)
        self.svc.add("/Info/Rssi", 0)
        self.svc.register()
        log.info("Service '%s' registered successfully.", name)

    def update(self, voltage: float, current: float, power: float, state: int, last_ble_update: int, rssi: int):
        self.svc.set("/Ac/Out/L1/Voltage", float(voltage))
        self.svc.set("/Ac/Out/L1/Current", float(current))
        self.svc.set("/Ac/Out/L1/Power", float(power))
        self.svc.set("/State", int(state))
        self.svc.set("/Info/LastBleUpdate", int(last_ble_update))
        self.svc.set("/Info/Rssi", int(rssi))
        _bump_update_index(self.svc)

    def set_test_mode(self, on: int):
        self.svc.set("/Info/TestMode", int(on))
        _bump_update_index(self.svc)


class PVInverterService:
    """com.victronenergy.pvinverter.outback_l1 – AC-PV Anteil auf L1 (niemals flappen)."""

    def __init__(self, name: str, device_instance: int, fw: str, dry: bool, power_limit: int):
        log.info("Registering service '%s' (PV) di=%d fw=%s dry=%s limit=%d", name, device_instance, fw, dry, power_limit)
        self.svc = VeDbusServiceWrapper(name, dry=dry, register=False)
        _common_init(self.svc, device_instance, "AC-PV (Outback L1)", 0xA002, fw)
        self.svc.add("/Ac/L1/Power", 0.0)
        self.svc.add("/Ac/L1/Energy/Forward", 0.0)  # kWh
        self.svc.add("/Ac/Power", 0.0)
        self.svc.add("/Ac/Energy/Forward", 0.0)
        self.svc.add("/Ac/Out/L1/PowerLimit", int(power_limit))
        self.svc.add("/Position", 1)  # 1 = AC-Out
        self.svc.set("/Connected", 1)
        self.svc.register()
        log.info("Service '%s' registered successfully.", name)

    def update(self, power: float, forward_kwh: float):
        p = float(max(0.0, power))
        self.svc.set("/Ac/L1/Power", p)
        self.svc.set("/Ac/Power", p)
        self.svc.set("/Ac/L1/Energy/Forward", float(forward_kwh))
        self.svc.set("/Ac/Energy/Forward", float(forward_kwh))
        _bump_update_index(self.svc)

    def set_test_mode(self, on: int):
        self.svc.set("/Info/TestMode", int(on))
        _bump_update_index(self.svc)


class GridGeneratorService:
    """com.victronenergy.grid.generator_tuya – Generator/AC-In (nur bei Passthrough aktiv)."""

    def __init__(self, name: str, device_instance: int, fw: str, dry: bool, power_limit: int):
        log.info("Registering service '%s' (GEN) di=%d fw=%s dry=%s limit=%d", name, device_instance, fw, dry, power_limit)
        self.svc = VeDbusServiceWrapper(name, dry=dry, register=False)
        _common_init(self.svc, device_instance, "Generator via Tuya", 0xA003, fw)
        self.svc.add("/Ac/L1/Voltage", 0.0)
        self.svc.add("/Ac/L1/Current", 0.0)
        self.svc.add("/Ac/L1/Power", 0.0)
        self.svc.add("/Status/Running", 0)
        self.svc.add("/Ac/Out/L1/PowerLimit", int(power_limit))
        self.svc.register()
        log.info("Service '%s' registered successfully.", name)

    def update(self, voltage: float, current: float, power: float, running: int):
        self.svc.set("/Ac/L1/Voltage", float(voltage))
        self.svc.set("/Ac/L1/Current", float(current))
        self.svc.set("/Ac/L1/Power", float(power))
        self.svc.set("/Status/Running", int(running))
        _bump_update_index(self.svc)

    def set_test_mode(self, on: int):
        self.svc.set("/Info/TestMode", int(on))
        _bump_update_index(self.svc)


class AcMeterService:
    """com.victronenergy.acmeter.et112_{L2|L3} – getrennte Abgaben, inkl. Forward-Zähler."""

    def __init__(self, name: str, device_instance: int, phase: str, fw: str, dry: bool, power_limit: int):
        assert phase in ("L2", "L3")
        log.info("Registering service '%s' (ACM %s) di=%d fw=%s dry=%s limit=%d", name, phase, device_instance, fw, dry, power_limit)
        self.phase = phase
        self.svc = VeDbusServiceWrapper(name, dry=dry, register=False)
        _common_init(self.svc, device_instance, f"ET112 ({phase})", 0xA004, fw)
        self.svc.add(f"/Ac/Out/{phase}/Voltage", 0.0)
        self.svc.add(f"/Ac/Out/{phase}/Current", 0.0)
        self.svc.add(f"/Ac/Out/{phase}/Power", 0.0)
        self.svc.add("/Ac/Energy/Forward", 0.0)
        self.svc.add(f"/Ac/Out/{phase}/PowerLimit", int(power_limit))
        self.svc.register()
        log.info("Service '%s' registered successfully.", name)

    def update(self, power: float, voltage: float, current: float, forward_kwh: float):
        p = float(max(0.0, power))
        self.svc.set(f"/Ac/Out/{self.phase}/Voltage", float(voltage))
        self.svc.set(f"/Ac/Out/{self.phase}/Current", float(current))
        self.svc.set(f"/Ac/Out/{self.phase}/Power", p)
        self.svc.set("/Ac/Energy/Forward", float(forward_kwh))
        _bump_update_index(self.svc)

    def set_test_mode(self, on: int):
        self.svc.set("/Info/TestMode", int(on))
        _bump_update_index(self.svc)

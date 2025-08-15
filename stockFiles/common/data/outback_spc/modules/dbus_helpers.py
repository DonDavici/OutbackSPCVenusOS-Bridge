# -*- coding: utf-8 -*-
"""
Hilfsfunktionen und Stubs für D‑Bus/vedbus & Settings.
Lokal (Dry‑Run) werden minimalistische Stubs genutzt. Auf Venus OS
werden automatisch die echten Klassen verwendet, falls verfügbar.
"""

import os
from typing import Any, Dict
import logging
log_dbus = logging.getLogger("DBUS")

REAL_DBUS = False
try:
    # Auf Venus OS vorhanden
    from vedbus import VeDbusService, VeDbusItemImport  # type: ignore
    REAL_DBUS = True
except Exception:
    VeDbusService = None  # Stub folgt
    VeDbusItemImport = None
    REAL_DBUS = False

try:
    import dbus  # type: ignore
except Exception:
    dbus = None


def _get_system_bus():
    if dbus is None:
        return None
    try:
        return dbus.SystemBus()
    except Exception:
        return None


def is_real_dbus() -> bool:
    return REAL_DBUS


def ensure_data_dir(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass


class _StubVeDbusService:
    """Minimaler VeDbusService-Ersatz für lokale Tests."""
    def __init__(self, name: str):
        self.name = name
        self.paths: Dict[str, Any] = {}
        self._registered = False

    def add_path(self, path: str, value=None, writeable=True, onchangecallback=None, gettextcallback=None):
        self.paths[path] = value
        return True

    def register(self):
        self._registered = True

    def __setitem__(self, path: str, value: Any):
        self.paths[path] = value

    def __getitem__(self, path: str) -> Any:
        return self.paths.get(path)


class VeDbusServiceWrapper:
    """
    Vereinheitlicht Zugriff auf echten VeDbusService und Stub.
    - Auf Venus OS: nutzt **SystemBus** explizit.
    - `register()` steht zur Verfügung (bei echtem Service wird wirklich registriert).
    """
    def __init__(self, name: str, dry: bool = False, register: bool = True):
        self.name = name
        self.dry = dry or not REAL_DBUS
        self._bus = _get_system_bus()
        if self.dry or self._bus is None or VeDbusService is None:
            self._svc = _StubVeDbusService(name)
        else:
            # Erst ohne auto-Register anlegen, damit wir zunächst Management-Pfade setzen können
            self._svc = VeDbusService(name, bus=self._bus, register=False)
        # Standard-Mgmt-Pfade hinzufügen; /Connected dann vom Aufrufer gesetzt
        try:
            self.add("/Mgmt/ProcessName", os.path.basename(__file__))
            self.add("/Mgmt/ProcessVersion", "python")
        except Exception:
            pass
        # Optional sofort registrieren
        if register:
            self.register()

    def register(self):
        try:
            self._svc.register()
            log_dbus.debug("registered service on system bus: %s", self.name)
        except Exception:
            # Stub oder bereits registriert
            pass

    def add(self, path: str, value=None):
        try:
            self._svc.add_path(path, value=value, writeable=True)
        except Exception:
            # Stub
            if isinstance(self._svc, _StubVeDbusService):
                self._svc.add_path(path, value=value, writeable=True)

    def set(self, path: str, value):
        try:
            self._svc[path] = value
        except Exception:
            try:
                self._svc.add_path(path, value=value, writeable=True)
            except Exception:
                pass

    def get(self, path: str, default=None):
        try:
            return self._svc[path]
        except Exception:
            return default

    @property
    def raw(self):
        """Zugriff auf das native Service-Objekt (VeDbusService oder Stub)."""
        return self._svc


class SettingsStore:
    """
    Schlanker Settings-Ersatz (com.victronenergy.settings).
    Persistiert in der übergebenen State-Referenz (JSON-Datei wird
    vom Hauptprogramm gespeichert).
    """
    def __init__(self, state_ref: Dict[str, Any]):
        self.state_ref = state_ref
        self.state_ref.setdefault("settings", {})

    def ensure_defaults(self, defaults: Dict[str, Any]):
        for k, v in defaults.items():
            self.state_ref["settings"].setdefault(k, v)

    def get(self, key: str, default=None):
        return self.state_ref.get("settings", {}).get(key, default)

    def set(self, key: str, value: Any):
        self.state_ref["settings"][key] = value

# Am Dateiende (oder nach SettingsStore) hinzufügen:
class BatteryDbusReader:
    """
    Liest – falls verfügbar – den ersten com.victronenergy.battery.* Service:
    /Dc/0/Voltage, /Dc/0/Current, /Dc/0/Power, /Soc
    Rückgabe: {"V":float,"I":float,"P":float,"SOC":float} oder None.
    """
    def __init__(self):
        self._bus = None
        self._paths = None
        if REAL_DBUS and dbus is not None and VeDbusItemImport is not None:
            try:
                # System-Bus
                self._bus = dbus.SystemBus()
                names = self._bus.list_names()
                target = next((n for n in names if str(n).startswith("com.victronenergy.battery.")), None)
                if target:
                    self._paths = {
                        "V": VeDbusItemImport(self._bus, target, "/Dc/0/Voltage"),
                        "I": VeDbusItemImport(self._bus, target, "/Dc/0/Current"),
                        "P": VeDbusItemImport(self._bus, target, "/Dc/0/Power"),
                        "SOC": VeDbusItemImport(self._bus, target, "/Soc"),
                    }
            except Exception:
                self._bus = None
                self._paths = None

    def read(self):
        if not self._paths:
            return None
        try:
            v = float(self._paths["V"].get_value())
            i = float(self._paths["I"].get_value())
            p = float(self._paths["P"].get_value())
            soc = float(self._paths["SOC"].get_value())
            return {"V": v, "I": i, "P": p, "SOC": soc}
        except Exception:
            return None


def list_system_services_prefix(prefix: str = "com.victronenergy.") -> list:
    if not REAL_DBUS or dbus is None:
        return []
    try:
        bus = _get_system_bus()
        names = bus.list_names()
        return [n for n in names if str(n).startswith(prefix)]
    except Exception:
        return []

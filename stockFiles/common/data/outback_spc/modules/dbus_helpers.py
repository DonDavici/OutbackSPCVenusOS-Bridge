# -*- coding: utf-8 -*-
"""
Hilfsfunktionen und Stubs für D‑Bus/vedbus & Settings.
Lokal (Dry‑Run) werden minimalistische Stubs genutzt. Auf Venus OS
werden automatisch die echten Klassen verwendet, falls verfügbar.
"""

import os
from typing import Any, Dict

REAL_DBUS = False
try:
    # Auf Venus OS vorhanden
    from vedbus import VeDbusService  # type: ignore
    REAL_DBUS = True
except Exception:
    VeDbusService = None  # Stub folgt


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

    def add_path(self, path: str, value=None, writeable=True, onchangecallback=None, gettextcallback=None):
        self.paths[path] = value
        return True

    def __setitem__(self, path: str, value: Any):
        self.paths[path] = value

    def __getitem__(self, path: str) -> Any:
        return self.paths.get(path)


class VeDbusServiceWrapper:
    """
    Vereinheitlicht Zugriff auf echten VeDbusService und Stub.
    """
    def __init__(self, name: str, dry: bool = False):
        self.name = name
        self.dry = dry or not REAL_DBUS
        self._svc = _StubVeDbusService(name) if self.dry else VeDbusService(name)

    def add(self, path: str, value=None):
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

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hauptprogramm: Orchestriert D‑Bus‑Services, Testmodus, Polling, EMA‑Glättung,
PV‑AC‑Berechnung ohne Doppelzählungen, Generator-Logik mit AND-Bedingung
und persistente Forward‑Zähler. Entwickelt für Venus OS (Raspberry Pi),
läuft aber auch lokal im Dry‑Run dank Stubs.
"""

import argparse
import json
import os
import signal
import sys
import time
from datetime import date
from typing import Dict, Any
import subprocess
import shlex
import re

# Lokale Module
from modules.loggerx import make_logger, Summary
from modules.state_machine import (
    compute_pv_ac, classify_state,
    STATE_OFF, STATE_INVERT, STATE_CHARGE, STATE_PASSTHROUGH,
    clamp, EMA
)
from modules.dbus_helpers import (
    is_real_dbus, VeDbusServiceWrapper, SettingsStore, ensure_data_dir, BatteryDbusReader
)
from modules.services import (
    InverterOutbackService, PVInverterService, GridGeneratorService, AcMeterService
)
from modules.testmode import TestMode
from modules.ble_client import BleOutbackClient
from modules.tuya_client import TuyaClient
from modules.et112_reader import Et112Reader

VERSION = "1.0.0"

DATA_DIR = "/data/outback_spc"
STATE_FILE = os.path.join(DATA_DIR, "state.json")

DEFAULT_DEVICE_INSTANCES = dict(inverter=18, pvinverter=28, grid=38, l2=48, l3=58)

RUN = True


def load_state() -> Dict[str, Any]:
    ensure_data_dir(DATA_DIR)
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"pv_forward_kwh": 0.0, "last_reset_ymd": "", "l2_forward_kwh": 0.0, "l3_forward_kwh": 0.0, "settings": {}}


def save_state(state: Dict[str, Any]) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_FILE)


def setup_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Outback SPC → Venus OS D‑Bus Bridge (L1/L2/L3, PV‑AC ohne Doppelzählung)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # Logging/Allgemein
    p.add_argument("--debug", action="store_true", help="Global DEBUG")
    p.add_argument("--log-format", choices=["text", "json"], default="text", help="Log-Format")
    p.add_argument("--summary-period", type=int, default=5, help="Sekunden für Summenzeile")
    p.add_argument("--dry-run", action="store_true", help="Kein echter D‑Bus, nur Stubs/Logs")
    p.add_argument("--once", action="store_true", help="Nur einen Poll‑Zyklus ausführen")
    p.add_argument("--dump-now", action="store_true", help="Sofortige Summenausgabe")
    p.add_argument("--balance-check", action="store_true", help="Bilanzprüfung pro Zyklus ausgeben")

    # Geräte-IDs / Limits
    p.add_argument("--service-prefix", default="", help="Optionaler Präfix für Servicenamen")
    p.add_argument("--di-inverter", type=int, default=DEFAULT_DEVICE_INSTANCES["inverter"])
    p.add_argument("--di-pvinverter", type=int, default=DEFAULT_DEVICE_INSTANCES["pvinverter"])
    p.add_argument("--di-grid", type=int, default=DEFAULT_DEVICE_INSTANCES["grid"])
    p.add_argument("--di-l2", type=int, default=DEFAULT_DEVICE_INSTANCES["l2"])
    p.add_argument("--di-l3", type=int, default=DEFAULT_DEVICE_INSTANCES["l3"])

    p.add_argument("--l1-limit", type=int, default=3000)
    p.add_argument("--l2-limit", type=int, default=3000)
    p.add_argument("--l3-limit", type=int, default=1500)

    # Quellen
    p.add_argument("--ble-mac", default="", help="Outback BLE MAC (optional)")
    p.add_argument("--ble-addrtype", choices=["public", "random"], default=None,
                   help="BLE Address Type (überschreibt ENV/Settings, Standard: public)")
    p.add_argument("--tuya-id", default="", help="Tuya Device ID (optional)")
    p.add_argument("--tuya-key", default="", help="Tuya LocalKey (optional)")
    p.add_argument("--et112-l2", default="", help="ET112 L2 Quelle (optional Hinweis)")
    p.add_argument("--et112-l3", default="", help="ET112 L3 Quelle (optional Hinweis)")
    p.add_argument("--hci", default="hci0", help="BLE-Adapter (z. B. hci0)")
    p.add_argument("--bt-interval", type=float, default=1.8, help="Mindest-Rundenintervall s")
    p.add_argument("--bt-backoff-max", type=float, default=15.0, help="Max. Backoff s bei Fehlern")

    # Testmodus
    p.add_argument("--testmode", choices=[
        "off", "night", "day", "day_plus_batt", "day_surplus", "gen", "custom"
    ], default="off")
    p.add_argument("--seed", type=int, default=0)
    # Custom/Test-Werte
    p.add_argument("--test-l1", type=float, default=None, help="W")
    p.add_argument("--test-l2", type=float, default=0.0, help="W")
    p.add_argument("--test-l3", type=float, default=0.0, help="W")
    p.add_argument("--test-pv-ac", type=float, default=None, help="W")
    p.add_argument("--test-pv-dc", type=float, default=0.0, help="W")
    p.add_argument("--test-gen", type=float, default=0.0, help="W")
    p.add_argument("--test-batt-p", type=float, default=None, help="W (+Laden / −Entladen)")
    p.add_argument("--test-batt-v", type=float, default=52.0, help="V")
    p.add_argument("--test-batt-i", type=float, default=0.0, help="A")
    p.add_argument("--test-batt-soc", type=float, default=75.0, help="%" )
    return p


def init_services(args, settings: SettingsStore, dry: bool):
    prefix = args.service_prefix or ""
    inverter = InverterOutbackService(
        name=f"{prefix}com.victronenergy.inverter.outback_l1",
        device_instance=args.di_inverter,
        fw=VERSION, dry=dry, power_limit=args.l1_limit
    )
    pvinv = PVInverterService(
        name=f"{prefix}com.victronenergy.pvinverter.outback_l1",
        device_instance=args.di_pvinverter,
        fw=VERSION, dry=dry, power_limit=args.l1_limit
    )
    grid = GridGeneratorService(
        name=f"{prefix}com.victronenergy.grid.generator_tuya",
        device_instance=args.di_grid,
        fw=VERSION, dry=dry, power_limit=args.l1_limit
    )
    l2 = AcMeterService(
        name=f"{prefix}com.victronenergy.acmeter.et112_l2",
        device_instance=args.di_l2,
        phase="L2", fw=VERSION, dry=dry, power_limit=args.l2_limit
    )
    l3 = AcMeterService(
        name=f"{prefix}com.victronenergy.acmeter.et112_l3",
        device_instance=args.di_l3,
        phase="L3", fw=VERSION, dry=dry, power_limit=args.l3_limit
    )
    # TestMode-Flag für Sichtbarkeit
    test_mode = 1 if settings.get("/Settings/Devices/OutbackSPC/TestMode", 0) else 0
    for s in (inverter, pvinv, grid, l2, l3):
        s.set_test_mode(test_mode)
    return inverter, pvinv, grid, l2, l3


def midnight_changed(last_ymd: str) -> (bool, str):
    now_ymd = date.today().isoformat()
    return (last_ymd != now_ymd), now_ymd



def graceful_exit(signum, frame):
    global RUN
    RUN = False

# ──────────────────────────────────────────────────────────────
# Autodetect-Helfer: bluetoothctl Wrapper + Parser
# ──────────────────────────────────────────────────────────────

def _btctl(cmd: str, timeout: int = 8) -> str:
    """bluetoothctl-Einzelbefehl ausführen und stdout liefern (robust, mit Timeout)."""
    try:
        out = subprocess.check_output(
            ["bash", "-lc", f"bluetoothctl --timeout {int(timeout)} {shlex.quote(cmd)}"],
            stderr=subprocess.STDOUT,
            timeout=timeout + 2,
        )
        return out.decode("utf-8", errors="ignore")
    except subprocess.CalledProcessError as e:
        return e.output.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _bt_list_devices() -> Dict[str, str]:
    """Parst `bluetoothctl devices` → {MAC: NAME}."""
    out = _btctl("devices", timeout=5)
    res: Dict[str, str] = {}
    for line in out.splitlines():
        m = re.match(r"^Device\s+([0-9A-F:]{17})\s+(.+)$", line.strip())
        if m:
            res[m.group(1)] = m.group(2).strip()
    return res


def _bt_info(mac: str) -> Dict[str, str]:
    """Parst `bluetoothctl info <MAC>` in ein Dict."""
    out = _btctl(f"info {mac}", timeout=5)
    info: Dict[str, str] = {}
    for line in out.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            info[k.strip()] = v.strip()
    return info


# Helper: Pairing mit PIN via bluetoothctl
def _bt_pair_with_pin(mac: str, pin: str, log) -> bool:
    """
    Versucht, via bluetoothctl mit PIN zu pairen.
    Ablauf:
      1) agent KeyboardOnly
      2) default-agent
      3) pair <MAC>
      4) PIN an stdin übergeben
    Erfolgsprüfung über bluetoothctl-Ausgabe und anschließendes `info`.
    """
    try:
        # Agent initialisieren
        _btctl("agent KeyboardOnly", timeout=5)
        _btctl("default-agent", timeout=5)

        # Interaktiv pairen und PIN einspeisen
        p = subprocess.Popen(["bluetoothctl"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            cmds = f"pair {mac}\n{pin}\n"
            stdout, stderr = p.communicate(input=cmds, timeout=25)
            log.debug(f"pair(pin): stdout={stdout.strip()} stderr={stderr.strip()}")
        except subprocess.TimeoutExpired:
            p.kill()
            log.warning(f"autodetect: Pairing Timeout für {mac}")
        except Exception as e:
            try: p.kill()
            except Exception: pass
            log.warning(f"autodetect: Pairing-Fehler für {mac}: {e}")

        # Erfolg via info prüfen
        info = _bt_info(mac)
        paired = info.get("Paired", "no").lower() == "yes"
        if paired:
            log.info(f"autodetect: Pair mit PIN erfolgreich für {mac}")
            return True
        return False
    except Exception as e:
        log.warning(f"autodetect: Pairing-Exception für {mac}: {e}")
        return False


def autodetect_outback_mac(log, target_name: str = "ID55355535553555") -> Dict[str, str]:
    """Sucht Outback anhand des Anzeigenamens, führt trust/pair aus (idempotent),
    und liefert {mac, addrtype, paired, trusted, source}."""
    log.info(f"autodetect: suche Gerät mit Name '{target_name}' …")

    devs = _bt_list_devices()
    mac = next((m for m, n in devs.items() if n.strip() == target_name), None)

    if not mac:
        log.debug("autodetect: kein Treffer in 'devices' – starte Scan (8s)")
        _btctl("scan on", timeout=8)
        _btctl("scan off", timeout=3)
        devs = _bt_list_devices()
        mac = next((m for m, n in devs.items() if n.strip() == target_name), None)

    if not mac:
        log.warning("autodetect: kein passendes Gerät gefunden")
        return {"mac": "", "addrtype": "public", "paired": False, "trusted": False, "source": "none"}

    log.info(f"autodetect: gefunden mac={mac}")
    info = _bt_info(mac)
    paired = info.get("Paired", "no").lower() == "yes"
    trusted = info.get("Trusted", "no").lower() == "yes"
    addr_raw = info.get("Address Type", "public")
    addrtype = "random" if addr_raw.lower().startswith("random") else "public"

    if not trusted:
        log.info(f"autodetect: setze trust für {mac}")
        _btctl(f"trust {mac}", timeout=5)
        info = _bt_info(mac)
        trusted = info.get("Trusted", "no").lower() == "yes"

    if not paired:
        # PIN aus ENV oder Default 123456
        pin = os.getenv("OUTBACK_BLE_PAIRPIN", "123456")
        log.info(f"autodetect: versuche pair mit PIN für {mac} (PIN={'*'*len(pin)})")
        ok = _bt_pair_with_pin(mac, pin, log)
        if not ok:
            log.warning("autodetect: Pairing mit PIN nicht bestätigt – versuche einmal ohne PIN")
            _btctl(f"pair {mac}", timeout=12)
        info = _bt_info(mac)
        paired = info.get("Paired", "no").lower() == "yes"
        if not paired:
            log.warning("autodetect: Pairing nicht erfolgreich (ggf. Gerät/Display bestätigen)")

    log.info(f"autodetect: status paired={paired} trusted={trusted} addrType={addrtype}")
    return {"mac": mac, "addrtype": addrtype, "paired": paired, "trusted": trusted, "source": "scan"}


def main():
    parser = setup_argparser()
    args = parser.parse_args()

    # Signale
    signal.signal(signal.SIGTERM, graceful_exit)
    signal.signal(signal.SIGINT, graceful_exit)

    # Persistenz
    state = load_state()

    # Settings (Stub oder echtes com.victronenergy.settings)
    settings = SettingsStore(state_ref=state)
    settings.ensure_defaults({
        "/Settings/Devices/OutbackSPC/TestMode": 1 if args.testmode != "off" else 0,
        "/Settings/Test/L1": args.test_l1 if args.test_l1 is not None else 400.0,
        "/Settings/Test/L2": args.test_l2,
        "/Settings/Test/L3": args.test_l3,
        "/Settings/Test/PV_AC": args.test_pv_ac if args.test_pv_ac is not None else 300.0,
        "/Settings/Test/PV_DC": args.test_pv_dc,
        "/Settings/Test/GenPower": args.test_gen,
        "/Settings/Test/Battery/Voltage": args.test_batt_v,
        "/Settings/Test/Battery/Current": args.test_batt_i,
        "/Settings/Test/Battery/Power": args.test_batt_p if args.test_batt_p is not None else 0.0,
        "/Settings/Test/Battery/Soc": args.test_batt_soc,
        "/Settings/Test/Battery/Override": 0,
        "/Settings/Log/Core": "DEBUG" if args.debug else "INFO",
        "/Settings/Log/Outback": "INFO",
        "/Settings/Log/Battery": "INFO",
        "/Settings/Log/PV": "INFO",
        "/Settings/Log/Gen": "INFO",
        "/Settings/Log/ET112": "INFO",
        "/Settings/Log/TestMode": "INFO",
        "/Settings/Log/RateLimitMs": 500,
        "/Settings/Log/SummaryPeriodSec": args.summary_period,
        "/Settings/Devices/OutbackSPC/BLE/Mac": "",
        "/Settings/Devices/OutbackSPC/BLE/AddrType": "public",
    })

    # Logger
    rl_ms = int(settings.get("/Settings/Log/RateLimitMs", 500))
    log_core = make_logger("CORE", settings.get("/Settings/Log/Core", "INFO"), args.log_format, rl_ms)
    log_inv = make_logger("INV", settings.get("/Settings/Log/Outback", "INFO"), args.log_format, rl_ms)
    log_bat = make_logger("BATT", settings.get("/Settings/Log/Battery", "INFO"), args.log_format, rl_ms)
    log_pv  = make_logger("PV", settings.get("/Settings/Log/PV", "INFO"), args.log_format, rl_ms)
    log_gen = make_logger("GEN", settings.get("/Settings/Log/Gen", "INFO"), args.log_format, rl_ms)
    log_et  = make_logger("ET112", settings.get("/Settings/Log/ET112", "INFO"), args.log_format, rl_ms)
    log_tst = make_logger("TEST", settings.get("/Settings/Log/TestMode", "INFO"), args.log_format, rl_ms)

    # Dienste
    dry = args.dry_run or (not is_real_dbus())
    inverter, pvinv, grid, l2, l3 = init_services(args, settings, dry)

    # Quellen
    testmode = TestMode(settings=settings, seed=args.seed, scenario=args.testmode)
    # BLE-MAC & AddrType resolvieren (Priorität: CLI > Settings > ENV)
    mac_cli = (args.ble_mac or "").strip()
    mac_cfg = (settings.get("/Settings/Devices/OutbackSPC/BLE/Mac", "") or "").strip()
    mac_env = (os.getenv("OUTBACK_BLE_MAC", "") or "").strip()
    mac_resolved = mac_cli or mac_cfg or mac_env  # leer → Client versucht Legacy utils

    addr_cli = (args.ble_addrtype or None)
    addr_cfg = (settings.get("/Settings/Devices/OutbackSPC/BLE/AddrType", "public") or "public").strip().lower()
    addr_env = (os.getenv("OUTBACK_BLE_ADDRTYPE", "") or "").strip().lower()
    addr_resolved = (addr_cli or addr_env or addr_cfg)
    if addr_resolved not in ("public", "random"):
        addr_resolved = "public"

    # Falls CLI-MAC gesetzt wurde und sich von Settings unterscheidet → in Settings spiegeln
    if mac_cli and mac_cli != mac_cfg:
        settings.set("/Settings/Devices/OutbackSPC/BLE/Mac", mac_cli)
    if addr_cli and addr_cli != addr_cfg:
        settings.set("/Settings/Devices/OutbackSPC/BLE/AddrType", addr_cli)

    # Wenn keine MAC vorhanden → Autodetect versuchen
    autodetect_used = False
    if not mac_resolved:
        log_core.info("startup: keine BLE-MAC in CLI/Settings/ENV → Autodetect wird versucht …")
        ad = autodetect_outback_mac(log_core)
        if ad.get("mac"):
            mac_resolved = ad["mac"]
            autodetect_used = True
            # AddrType ggf. aus Info übernehmen, wenn nicht explizit gesetzt
            if not args.ble_addrtype and not addr_env:
                addr_resolved = ad.get("addrtype", addr_resolved)
            # Persistieren in Settings
            settings.set("/Settings/Devices/OutbackSPC/BLE/Mac", mac_resolved)
            settings.set("/Settings/Devices/OutbackSPC/BLE/AddrType", addr_resolved)
            log_core.info(f"autodetect: MAC in Settings gespeichert ({mac_resolved}, addrType={addr_resolved})")
        else:
            log_core.warning("autodetect: kein Gerät gefunden – weiter ohne BLE (L1 bleibt 0)")

    # BLE-Client anlegen (mac darf leer sein → Client versucht ENV/utils)
    os.environ.setdefault("OUTBACK_BLE_ADDRTYPE", addr_resolved)
    if mac_resolved:
        os.environ.setdefault("OUTBACK_BLE_MAC", mac_resolved)

    ble = BleOutbackClient(mac=mac_resolved, hci=args.hci,
                           min_interval_s=args.bt_interval,
                           backoff_max_s=args.bt_backoff_max,
                           debug=args.debug)
    src = ("CLI" if mac_cli else ("SETTINGS" if mac_cfg else ("ENV" if mac_env else ("SCAN" if 'autodetect_used' in locals() and autodetect_used and mac_resolved else "AUTO"))))
    try:
        s0 = ble.get_status() if hasattr(ble, "get_status") else {}
    except Exception:
        s0 = {}
    log_core.info(
        f"startup: BLE mac={s0.get('mac', mac_resolved or '?')} (src={src}) "
        f"(hci={s0.get('hci', args.hci)}) "
        f"backend={s0.get('backend','?')}/{s0.get('addr_type', addr_resolved)} "
        f"debug={str(bool(args.debug)).lower()}"
    )
    batt_reader = BatteryDbusReader()
    tuya = TuyaClient(dev_id=args.tuya_id, local_key=args.tuya_key)
    et_l2 = Et112Reader(source_hint=args.et112_l2)
    et_l3 = Et112Reader(source_hint=args.et112_l3)

    # EMA-Glätter
    ema_p_l1 = EMA(alpha=0.3)
    ema_p_pv = EMA(alpha=0.3)
    ema_p_gen = EMA(alpha=0.3)
    ema_p_l2 = EMA(alpha=0.3)
    ema_p_l3 = EMA(alpha=0.3)

    # Generator-Hysterese
    gen_thr_on = 200.0  # W Einschalt-Schwelle (Tuya-Leistung)
    gen_thr_off = gen_thr_on - 100.0  # 100 W Hysterese
    gen_min_runtime_s = 7.0
    gen_running = False
    gen_last_change = 0.0

    # Forward-Zähler Initialisierung
    pv_forward_kwh = float(state.get("pv_forward_kwh", 0.0))
    l2_forward_kwh = float(state.get("l2_forward_kwh", 0.0))
    l3_forward_kwh = float(state.get("l3_forward_kwh", 0.0))
    last_reset_ymd = state.get("last_reset_ymd", "")
    changed, now_ymd = midnight_changed(last_reset_ymd)
    if changed:
        pv_forward_kwh = 0.0
        last_reset_ymd = now_ymd

    # Summenlogger
    summary = Summary(period_s=int(settings.get("/Settings/Log/SummaryPeriodSec", 5)))
    # BLE-Status CORE-DEBUG Ticker (~5s)
    ble_dbg_last = 0.0
    ble_dbg_period = 5.0

    # Einmal Dump?
    if args.dump_now:
        log_core.info("dump-now: pv_forward_kwh=%.3f l2=%.3f l3=%.3f" % (pv_forward_kwh, l2_forward_kwh, l3_forward_kwh))

    # Hauptschleife
    poll_interval = 1.0  # s
    t_prev = time.monotonic()

    while RUN:
        t_loop = time.monotonic()
        dt = max(0.001, t_loop - t_prev)
        t_prev = t_loop

        # === Messwerte beziehen ===
        if settings.get("/Settings/Devices/OutbackSPC/TestMode", 0):
            sim = testmode.step(dt)
            l1_power = sim["L1"]
            l2_power = sim["L2"]
            l3_power = sim["L3"]
            pv_ac = sim["PV_AC"]
            pv_dc = sim["PV_DC"]
            gen_power = sim["GEN"]
            batt_p = sim["BATT_P"]
            batt_v = sim["BATT_V"]
            batt_i = sim["BATT_I"]
            batt_soc = sim["BATT_SOC"]
            outback_state = sim["OUTBACK_STATE"]
            rssi = -50
            last_ble_update = int(time.time())
            log_tst.debug("sim values applied")
        else:
            # Outback via BLE Snapshot (A03 & A11) – Stub liefert None falls nicht verfügbar
            snap = ble.snapshot()
            if snap:
                l1_power = float(snap.get("power_w", 0.0))
                batt_p = float(snap.get("batt_power_w", 0.0)) if "batt_power_w" in snap else 0.0
                outback_state = int(snap.get("state", STATE_INVERT))
                rssi = int(snap.get("rssi", -70))
                last_ble_update = int(time.time())
            else:
                # Ohne BLE: Werte 0, State Invert, alles ruhig
                l1_power = 0.0
                outback_state = STATE_INVERT
                rssi = -99
                last_ble_update = 0

            # Batterie vom BMV-712 (DC-Wahrheit) via D-Bus bevorzugen; Fallback lokal
            b_live = batt_reader.read()
            if b_live is not None:
                batt_v, batt_i, batt_p, batt_soc = b_live["V"], b_live["I"], b_live["P"], b_live["SOC"]
            else:
                b = testmode.read_battery_live_fallback()
                batt_v, batt_i, batt_p, batt_soc = b["V"], b["I"], b["P"], b["SOC"]

            # L2/L3 von ET112 (optional). Stub = 0.
            l2_power = et_l2.read_power()
            l3_power = et_l3.read_power()

            # Generatorleistung via Tuya (optional)
            gen_power = tuya.read_power()
            pv_ac = compute_pv_ac(l1_power, batt_p)
            pv_dc = 0.0  # DC-PV ausschließlich externer Victron-MPPT, hier NICHT ableiten!

        # === BLE-Status (CORE DEBUG, alle ~5s) ===
        now_mono = time.monotonic()
        if (now_mono - ble_dbg_last) >= ble_dbg_period:
            try:
                s = ble.get_status() if hasattr(ble, "get_status") else {}
            except Exception:
                s = {}
            stat = s.get("status", "n/a")
            nxt = s.get("next_in_s", 0.0)
            okc = s.get("ok", 0)
            flc = s.get("fail", 0)
            cfc = s.get("consec_fails", 0)
            mac = s.get("mac", "?")
            hci = s.get("hci", "?")
            backend = s.get("backend", "?")
            addr_t  = s.get("addr_type", "?")
            log_core.debug(
                f"BLE[{stat}] mac={mac} hci={hci} backend={backend}/{addr_t} "
                f"next={nxt:.1f}s ok={okc} fail={flc} consec={cfc} rssi={rssi}"
            )
            ble_dbg_last = now_mono

        # === EMA-Glättung ===
        l1_power_s = ema_p_l1.update(l1_power)
        pv_ac_s = ema_p_pv.update(pv_ac)
        gen_power_s = ema_p_gen.update(gen_power)
        l2_power_s = ema_p_l2.update(l2_power)
        l3_power_s = ema_p_l3.update(l3_power)

        # === Generator AND-Logik mit Hysterese + Mindestlaufzeit ===
        now = time.monotonic()
        if outback_state == STATE_PASSTHROUGH and gen_power_s >= gen_thr_on:
            if not gen_running:
                gen_running = True
                gen_last_change = now
        elif gen_running and (gen_power_s <= gen_thr_off) and (now - gen_last_change >= gen_min_runtime_s):
            gen_running = False
            gen_last_change = now

        # === Systemzustand bestimmen ===
        sys_state = classify_state(
            pv_ac=pv_ac_s, l1_out=l1_power_s, batt_p=batt_p,
            outback_state=outback_state, gen_power=gen_power_s, eps=50.0
        )

        # === Forward-Zähler sekündlich integrieren + Tagesreset ===
        changed, now_ymd = midnight_changed(last_reset_ymd)
        if changed:
            pv_forward_kwh = 0.0
            last_reset_ymd = now_ymd
        pv_forward_kwh += max(0.0, pv_ac_s) / 3600.0 / 1000.0 * dt
        l2_forward_kwh += max(0.0, l2_power_s) / 3600.0 / 1000.0 * dt
        l3_forward_kwh += max(0.0, l3_power_s) / 3600.0 / 1000.0 * dt

        # === D‑Bus Services aktualisieren ===
        # Inverter L1
        inverter.update(
            voltage=clamp(230.0, 0, 300),  # Schätzwert
            current=l1_power_s / 230.0 if 230.0 > 0 else 0.0,
            power=l1_power_s,
            state=outback_state,
            last_ble_update=last_ble_update,
            rssi=rssi
        )
        # PV‑Inverter L1
        pvinv.update(power=pv_ac_s, forward_kwh=pv_forward_kwh)

        # Generator/Grid
        grid.update(
            voltage=230.0 if gen_running else 0.0,
            current=gen_power_s / 230.0 if gen_running else 0.0,
            power=gen_power_s if gen_running else 0.0,
            running=1 if gen_running else 0
        )

        # AC‑Meter L2/L3
        l2.update(power=l2_power_s, voltage=230.0 if l2_power_s > 0 else 0.0,
                  current=(l2_power_s / 230.0) if l2_power_s > 0 else 0.0,
                  forward_kwh=l2_forward_kwh)
        l3.update(power=l3_power_s, voltage=230.0 if l3_power_s > 0 else 0.0,
                  current=(l3_power_s / 230.0) if l3_power_s > 0 else 0.0,
                  forward_kwh=l3_forward_kwh)

        # === Logging (kurz & knapp) ===
        log_pv.info(f"l1_pv={int(round(pv_ac_s))}W → pvinverter:/Ac/L1/Power")
        log_inv.info(f"l1_out={int(round(l1_power_s))}W | batt={int(round(batt_p))}W | state={outback_state}")
        log_core.info(f"state: {sys_state} (pv={int(round(pv_ac_s))} l1={int(round(l1_power_s))} batt={int(round(batt_p))})")
        log_pv.debug(f"calc: p_pv_ac=clamp({int(round(l1_power))}-max(0,{int(round(-batt_p))}),0,{int(round(l1_power))})={int(round(pv_ac))}W")
        if args.balance_check:
            loads = l1_power_s + l2_power_s + l3_power_s
            sources = pv_ac_s + pv_dc + (gen_power_s if gen_running else 0.0) + max(0.0, -batt_p)
            diff = loads - sources
            log_core.debug(f"balance: loads={int(loads)} sources={int(sources)} diff={int(diff)}W")

        # Summenzeile
        if summary.due():
            summary.emit(
                f"L1={int(round(l1_power_s))} L2={int(round(l2_power_s))} L3={int(round(l3_power_s))} | "
                f"PV_ac={int(round(pv_ac_s))} PV_dc={int(round(pv_dc))} | GEN={int(round(gen_power_s if gen_running else 0.0))} | "
                f"BATT={int(round(batt_p))} (SOC={round(batt_soc,1)})"
            )

        # Persistenz sichern
        state["pv_forward_kwh"] = pv_forward_kwh
        state["l2_forward_kwh"] = l2_forward_kwh
        state["l3_forward_kwh"] = l3_forward_kwh
        state["last_reset_ymd"] = last_reset_ymd
        save_state(state)

        if args.once:
            break
        # 1‑Sekunden‑Takt
        t_sleep = poll_interval - (time.monotonic() - t_loop)
        if t_sleep > 0:
            time.sleep(t_sleep)

    log_core.info("Beendet.")


if __name__ == "__main__":
    main()

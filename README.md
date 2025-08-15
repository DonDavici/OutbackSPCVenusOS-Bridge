# Outback SPC → Venus OS Bridge

Modulare Python‑Lösung für Venus OS (Raspberry Pi), die die Inselanlage korrekt visualisiert:
- **L1** (Outback Inverter), **PV‑AC** (berechnet ohne Doppelzählung), **Generator (Tuya)**, **L2/L3** (ET112)  
- PV‑Forward‑Zähler mit Tages‑Reset, **niemals flappender PV‑Service**  
- Testmodus & CLI, sauberes Logging mit Summenzeilen

## Installation (GX mit SetupHelper)
```bash
# Auf dem GX:
scp -r outback-spc-venus root@venus:/data/
ssh root@venus
cd /data/outback-spc-venus
./setup install
```

## Starten/Stoppen
```bash
systemctl enable outback-venus && systemctl start outback-venus
journalctl -u outback-venus -f
```

## CLI‑Beispiele (Testmodus)
```bash
# Nacht:
python3 /data/outback_spc/outback_venus.py --dry-run --debug --testmode night --once
# PV + Batterie:
python3 /data/outback_spc/outback_venus.py --dry-run -d --testmode day_plus_batt --test-l1 900 --test-pv-ac 620 --test-batt-p -280 --once
# Generator:
python3 /data/outback_spc/outback_venus.py --dry-run -d --testmode gen --test-gen 1200 --test-pv-ac 500 --test-l1 1600 --once
```

## Troubleshooting
- Keine D‑Bus‑Dienste sichtbar? `--dry-run` entfernen, Service per `systemctl` starten.  
- PV‑AC bleibt 0? Formel nutzt **Batterie‑Power** (BMV). Im Testmodus ggf. Override prüfen.  
- Zähler zurücksetzen: `/data/outback_spc/state.json` löschen (nur wenn Service gestoppt).

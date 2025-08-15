#!/usr/bin/env python3
import os, asyncio, sys

MAC = sys.argv[1] if len(sys.argv) > 1 else "B0:7E:11:F9:BC:F2"

async def bleak_probe():
    from bleak import BleakClient
    print("[Bleak] Connecting ...")
    async with BleakClient(MAC, timeout=4.0) as cli:
        print("[Bleak] Connected =", await cli.is_connected())
        svcs = await cli.get_services()
        for s in svcs:
            print(f"[Bleak] Service {s.uuid}")
            for c in s.characteristics:
                props = ",".join(c.properties)
                print(f"  Char {c.uuid}  props={props}")
                if "read" in c.properties:
                    try:
                        data = await asyncio.wait_for(cli.read_gatt_char(c.uuid), timeout=1.5)
                        print(f"    read {len(data)} bytes")
                    except Exception as e:
                        print(f"    read FAIL: {e}")

def bluepy_probe(addrtype="public"):
    from bluepy.btle import Peripheral, BTLEException, UUID
    print(f"[bluepy] Connecting addrType={addrtype} ...")
    p = Peripheral(MAC, addrType=addrtype)
    try:
        for s in p.getServices():
            print(f"[bluepy] Service {s.uuid}")
            try:
                for ch in s.getCharacteristics():
                    print(f"  Char {ch.uuid} handle={ch.getHandle()}")
                    try:
                        data = ch.read()
                        print(f"    read {len(data)} bytes")
                    except Exception as e:
                        print(f"    read FAIL: {e}")
            except Exception as e:
                print(f"  list chars FAIL: {e}")
    finally:
        p.disconnect()

if __name__ == "__main__":
    print("=== Probe on", MAC, "===")
    # 1) Bleak
    try:
        asyncio.run(bleak_probe())
    except Exception as e:
        print("[Bleak] FAIL:", e)
    # 2) bluepy random
    try:
        bluepy_probe("random")
    except Exception as e:
        print("[bluepy random] FAIL:", e)
    # 3) bluepy public
    try:
        bluepy_probe("public")
    except Exception as e:
        print("[bluepy public] FAIL:", e)
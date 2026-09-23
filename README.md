# Inverter Simulator

SMA inverter + POI meter simulator for HIL testing of a Hybrid Controller (HYC),
over Modbus TCP/UDP, with a browser GUI. Runs headless on a Raspberry Pi.

## Requirements

Python 3.9+ and **pymodbus 3.6.9** — the version is pinned. The script uses the
3.6.x datastore/server API and will not start on 3.9+ or 4.x.

```bash
python3 -m venv ~/modbus-env
~/modbus-env/bin/pip install 'pymodbus==3.6.9'
```

## Run

```bash
sudo ~/modbus-env/bin/python inverter_simulator.py \
    --inverters 2 --base-port 502 --meter-port 600 --host 0.0.0.0
```

`sudo` is needed for ports below 1024. GUI on `http://<pi>:8080`.

Useful flags: `--name <owner>` labels the instance in the GUI, `--config <path>`
picks the saved-rig file, `--no-restore` ignores it.

## Multi-Pi mode

For HYC firmware that can only be given an inverter's IP and always uses port
502, run one Pi per inverter and set the roles in the **Multi-Pi mode** card:

- satellites: click **satellite** (their POI meter stops)
- master: click **master**, paste the satellite IPs, **Apply list**

Then **Save config** on each Pi so the roles survive a reboot. Point the HYC's
inverter entries at each Pi on port 502, and its meter entry at the master.

## Updating a Pi

```bash
cd ~/inverter-sim && git pull
```

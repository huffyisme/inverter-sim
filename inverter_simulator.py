#!/usr/bin/env python3
"""
Inverter_simulatorV1.py - SMA Inverter Simulator for HIL testing of an
SMA Hybrid Controller (HYC).

  * A CONFIGURABLE FLEET of simulated inverters (SMA "Sunny Central Kodiak"
    profile, unit id 102). Add/remove them live from the GUI; each is a Modbus
    TCP server on its own port (one IP, ports allocated from --base-port up).
    Two kinds:
      - PV   (kind="pv",   default): generation only, profile rev 1135,
             battery channels answer -1/unsupported (as measured on the
             real 4.2 MW PV unit).
      - BESS (kind="bess"): battery inverter with a live SOC model
             (charge = negative WSpt, discharge = positive). Register image
             and scaling taken from a wire capture of a REAL battery Kodiak
             (tcpdump 2026-07, SN 3022609601): profile rev 1035, PLAIN kW
             scaling (no x36!), WSptMin/Max = -/+VA rating, live
             Bat.SOCConn / Bsc.WhInAvail / Bsc.WhOutAvail / Bsc.WInMax /
             Bsc.WOutMax / Bsc.VArOxMax / Bsc.VArUxMax, Dcs.* group all 0
             (SOC lives in Bat.SOCConn on this unit, NOT Dcs.TotSoc),
             Bsc.SrcSel=21410, Bsc.InvStrMod=1438, BatWMin/MaxMod=3,
             OpStt 1392 when stopped. Honors HYC writes to
             AuxCtl.SOCOpMax/Min (HR 1400-block, observed on the wire) and
             BatWSptMax/Min (HR 1504/1506 or 1530/1532). Spawn with --bess N
             or the "+ Add BESS" GUI button; SOC and usable capacity (kWh)
             are editable in the selected-inverter panel.
  * 1 simulated meter (SMA "Power Analyzer" POI block) on its own port, whose
    readings are the LIVE AGGREGATE of the switched-on inverters passed through
    a configurable grid model (Thevenin voltage rise + collector/transformer
    losses).
  * A built-in web GUI (Python standard library only). Full per-inverter control
    (cap, manual P/Q, rating, serial, port, ramp on/off + rates, noise on/off +
    amplitudes, tracking) via a "selected inverter" panel, plus plant-wide and
    grid-model controls. Designed to run headless on a Raspberry Pi.

Only dependency:  pip install 'pymodbus==3.6.9'

Run (venv recommended on Raspberry Pi OS):
    python3 -m venv .venv
    .venv/bin/pip install 'pymodbus==3.6.9'
    .venv/bin/python Inverter_simulatorV1.py --inverters 3 --base-port 1502 --meter-port 1600
    # browse to http://<pi-ip>:8080
(ports < 1024 need root: sudo .venv/bin/python Inverter_simulatorV1.py ...)

--inverters N just sets how many to spawn AT STARTUP; add/remove more anytime
in the GUI.

HYC setpoint addresses: against a real Kodiak (wire capture) the HYC writes
the PROFILE setpoint block Holding 1500-1512 in one FC16 request over UDP
(~40 ms, fire-and-forget - the inverter never answers UDP writes), and
block-reads HR 1200 x54 / IR 1000 x116 over TCP (~220 ms). If the device
reports Modbusd.PPC.Prf.Rev = 0 the HYC falls back to writing the raw
uniqueid addresses 40023 (WSpt), 40022 (VArSpt), 40018 (FstStop). The sim
reports Prf.Rev=1035 / Dev.Id=102 and accepts BOTH conventions - per signal,
the most recently written source wins. UDP write replies are suppressed by
default to match the real hardware (toggle in the GUI).

ERROR ACKNOWLEDGEMENT FROM THE CONTROLLER: the PPC/HYC profile carries exactly
one error-clear channel - ErrClr @ Holding 1544 (U16, uniqueid 8275, named
Dcs.DcDcErrClr in ppc_profile_7_3.ppc), in the "write holding registers, only
to be sent for changes" group. When the operator acknowledges an inverter
error on the controller, that register is written non-zero. The sim treats
any HYC write of a non-zero value to 1544 (or, as a tolerance, to the raw
uniqueid address 8275) as ErrClr: it clears the active ErrNo, re-arms the
trigger back to 0 like a real one-shot channel, and - if the error was a red
fault - sends the unit back through the connect sequence. Previously the only
way to clear an error was the GUI's own ErrClr button, so an acknowledgement
from the controller had no effect.
Every HYC write that lands OUTSIDE the cyclic setpoint block (1500-1520) is
also logged to the console as "[hyc-write] ...", so if a controller firmware
uses a different register for the acknowledgement it shows up in one test run.
"""
import argparse
import asyncio
import concurrent.futures
import io
import json
import logging
import math
import os
import random
import socket
import threading
import time
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.request
from urllib.parse import urlparse, parse_qs

from pymodbus.datastore import (
    ModbusServerContext,
    ModbusSlaveContext,
    ModbusSequentialDataBlock,
    ModbusSparseDataBlock,
)
from pymodbus.server import ModbusTcpServer, ModbusUdpServer

# pymodbus floods the console with a context-free
#   "Exception response Exception Response(144, 16, IllegalAddress)"
# for every rejected request (pymodbus/pdu.py doException -> Log.error).
# Silence it - StrictSlaveContext below logs its own line WITH the device,
# function code, address and count, which is actually debuggable.
logging.getLogger("pymodbus").setLevel(logging.CRITICAL)

SQRT3 = math.sqrt(3.0)

# ==========================================================================
# INVERTER register map (Sunny Central Kodiak - family, unit id 102)
# Only the 1000-range HYC group is SEEDED - the HYC sources all data there.
# The profile's mirror blocks (legacy 0-80, PPC ratings 0-14, 2000-range)
# stay unseeded: the PermissiveBlock answers reads there with 0 (never an
# error), and the monitor still names them if the HYC ever polls them.
# ==========================================================================
H_SERNO, H_OPSTT, H_KEYSW, H_ERRNO = 1000, 1004, 1006, 1008
H_DCWATT, H_W, H_VAR, H_VA = 1010, 1012, 1014, 1016
H_SOC, H_SOH = 1020, 1021
H_VOLT_AB, H_VOLT_BC, H_VOLT_CA, H_FREQ = 1024, 1026, 1028, 1030
H_DCVOLT, H_DCAMP = 1032, 1034
H_TOTOPTM, H_ACWHOUT_DAY, H_ACWHOUT_TOT = 1036, 1038, 1040
# AC/DC conversion efficiency (sim only): lets DC power slightly exceed AC
# while generating - and fall short of it while charging - so DcMs.TotWatt
# and DcMs.TotAmp are not simply a copy of the AC side.
INV_ETA = 0.985
H_WAVAL_P, H_VARAVL, H_WINMAX, H_WOUTMAX = 1062, 1064, 1066, 1070
H_VAROXMAX, H_VARUXMAX, H_VARAVL2, H_ERRSTT = 1074, 1078, 1080, 1095
H_WAVAL2, H_WSPTMIN, H_WSPTMAX = 1101, 1112, 1114
# NOTE: the legacy PPC-group ratings mirror (Holding 4/10/14, same uniqueids
# 318/319/323 as 1208/1210/1250) is intentionally NOT seeded anymore - the
# HYC only reads the 1200-block. The PermissiveBlock still answers reads
# there with 0, so a legacy SCADA poll never gets a Modbus exception.
HR_INVOPMOD, HR_REMRDY, HR_WRTG, HR_VARRTG, HR_VARTG = 1200, 1202, 1208, 1210, 1250
W_VARSPT, W_WSPT, W_FSTSTOP = 40022, 40023, 40018  # legacy/uniqueid raw addrs
# profile setpoint block: the HYC writes 1500-1512 in ONE FC16 request
# (VArSpt, WSpt, WSptMin, BatWSptMax, BatWSptMin, FstStop, HzNomSpt,
# VolNomSpt) when the device reports a valid Modbusd.PPC.Prf.Rev. If the
# revision reads 0/unknown it falls back to the raw uniqueid addresses
# above - the sim supports BOTH, most recent write wins per signal.
P_VARSPT, P_WSPT, P_FSTSTOP = 1500, 1501, 1508
P_FSTSTOP_B = 1534          # FstStop copy in the HYBRID "changes only" group
# ErrClr: the profile's only error-acknowledge channel (Holding 1544, U16,
# uniqueid 8275, "Dcs.DcDcErrClr" in the .ppc). It sits in the HYC's
# "write ... only to be sent for changes" group: the controller pokes it when
# the operator acknowledges an inverter error. W_ERRCLR is the raw uniqueid
# address, accepted as a tolerance the same way the setpoints are.
P_ERRCLR, W_ERRCLR = 1544, 8275
# --------------------------------------------------------------------------
# ErrClr in the *SMA Modbus profile* (MODBUS-SCxxx-TI-en-19 section 5.3.1,
# valid for SC-2200/2475/2500-EV/4xxx UP). This is the profile the HYC uses
# for the "Acknowledge inverter error" button - NOT the PPC/HYC profile:
#     Unit ID 3, Holding 8  = ErrClr        (uid 733)  973 '---' | 26 'Ackn'
#     Unit ID 3, Holding 20 = ErrClr.ProErr (uid 7211) 973 '---' | 21118 Gfdi,
#                             28 Aid, 2490 Pld, 21119 IsoBender, 21120 Frq
# The sim answers its real data on unit 102 and keeps 1/2/3 as scratch alias
# units, so an acknowledge used to land in the scratch block and vanish.
SMA_ERRCLR_HR, SMA_ERRCLR_PROERR_HR = 8, 20
SMA_ERRCLR_ACK, SMA_ENUM_IDLE = 26, 973
# SMA Modbus profile holding parameters (unit id 3) - named so a write to an
# alias unit is identifiable in the console instead of "HR@8 x2"
SMA_HR_NAMES = {a: (n, 2, "s32") for a, n in (
    ( 0, "InvOpMod"), ( 2, "RemRdy"), ( 4, "GriMng.VArMod"),
    ( 6, "GriMng.WMod"), ( 8, "ErrClr"), (10, "VADrtPriMod"),
    (12, "WGraMod"), (14, "WGra"), (16, "VArGraMod"), (18, "VArGra"),
    (20, "ErrClr.ProErr"), (22, "GriMng.InvVArMod"), (26, "Bfp.Ena"),
    (28, "GriMng.VolNomMod"), (30, "InstFunc"), (34, "PvGnd.OpnRemGfdi"),
    (36, "StbySfCapacMod"), (38, "Aid.Mod"))}
PRF_REV, DEV_ID = 1135, 287  # identity measured on the REAL unit 2026-07-16:
#   Modbusd.PPC.Prf.Rev = 1135 (fw newer than .ppc v1035; matches ppc v1135)
#   Modbusd.Dev.Id      = 287  (SC30COM susyid - NOT the Modbus unit id 102!)
# registers the HYC WRITES (commands/setpoints) - everything else is read-only data
INV_HYC_WRITE_ADDRS = frozenset(
    range(W_FSTSTOP, W_FSTSTOP + 2)) | frozenset((W_VARSPT, W_WSPT)) | \
    frozenset(range(1500, 1521))

P2G_IR_2000_IMAGE = (  # fc4@2000 x116 census image of the real unit
    45962,48732,0,9461,0,1392,0,308,0,8713,0,311,
    0,0,0,0,0,0,0,6254,0,6247,0,0,
    0,6484,0,6481,0,6487,0,5000,0,12501,65535,65515,
    76,38647,0,189,0,35601,0,311,0,311,0,311,
    65535,65535,65535,65535,65535,65535,65535,65535,65535,65535,65535,65535,
    65535,65535,65535,65535,65535,65535,65535,65535,65535,65535,65535,65535,
    65535,65535,65535,65535,65535,65535,65535,65535,0,0,0,311,
    0,311,0,311,0,2,0,11401,65535,65535,1,0,
    1392,0,311,0,0,0,0,65535,65535,65535,1525,57600,
    0,21605,65535,65535,0,0,2,20128)

OPSTT_GRIDFEED, OPSTT_STOP, ERRSTT_OK, FSTSTOP_FULLSTOP = 3526, 381, 307, 1749
FSTSTOP_RUN = 1467          # "run permitted" - the idle value of FstStop
OPSTT_GRIDFORM = 21429      # OpStt while running in a grid-forming mode
# AuxCtl.SCSOpCmd (HR 1400, U32, uid 3960) -> the OpStt the unit reports while
# it is running. ONLY the rows confirmed by SMA are mapped here:
#   21416 Power Control                      -> 3526  GridFeed
#   21521 / 22321 / 22322 / 22323 GFM modes  -> 21429 Gridform
#   381   Stop                               -> unit stops
# The remaining enum members (2291 Battery Standby, 21415 Inverter Standby,
# 21417 DC Voltage Control, 21418 DC Current Control, 21454 QonDemand) are
# deliberately NOT mapped - their OpStt result has not been confirmed, so the
# state machine keeps control and the value is logged instead of guessed.
SCS_STOP = 381
SCS_TO_OPSTT = {21416: OPSTT_GRIDFEED, 21521: OPSTT_GRIDFORM,
                22321: OPSTT_GRIDFORM, 22322: OPSTT_GRIDFORM,
                22323: OPSTT_GRIDFORM}

# --------------------------------------------------------------------------
# BESS (battery Kodiak) identity & register image - measured from a wire
# capture of a live battery unit (tcpdump 2026-07, SN 3022609601, HYC
# polling fc3@1200x54 / fc4@1000x116, writing fc16@1244 and fc16@1400):
#   * Modbusd.PPC.Prf.Rev = 1035 (HR 1228) - NOT 1135. This profile serves
#     kW/kVAr/kVA channels PLAIN (no x36 pre-multiplication): the unit
#     answered WRtg=2680, VArRtg=2070, VARtg=3450, WSptMin/Max=-3450/+3450,
#     InvMs.TotW plain kW with the 20 ms mirrors (1042/1044) FIX3 (x1000).
#   * OpStt = 3526 while feeding, 1392 when stopped (PV unit stops at 381).
#   * SOC is served in Bat.SOCConn (x0.1 %) - Dcs.TotSoc stays 0 (the
#     whole Dcs.* group is 0 / OpMod 381 on this unit: no DC-DC sections).
#   * Bat.TmpAvg/Min/Max answer 0 (not -1 like the PV unit).
#   * On shutdown Bsc.WInMax/WOutMax/VArOx/UxMax and WAval collapse to 0.
BESS_PRF_REV = 1035
OPSTT_STOP_BESS = 1392
BESS_SRCSEL = 21410          # HR 1230 Bsc.SrcSel (capture)
BESS_INVSTRMOD = 1438        # HR 1244 Bsc.InvStrMod idle enum (HYC re-writes it)
BESS_BATWMOD = 3             # HR 1246/1248 GriMng.BatWMinMod/BatWMaxMod
BESS_RISO_WARN, BESS_RISO_ERR = 3500, 1500   # HR 1206/1207 (capture)
# rating ratios measured on the 2680 kW unit: VArRtg 2070, VARtg 3450
BESS_VAR_RATIO, BESS_VA_RATIO = 2070.0 / 2680.0, 3450.0 / 2680.0
BESS_ETA = 0.96              # one-way charge/discharge efficiency (sim only)
H_BATTMP_AVG, H_BATTMP_MIN, H_BATTMP_MAX = 1046, 1048, 1050
H_SOCCONN, H_WHINAVAIL, H_WHOUTAVAIL = 1056, 1058, 1060
H_ACWHIN = 1088              # Cnt.TotAcWhIn (x0.01 MWh)
HR_AUXCTL_CMD, HR_SOCOPMAX, HR_SOCOPMIN = 1400, 1402, 1404
HR_BATWMAX_A, HR_BATWMIN_A = 1504, 1506    # BatWSptMax/Min (%) - 1500 group
HR_BATWMAX_B, HR_BATWMIN_B = 1530, 1532    # BatWSptMax/Min (%) - 1530 group
# ErrorStt / ErrStt is uniqueid 328, and BOTH SMA specs agree its enum has
# exactly three legal values: 973 '---', 307 'Ok', 1392 'Error'
#   - ppc_profile_7_3.ppc  <mapping uniqueid="328">
#   - MODBUS-SCxxx-TI-en-19 section 5.3.2, input register 94 (ErrStt)
# The generic taglist ids 35 'Fault' / 455 'Warning' are NOT members of this
# channel's enum, so a master decoding uid 328 could not map them. Report
# 1392 for both severities; the fault/warning distinction is kept internally
# (err_sev) for the GUI and for the ErrNo the master actually reads.
ERRSTT_ERROR = 1392
ERRSTT_FAULT = ERRSTT_WARN = ERRSTT_ERROR

# --------------------------------------------------------------------------
# STATIC register image of a REAL Sunny Central Kodiak, extracted from a
# wire capture of the HYC polling a live 4.2 MW unit (568 block responses,
# tcpdump 2026-06). Key insight: unsupported/optional channels answer
# -1 (0xFFFFFFFF), NOT 0 - and 0 is not a valid SMA enum, so a block full
# of zeros fails the HYC's validation and it drops to per-channel polling.
# Values here are the constant (non-measurement) words; measurements,
# ratings, serial, status and error registers stay simulated.
#   addr -> (value, kind)  kind: 's32'/'u32' pair, 'u16' single
# --------------------------------------------------------------------------
REAL_IR_STATIC = {
    1002: (9461, "s32"),        # HaNsDampF (enum)
    1018: (303, "u32"),         # License.Inertia (303 = Off)
    1020: (0, "u16"),           # Dcs.TotSoc (PV unit - no battery)
    1021: (0, "u16"),           # Dcs.TotSoh
    1022: (-1, "s32"),          # InvType (commented out of profile - real device answers -1)
    1036: (2945691, "u32"),     # Cnt.TotOpTm (s)
    1038: (132, "s32"),         # Cnt.AcWhOut (x0.01 MWh)
    1040: (17655, "s32"),       # Cnt.TotAcWhOut (x0.01 MWh)
    1046: (-1, "s32"),          # Bat.TmpAvg - unsupported on PV Kodiak
    1048: (-1, "s32"),          # Bat.TmpMin
    1050: (-1, "s32"),          # Bat.TmpMax
    1052: (0, "s32"),           # Dcs.DcWInAval
    1054: (0, "s32"),           # Dcs.DcWOutAval
    1056: (-1, "s32"),          # Bat.SOCConn
    1058: (-1, "s32"),          # Bsc.WhInAvail
    1060: (-1, "s32"),          # Bsc.WhOutAvail
    1066: (-1, "s32"),          # Bsc.WInMax (battery channel: unsupported)
    1068: (0, "s32"),           # Dcs.DcW
    1070: (-1, "s32"),          # Bsc.WOutMax
    1072: (0, "s32"),           # Dcs.DcWhInAval
    1074: (-1, "s32"),          # Bsc.VArOxMax
    1076: (0, "s32"),           # Dcs.DcWhOutAval
    1078: (-1, "s32"),          # Bsc.VArUxMax
    1082: (51, "s32"),          # DcSw1Stt (51 = Closed)
    1084: (51, "s32"),          # DcSw2Stt
    1086: (51, "s32"),          # DcSw3Stt
    1088: (0, "s32"),           # Cnt.TotAcWhIn
    1090: (96, "u32"),          # Cnt.FrtDet
    1092: (308, "u32"),         # License.BasicGridForming (308 = On)
    1094: (1, "u16"),           # Hw.PreChaCfg
    1097: (51, "s32"),          # AcSwStt (51 = Closed)
    1099: (0, "u32"),           # DcPreChaRmgTm
    1103: (0, "u16"),           # Dcs.DevStt.Inst
    1104: (0, "u16"),           # Dcs.DevStt.Run
    1105: (0, "u16"),           # Dcs.DevStt.Err
    1106: (100000000, "s32"),   # PvGnd.RisIso (x0.1 kOhm)
    1108: (21603, "s32"),       # PwrOffReas (enum)
    1110: (0, "u32"),           # Cnt.GriForm.OvAmp
}
REAL_HR_STATIC = {
    1204: (381, "s32"),         # Dcs.OpMod (381 = Stop - no battery section)
    1206: (200, "u16"),         # PvGnd.RisIsoWarnLim (x0.1 kOhm)
    1207: (20, "u16"),          # PvGnd.RisIsoErrLim
    1212: (303, "s32"),         # PvGnd.OpnRemGfdi (303 = Off)
    1214: (-1, "s32"),          # Ec.DcAmpDynMax - unsupported: -1
    1216: (-1, "s32"),          # Ec.DcAmpDynMin
    1218: (-1, "s32"),          # Ec.DcAmpOpMax
    1220: (-1, "s32"),          # Ec.DcAmpOpMin
    1222: (500, "u32"),         # DcAmpSpntGra (A/s)
    1224: (-1, "s32"),          # (commented channel - device answers -1)
    1226: (-1, "s32"),          # (commented channel)
    1230: (-1, "u32"),          # Bsc.SrcSel
    1232: (-1, "u32"),          # Ec.DcVolDynMax
    1234: (-1, "u32"),          # Ec.DcVolDynMin
    1236: (-1, "u32"),          # Ec.DcVolOpMax
    1238: (-1, "u32"),          # Ec.DcVolOpMin
    1240: (2, "u32"),           # (commented channel - device answers 2)
    1242: (-1, "u32"),          # (commented channel)
    1244: (-1, "u32"),          # Bsc.InvStrMod
    1246: (3, "u32"),           # GriMng.BatWMinMod
    1248: (3, "u32"),           # GriMng.BatWMaxMod
    1252: (0, "u16"),           # DcDsch.Ena.Trg
    1253: (0, "u16"),           # DcDsch.Dis.Trg
}

# --------------------------------------------------------------------------
# Kodiak diagnostics (extracted from Kodiak_WR_Diagnoseliste)
#   sev: 'RD' = red fault (inverter trips, restart needed after ErrClr)
#        'YW' = yellow warning (inverter keeps running)
# --------------------------------------------------------------------------
KODIAK_ERRORS = {
    104:  ("EvtAcVMaxPpVFst",   "Grid overvoltage fast",                      "RD"),
    204:  ("EvtAcVMinPpVFst",   "Grid undervoltage fast",                     "RD"),
    502:  ("EvtAcHzMin",        "Minimum grid frequency disturbance",         "RD"),
    503:  ("EvtAcHzMax",        "Maximum grid frequency disturbance",         "RD"),
    1304: ("EvtExtPhSeq",       "Ext. network has no correct rotary field",   "RD"),
    3501: ("EvtLeakRis",        "Insulation failure",                         "RD"),
    3502: ("EvtGfdi",           "Ground fault detected",                      "RD"),
    3511: ("EvtGfdiWrn",        "Warning grounding error",                    "YW"),
    3601: ("EvtIsoFltWrn",      "Warning insulation failure",                 "YW"),
    6502: ("EvtOvTmpWCir",      "Overtemperature power unit",                 "RD"),
    6506: ("EvtOvTmpTrf",       "Overtemp. transformer area",                 "RD"),
    6508: ("EvtOvTmpExl",       "Overtemperature outside",                    "RD"),
    7502: ("EvtFanIn2",         "Fan fault interior 2",                       "YW"),
    7602: ("EvtComCan2",        "CAN communication error (no lifesign)",      "RD"),
    7707: ("EvtAcSwFlt",        "AC separating point",                        "RD"),
    7902: ("EvtStkOvTmpIGBT",   "Overtemperature IGBT",                       "RD"),
    8712: ("EvtTmOutGriMgtOn",  "Timeout comm. grid mgmt, device continues",  "YW"),
    8713: ("EvtTmOutGriMgtOff", "Timeout comm. grid mgmt, switching off",     "RD"),
    9009: ("EvtFstStop",        "Quick stop",                                 "RD"),
    9017: ("EvtFstStopMan",     "Quick stop triggered manually",              "RD"),
    9026: ("EvtFstStopExt",     "Fast stop external triggered",               "RD"),
}
ERR_FSTSTOP = 9009

# Startup walk (Cpu2OpStt sheet). These used to be display-only names with the
# OpStt register parked at 381 (Stop) for the whole connect sequence. A real
# Sunny Central reports the walk in OpStt itself - the enum values below are
# the OpStt (uniqueid 332) members from MODBUS-SCxxx-TI-en-19 section 5.3.2
# and the same mapping in ppc_profile_7_3.ppc, so the HYC sees the connect
# sequence at IR 1004 exactly as it would from real hardware.
#   (display name, OpStt enum)
STARTUP_PHASES = (("Init", 1787), ("WaitAC", 1394), ("ConnectAC", 3524),
                  ("WaitDC", 1393), ("ConnectDC", 3525))

# ==========================================================================
# METER register map (SMA Power Analyzer, POI block)
# ==========================================================================
M_VTG_POI, M_FAC_POI = 5020, 5022
M_PWR_AT_POI, M_PWR_AT_L1, M_PWR_AT_L2, M_PWR_AT_L3 = 5024, 5026, 5028, 5030
M_PWR_RT_POI, M_PWR_RT_L1, M_PWR_RT_L2, M_PWR_RT_L3 = 5032, 5034, 5036, 5038
M_PWR_AP_POI, M_PF_POI = 5040, 5042
M_VTG_L1L2, M_VTG_L2L3, M_VTG_L3L1 = 5044, 5046, 5048
M_VTG_L1, M_VTG_L2, M_VTG_L3, M_VTG_AVG_LN, M_VTG_AVG_LL = 5050, 5052, 5054, 5056, 5058
M_IAC_L1, M_IAC_L2, M_IAC_L3 = 5060, 5062, 5064

# ==========================================================================
# Register-monitor name maps - HARDCODED from the PPC profile
# "Sunny Central Kodiak - family" unitid=102 susyid=274 version=1035
# (ppc_profile_7_3.ppc). addr -> (channel name, n words, type)
#   type: s32/u32 = SMA 32-bit big-word-first pair, s16/u16 = single register
# The register monitor ONLY names/displays addresses in these maps (plus the
# three verified raw HYC write addresses below). Anything else the HYC
# touches is flagged unknown and hidden behind the "show unknown" toggle.
# ==========================================================================
_T_NW = {"s32": 2, "u32": 2, "s16": 1, "u16": 1}

_INV_IR_PROFILE = (
    (    0, "Cnt.TotAcWhOut (kWh)", "u32"),
    (    2, "Cnt.AcWhOut (kWh)", "u32"),
    (    4, "Cnt.TotOpTm (s)", "u32"),
    (    6, "Cnt.TotFeedTm (s)", "u32"),
    (   32, "ErrNoSma", "u32"),
    (   34, "VArSpt (kVAr)", "s32"),
    (   36, "WSpt (kW)", "s32"),
    (   50, "DcMs.Amp.Stk.Sum (A)", "s32"),
    (   52, "GriMs.V.PhsAB (x0.1 V)", "s32"),
    (   54, "GriMs.V.PhsBC (x0.1 V)", "s32"),
    (   56, "GriMs.V.PhsCA (x0.1 V)", "s32"),
    (   58, "GriMs.Hz (x0.01 Hz)", "s32"),
    (   60, "InvMs.TotVAr (kVAr)", "s32"),
    (   62, "InvMs.TotVA (kVA)", "s32"),
    (   70, "Modbusd.Sys.Tm (s)", "u32"),
    (   72, "KeySwitch", "s32"),
    (   74, "OpStt", "s32"),
    (   76, "ErrorStt", "s32"),
    (   78, "DcMs_Vol (x0.1 V)", "s32"),
    (   80, "InvMs.TotW (kW)", "s32"),
    ( 1000, "DevInf.SerNo", "u32"),
    ( 1002, "HaNsDampF", "s32"),
    ( 1004, "OpStt", "s32"),
    ( 1006, "Rio.KeySw", "s32"),
    ( 1008, "ErrNo", "u32"),
    ( 1010, "DcMs.TotWatt (x0.1)", "s32"),
    ( 1012, "InvMs.TotW (kW)", "s32"),
    ( 1014, "InvMs.TotVAr (kVAr)", "s32"),
    ( 1016, "InvMs.TotVA (kVA)", "s32"),
    ( 1018, "License.Inertia", "u32"),
    ( 1020, "Dcs.TotSoc (x0.1 %)", "u16"),
    ( 1021, "Dcs.TotSoh (x0.1 %)", "u16"),
    ( 1024, "GriMs.V.PhsAB (x0.1 V)", "s32"),
    ( 1026, "GriMs.V.PhsBC (x0.1 V)", "s32"),
    ( 1028, "GriMs.V.PhsCA (x0.1 V)", "s32"),
    ( 1030, "GriMs.Hz (x0.01 Hz)", "s32"),
    ( 1032, "DcMs.Vol (x0.1 V)", "s32"),
    ( 1034, "DcMs.TotAmp (A)", "s32"),
    ( 1036, "Cnt.TotOpTm (s)", "u32"),
    ( 1038, "Cnt.AcWhOut (x0.01 MWh)", "s32"),
    ( 1040, "Cnt.TotAcWhOut (x0.01 MWh)", "s32"),
    ( 1042, "InvMs.TotW (kW)", "s32"),
    ( 1044, "InvMs.TotVAr (kVAr)", "s32"),
    ( 1046, "Bat.TmpAvg (x0.1 °C)", "s32"),
    ( 1048, "Bat.TmpMin (x0.1 °C)", "s32"),
    ( 1050, "Bat.TmpMax (x0.1 °C)", "s32"),
    ( 1052, "Dcs.DcWInAval (kW)", "s32"),
    ( 1054, "Dcs.DcWOutAval (kW)", "s32"),
    ( 1056, "Bat.SOCConn (x0.1 %)", "s32"),
    ( 1058, "Bsc.WhInAvail (x0.1 kWh)", "s32"),
    ( 1060, "Bsc.WhOutAvail (x0.1 kWh)", "s32"),
    ( 1062, "WAval (pu)", "s32"),
    ( 1064, "VArAval (pu)", "s32"),
    ( 1066, "Bsc.WInMax (x0.1 kW)", "s32"),
    ( 1068, "Dcs.DcW (x0.1 kW)", "s32"),
    ( 1070, "Bsc.WOutMax (x0.1 kW)", "s32"),
    ( 1072, "Dcs.DcWhInAval (kWh)", "s32"),
    ( 1074, "Bsc.VArOxMax (x0.1 kVAr)", "s32"),
    ( 1076, "Dcs.DcWhOutAval (kWh)", "s32"),
    ( 1078, "Bsc.VArUxMax (x0.1 kVAr)", "s32"),
    ( 1080, "VArAval (FIX4 pu)", "s32"),
    ( 1082, "DcSw1Stt", "s32"),
    ( 1084, "DcSw2Stt", "s32"),
    ( 1086, "DcSw3Stt", "s32"),
    ( 1088, "Cnt.TotAcWhIn (x0.01 MWh)", "s32"),
    ( 1090, "Cnt.FrtDet", "u32"),
    ( 1092, "License.BasicGridForming", "u32"),
    ( 1094, "Hw.PreChaCfg", "u16"),
    ( 1095, "ErrorStt", "s32"),
    ( 1097, "AcSwStt", "s32"),
    ( 1099, "DcPreChaRmgTm", "u32"),
    ( 1101, "WAval (FIX4 pu)", "s32"),
    ( 1103, "Dcs.DevStt.Inst", "u16"),
    ( 1104, "Dcs.DevStt.Run", "u16"),
    ( 1105, "Dcs.DevStt.Err", "u16"),
    ( 1106, "PvGnd.RisIso (x0.1 kOhm)", "s32"),
    ( 1108, "PwrOffReas", "s32"),
    ( 1110, "Cnt.GriForm.OvAmp", "u32"),
    ( 1112, "WSptMin (kW)", "s32"),
    ( 1114, "WSptMax (kW)", "s32"),
    ( 2000, "DevInf.SerNo", "u32"),
    ( 2002, "HaNsDampF", "s32"),
    ( 2004, "OpStt", "s32"),
    ( 2006, "Rio.KeySw", "s32"),
    ( 2008, "ErrNo", "u32"),
    ( 2010, "GfdiSwStt", "s32"),
    ( 2012, "InvMs.TotW (kW)", "s32"),
    ( 2014, "InvMs.TotVAr (kVAr)", "s32"),
    ( 2016, "InvMs.TotVA (kVA)", "s32"),
    ( 2018, "DcMs.Vol.PosGnd (x0.1 V)", "s32"),
    ( 2020, "DcMs.Vol.NegGnd (x0.1 V)", "s32"),
    ( 2022, "Gfdi.AmpPrc (x0.01 A)", "s16"),
    ( 2023, "Gfdi.AmpErr (x0.01 A)", "s16"),
    ( 2024, "GriMs.V.PhsAB (x0.1 V)", "s32"),
    ( 2026, "GriMs.V.PhsBC (x0.1 V)", "s32"),
    ( 2028, "GriMs.V.PhsCA (x0.1 V)", "s32"),
    ( 2030, "GriMs.Hz (x0.01 Hz)", "s32"),
    ( 2032, "DcMs.Vol (x0.1 V)", "s32"),
    ( 2034, "DcMs.TotAmp (A)", "s32"),
    ( 2036, "Cnt.TotOpTm (s)", "u32"),
    ( 2038, "Cnt.AcWhOut (x0.01 MWh)", "s32"),
    ( 2040, "Cnt.TotAcWhOut (x0.01 MWh)", "s32"),
    ( 2042, "DcDschSwStt", "s32"),
    ( 2044, "DcGndSwStt", "s32"),
    ( 2046, "DcPreChaSwStt", "s32"),
    ( 2080, "VArAval (FIX4 pu)", "s32"),
    ( 2082, "DcSw1Stt", "s32"),
    ( 2084, "DcSw2Stt", "s32"),
    ( 2086, "DcSw3Stt", "s32"),
    ( 2088, "Cnt.TotAcWhIn (x0.01 MWh)", "s32"),
    ( 2090, "Cnt.FrtDet", "u32"),
    ( 2094, "Hw.PreChaCfg", "u16"),
    ( 2095, "ErrorStt", "s32"),
    ( 2097, "AcSwStt", "s32"),
    ( 2099, "DcPreChaRmgTm", "u32"),
    ( 2101, "WAval (FIX4)", "s32"),
    ( 2106, "PvGnd.RisIso (x0.1 kOhm)", "s32"),
    ( 2108, "PwrOffReas", "s32"),
    ( 2112, "WSptMin (kW)", "s32"),
    ( 2114, "WSptMax (kW)", "s32"),
)
_INV_HR_PROFILE = (
    (    0, "Modbusd.PPC.Prf.Rev", "u32"),
    (    2, "Modbusd.Dev.Id", "u32"),
    (    4, "WRtg (KW)", "s32"),
    (    6, "GriMng.VArMod", "s32"),
    (    8, "GriMng.WMod", "s32"),
    (   10, "VArRtg (kVAr)", "s32"),
    (   12, "QoDMod", "s32"),
    (   14, "VARtg (kVA)", "s32"),
    ( 1200, "InvOpMod", "s32"),
    ( 1202, "RemRdy", "s32"),
    ( 1204, "Dcs.OpMod", "s32"),
    ( 1206, "PvGnd.RisIsoWarnLim (x0.1 kOhm)", "u16"),
    ( 1207, "PvGnd.RisIsoErrLim (x0.1 kOhm)", "u16"),
    ( 1208, "WRtg (kW)", "s32"),
    ( 1210, "VArRtg (kVAr)", "s32"),
    ( 1212, "PvGnd.OpnRemGfdi", "s32"),
    ( 1214, "Ec.DcAmpDynMax (A)", "s32"),
    ( 1216, "Ec.DcAmpDynMin (A)", "s32"),
    ( 1218, "Ec.DcAmpOpMax (A)", "s32"),
    ( 1220, "Ec.DcAmpOpMin (A)", "s32"),
    ( 1222, "DcAmpSpntGra (A/s)", "u32"),
    ( 1228, "Modbusd.PPC.Prf.Rev", "u32"),
    ( 1230, "Bsc.SrcSel", "u32"),
    ( 1232, "Ec.DcVolDynMax (V)", "u32"),
    ( 1234, "Ec.DcVolDynMin (V)", "u32"),
    ( 1236, "Ec.DcVolOpMax (V)", "u32"),
    ( 1238, "Ec.DcVolOpMin (V)", "u32"),
    ( 1244, "Bsc.InvStrMod", "u32"),
    ( 1246, "GriMng.BatWMinMod", "u32"),
    ( 1248, "GriMng.BatWMaxMod", "u32"),
    ( 1250, "VARtg (kVA)", "u32"),
    ( 1252, "DcDsch.Ena.Trg", "u16"),
    ( 1253, "DcDsch.Dis.Trg", "u16"),
    ( 1400, "AuxCtl.SCSOpCmd", "u32"),
    ( 1402, "AuxCtl.SOCOpMax (x0.01 %)", "s32"),
    ( 1404, "AuxCtl.SOCOpMin (x0.01 %)", "s32"),
    ( 1406, "GriForm.AcCtl.InertiaPlantLevelMod", "s32"),
    ( 1500, "VArSpt (%)", "s16"),
    ( 1501, "WSpt (%)", "s16"),
    ( 1502, "WSptMin (%)", "s32"),
    ( 1504, "BatWSptMax (%)", "s32"),
    ( 1506, "BatWSptMin (%)", "s32"),
    ( 1508, "FstStop", "u32"),
    ( 1510, "HzNomSpt (Hz)", "u32"),
    ( 1512, "VolNomSpt (pu)", "u16"),
    ( 1513, "Poi.DiffW (%)", "s32"),
    ( 1515, "Poi.DiffVAr (%)", "s32"),
    ( 1517, "Poi.Vol (pu)", "s32"),
    ( 1519, "WFwd (%)", "s16"),
    ( 1520, "VArFwd (%)", "s16"),
    ( 1530, "BatWSptMax (%)", "s32"),
    ( 1532, "BatWSptMin (%)", "s32"),
    ( 1534, "FstStop", "u32"),
    ( 1536, "HzNomSpt (Hz)", "u32"),
    ( 1538, "VolNomSpt (pu)", "u16"),
    ( 1539, "Dcs.SocMax (x0.1 %)", "u16"),
    ( 1540, "Dcs.SocMin (x0.1 %)", "u16"),
    ( 1541, "Dcs.OpMod", "s32"),
    ( 1543, "Dcs.BatPriEna", "u16"),
    ( 1544, "Dcs.DcDcErrClr", "u16"),
    ( 1700, "VArSpt (%)", "s16"),
    ( 1701, "DcAmpSpt (A)", "s32"),
)
INV_IR_NAMES = {a: (n, _T_NW[t], t) for a, n, t in _INV_IR_PROFILE}
INV_HR_NAMES = {a: (n, _T_NW[t], t) for a, n, t in _INV_HR_PROFILE}
# Raw addresses this HYC firmware actually writes on the wire; they are the
# profile channels' *uniqueids* (VArSpt@1500 uid 40022, WSpt@1501 uid 40023,
# FstStop@1508 uid 40018). Always visible in the monitor.
INV_HR_NAMES[W_FSTSTOP] = ("FstStop [legacy uid fallback]", 2, "u32")
INV_HR_NAMES[W_VARSPT] = ("VArSpt [legacy uid fallback] (FIX2 %)", 1, "s16")
INV_HR_NAMES[W_WSPT] = ("WSpt [legacy uid fallback] (FIX2 %)", 1, "s16")
INV_HR_NAMES[W_ERRCLR] = ("ErrClr [legacy uid fallback]", 1, "u16")
# name 1544 for what it does here: it is the channel the controller writes to
# acknowledge an error (the .ppc calls it Dcs.DcDcErrClr)
INV_HR_NAMES[P_ERRCLR] = ("ErrClr (Dcs.DcDcErrClr)", 1, "u16")
# always shown in the monitor, traffic or not: the three HYC <100ms groups
# (read IR 1000-1114, read HR 1200-1253, write HR 1500-1520) + raw setpoints
INV_IR_ALWAYS = frozenset(a for a in INV_IR_NAMES if 1000 <= a <= 1114)
# legacy uid trio deliberately NOT in the always set: it only appears in
# the monitor if the HYC actually falls back to writing it, so the
# "written by HYC" section shows ONE setpoint block in normal operation
INV_HR_ALWAYS = frozenset(a for a in INV_HR_NAMES
                          if 1200 <= a <= 1253 or 1500 <= a <= 1520) | {P_ERRCLR}

MET_NAMES = {}
for _a, _n, _t in (
    (M_VTG_POI,    "VtgPoi (V L-L)",       "s32"),
    (M_FAC_POI,    "FacPoi (mHz)",         "u32"),
    (M_PWR_AT_POI, "PwrAtPoi (W)",         "s32"),
    (M_PWR_AT_L1,  "PwrAtPoiL1 (W)",       "s32"),
    (M_PWR_AT_L2,  "PwrAtPoiL2 (W)",       "s32"),
    (M_PWR_AT_L3,  "PwrAtPoiL3 (W)",       "s32"),
    (M_PWR_RT_POI, "PwrRtPoi (VAr)",       "s32"),
    (M_PWR_RT_L1,  "PwrRtPoiL1 (VAr)",     "s32"),
    (M_PWR_RT_L2,  "PwrRtPoiL2 (VAr)",     "s32"),
    (M_PWR_RT_L3,  "PwrRtPoiL3 (VAr)",     "s32"),
    (M_PWR_AP_POI, "PwrApPoi (VA)",        "s32"),
    (M_PF_POI,     "PFPoi (x0.001)",       "s32"),
    (M_VTG_L1L2,   "VtgPoiL1L2 (V)",       "s32"),
    (M_VTG_L2L3,   "VtgPoiL2L3 (V)",       "s32"),
    (M_VTG_L3L1,   "VtgPoiL3L1 (V)",       "s32"),
    (M_VTG_L1,     "VtgPoiL1 (V L-N)",     "s32"),
    (M_VTG_L2,     "VtgPoiL2 (V L-N)",     "s32"),
    (M_VTG_L3,     "VtgPoiL3 (V L-N)",     "s32"),
    (M_VTG_AVG_LN, "VtgPoiAvg (V L-N)",    "s32"),
    (M_VTG_AVG_LL, "VtgPoiAvg (V L-L)",    "s32"),
    (M_IAC_L1,     "IacPoiL1 (mA)",        "s32"),
    (M_IAC_L2,     "IacPoiL2 (mA)",        "s32"),
    (M_IAC_L3,     "IacPoiL3 (mA)",        "s32"),
    (5066,         "EgyConsTotPoi (Wh)",   "u32"),
    (5068,         "EgyDelTotPoi (Wh)",    "u32"),
    (5074,         "ModelTag",             "u32"),
):
    MET_NAMES[_a] = (_n, 2, _t)
for _a, _n in ((5070, "GSP274SyncState"), (5071, "DigIo1Raw"),
               (5072, "DigIo2Raw"), (5073, "Gsp274Ena")):
    MET_NAMES[_a] = (_n, 1, "u16")


# --------------------------------------------------------------------------
# packing helpers (SMA = high word first)
# --------------------------------------------------------------------------
def set_s32(block, addr, value):
    u = int(round(float(value))) & 0xFFFFFFFF
    block.set_internal(addr, [(u >> 16) & 0xFFFF, u & 0xFFFF])


def set_u16(block, addr, value):
    block.set_internal(addr, [int(round(float(value))) & 0xFFFF])


def get_s32(block, addr):
    hi, lo = block.get_internal(addr, 2)
    u = (hi << 16) | lo
    return u - 0x100000000 if u >= 0x80000000 else u


def get_u32(block, addr):
    hi, lo = block.get_internal(addr, 2)
    return (hi << 16) | lo


def get_s16(block, addr):
    v = block.get_internal(addr, 1)[0]
    return v - 0x10000 if v >= 0x8000 else v


def _step_toward(cur, tgt, rate, dt):
    step = rate * dt
    if abs(tgt - cur) <= step:
        return tgt
    return cur + step if tgt > cur else cur - step




class PermissiveBlock(ModbusSparseDataBlock):
    """Accepts ANY address; never IllegalAddress. Unwritten cells answer
    `fill` - the REAL Kodiak answers 0xFFFF there (census 2026-07-16), so
    inverter blocks use fill=0xFFFF while the meter keeps 0.

    getValues()/setValues() are only ever called by the pymodbus server, i.e.
    they represent REAL Modbus traffic from the HYC. The simulator itself uses
    get_internal()/set_internal(), so the per-address timestamps in self.ext
    are a faithful record of what the HYC actually read/wrote (register
    monitor), including WHICH TRANSPORT (Modbus TCP or UDP) last touched it.
    A register can be 'held' (self.holds): neither the sim nor the
    HYC can change it until released."""
    def __init__(self, fill=0):
        super().__init__({0: 0}); self.mutable = True; self.on_write = None
        self.last_proto = None
        self.fill = fill & 0xFFFF
        # the {0: 0} above only satisfies the sparse-block constructor; drop
        # it so address 0 doesn't fake a seeded register in the monitor
        # (profile channels really do live at address 0)
        self.values.pop(0, None)
        # addr -> [last_HYC_read_ts | None, last_HYC_write_ts | None,
        #          set of transports seen ('T' tcp / 'U' udp)]
        self.ext = {}
        self.holds = {}  # addr -> held raw u16 value
    def validate(self, address, count=1):
        return True
    def _touch(self, address, count, slot, proto=None):
        now = time.time()
        for i in range(count):
            e = self.ext.get(address + i)
            if e is None:
                e = [None, None, set()]
                self.ext[address + i] = e
            e[slot] = now
            if proto:
                e[2].add(proto)
    def hyc_get(self, address, count=1, proto=None):    # HYC read
        self._touch(address, count, 0, proto)
        return [self.values.get(address + i, self.fill) for i in range(count)]
    def hyc_set(self, address, values, proto=None):     # HYC write
        # remembered so a write mirrored from an alias unit into the main
        # block keeps the transport it really arrived on (the register
        # monitor's TCP/UDP column would otherwise always say TCP)
        self.last_proto = proto
        if not isinstance(values, (list, tuple)): values=[values]
        for i, v in enumerate(values):
            a = address + i
            self.values[a] = self.holds.get(a, int(v) & 0xFFFF)
        self._touch(address, len(values), 1, proto)
        if self.on_write:
            try: self.on_write(address, len(values))
            except Exception: pass
    # pymodbus-facing names (only hit if a block is used without a
    # TransportView; transport then stays unknown)
    def getValues(self, address, count=1):
        return self.hyc_get(address, count)
    def setValues(self, address, values):
        self.hyc_set(address, values)
    def set_internal(self, address, values):
        """Write without triggering on_write or HYC-activity stamps
        (sim-internal, not HYC comms). Held registers keep their value."""
        for i, v in enumerate(values):
            a = address + i
            self.values[a] = self.holds.get(a, int(v) & 0xFFFF)
    def get_internal(self, address, count=1):
        """Read without leaving an HYC-activity stamp (sim-internal)."""
        return [self.values.get(address + i, self.fill) for i in range(count)]
    def force(self, address, values):
        """Manual GUI write: overrides even a held register (updates hold)."""
        for i, v in enumerate(values):
            a = address + i
            v = int(v) & 0xFFFF
            self.values[a] = v
            if a in self.holds:
                self.holds[a] = v
    def hold(self, address, nwords):
        for i in range(nwords):
            a = address + i
            self.holds[a] = self.values.get(a, 0)
    def release(self, address, nwords):
        for i in range(nwords):
            self.holds.pop(address + i, None)


class TransportView:
    """Thin per-transport wrapper handed to pymodbus so each real HYC
    read/write is stamped with the transport it arrived on ('T' = Modbus
    TCP, 'U' = Modbus UDP). Both views share ONE underlying PermissiveBlock,
    so TCP and UDP always see identical register data."""
    def __init__(self, block, proto):
        self.block, self.proto = block, proto
    def validate(self, address, count=1):
        return True
    def getValues(self, address, count=1):
        return self.block.hyc_get(address, count, self.proto)
    def setValues(self, address, values):
        self.block.hyc_set(address, values, self.proto)


def _dual_contexts(*, ir, hr, strict, windows_by_fc, label="?",
                   unit_id=None, alias_units=(), alias_blocks=None):
    """(tcp_context, udp_context) sharing the same underlying blocks but
    tagging traffic with the transport it came in on, and enforcing the
    profile address windows when strict addressing is on.

    unit_id=None keeps the old behavior (single=True: every unit id answers
    the main blocks). With unit_id set, the census-measured Kodiak unit-id
    layering is replicated: the main blocks answer ONLY on `unit_id`;
    `alias_units` (real HW: 1, 2, 3) answer all-0xFFFF data from a shared
    scratch block pair that also absorbs the HYC's one-shot u2/u3 writes
    (@40018/@127/@129 - the real unit tolerates them); any other unit id
    (e.g. 0/255) gets a Modbus error like the real firmware."""
    ctxs = []
    for proto in ("T", "U"):
        store = StrictSlaveContext(
            strict, windows_by_fc,
            label=f"{label} ({'TCP' if proto == 'T' else 'UDP'})",
            ir=TransportView(ir, proto), hr=TransportView(hr, proto),
            zero_mode=True)
        if unit_id is None:
            ctxs.append(ModbusServerContext(slaves=store, single=True))
        else:
            slaves = {int(unit_id): store}
            if alias_blocks is not None:
                a_ir, a_hr = alias_blocks
                alias = ModbusSlaveContext(
                    ir=TransportView(a_ir, proto),
                    hr=TransportView(a_hr, proto), zero_mode=True)
                for u in alias_units:
                    slaves[int(u)] = alias
            ctxs.append(ModbusServerContext(slaves=slaves, single=False))
    return ctxs



# --------------------------------------------------------------------------
# STRICT ADDRESSING: real firmware answers IllegalDataAddress for registers
# outside its profile groups ("Only 4xxxx Registers can be allways written
# ... otherwise an modbus exception is given" - profile comment). The sim
# mimics that: requests fully inside a window are served (gaps inside a
# group answer like real HW), anything else gets exception 0x02.
# Toggle live in the GUI ("strict addressing").
# --------------------------------------------------------------------------
# FLEX device type block-reads IR 1000 x124 (8 extra channels vs the
# Kodiak template's x116) - windows extended so both templates work.
INV_IR_WINDOWS = ((0, 82), (1000, 1124), (2000, 2124))
INV_HR_READ_WINDOWS = ((0, 16), (1200, 1254), (1400, 1408),
                       (1500, 1545), (1700, 1703))
# NOTE: the real firmware rule is "Only 4xxxx Registers can be allways
# written" - i.e. the ENTIRE uniqueid range 40000+ accepts writes, not just
# the three setpoint uniqueids. The HYC writes its legacy setpoint group as
# a block (and may touch other uniqueids); whitelisting only 40018/40022/
# 40023 made those FC16 writes fail with IllegalAddress on the sim while a
# real Kodiak accepts them.
INV_HR_WRITE_WINDOWS = ((127, 199), (1244, 1246), (1252, 1254),
                        (1400, 1408), (1500, 1545), (1700, 1703),
                        (W_ERRCLR, W_ERRCLR + 1), (40000, 50000))
# 1244 Bsc.InvStrMod: MEASURED - the HYC writes fc16@1244 x2 = 1438 to the
#   battery unit (tcpdump 2026-07, the same capture the BESS image came from,
#   and again on the Culcairn bench). It sits in the 1200 read group, so the
#   sim used to answer that write with IllegalDataAddress even though the real
#   unit accepts it and the docstring above says the HYC makes it.
# 1252/1253 DcDsch.Ena.Trg / Dis.Trg: NOT measured, allowed on the profile's
#   own naming - a "Trg" (trigger) channel that cannot be written is
#   meaningless. Remove this span if a real unit is ever seen rejecting it.
MET_WINDOWS = ((5000, 5120),)
# the HYC also WRITES the meter: observed FC16 @20014 x2 = enum 1749
# (fast-stop/GSP command) on the bench. Allow a small window around it;
# reads stay limited to the 5000-block like before.
MET_WRITE_WINDOWS = ((5000, 5120), (20000, 20050))
WRITE_FCS = frozenset((5, 6, 15, 16, 22, 23))


def _in_windows(windows, address, count):
    return any(lo <= address and address + count <= hi
               for lo, hi in windows)


class StrictSlaveContext(ModbusSlaveContext):
    """Slave context that knows the function code during validation, so
    reads and writes can be checked against separate profile windows.
    A request outside its window gets IllegalDataAddress, like real
    firmware. strict is a shared dict ({'on': bool}) toggled from the GUI;
    windows_by_fc maps function code -> allowed (lo, hi) spans."""
    def __init__(self, strict, windows_by_fc, label="?", **kw):
        super().__init__(**kw)
        self._strict = strict
        self._wins = windows_by_fc
        self._label = label
        self._rej_seen = {}   # (fx, address, count) -> last log timestamp
    def validate(self, fx, address, count=1):
        if not self._strict.get("on"):
            return True
        if _in_windows(self._wins.get(fx, ()), address, count):
            return True
        # rejected: say WHAT was rejected (rate-limited to 1 line / 5 s per
        # unique request shape, so a retrying HYC can't flood the console)
        key = (fx, address, count)
        now = time.time()
        if now - self._rej_seen.get(key, 0.0) >= 5.0:
            self._rej_seen[key] = now
            kind = "write" if fx in WRITE_FCS else "read"
            print(f"[strict] {self._label} REJECT fc{fx} {kind} "
                  f"@{address} x{count} -> IllegalDataAddress "
                  f"(outside profile windows; repeats muted 5s)")
        return False


# ==========================================================================
# Controllable Modbus server (start/stop at runtime)
# ==========================================================================
# --------------------------------------------------------------------------
# "who is talking to this device?" - the GUI shows the remote IP connected to
# each simulated inverter / the meter, so it is obvious on a shared bench
# which controller has picked up which port.
#
# Read from the OS socket table rather than from pymodbus internals: the
# server classes were rewritten between pymodbus versions, and the socket
# table is the same answer without the version coupling. /proc/net/tcp is the
# fast path (the sim's normal home is a Raspberry Pi); anything else falls
# back to parsing netstat. Results are cached briefly - the GUI polls at 1 Hz
# and a controller connection lasts for hours.
# --------------------------------------------------------------------------
_PEER_CACHE = {"ts": 0.0, "by_port": {}}
_PEER_TTL = 2.0


def _hex_ip(h):
    """/proc/net/tcp encodes IPv4 little-endian hex, IPv6 as 32 hex chars."""
    if len(h) == 8:
        b = bytes.fromhex(h)[::-1]
        return ".".join(str(x) for x in b)
    try:
        b = bytes.fromhex(h)
        grp = [b[i:i + 4][::-1] for i in range(0, 16, 4)]
        flat = b"".join(grp)
        if flat[:12] == bytes(10) + bytes([255, 255]):   # v4-mapped
            return ".".join(str(x) for x in flat[12:])
        return ":".join(f"{flat[i]<<8 | flat[i+1]:x}" for i in range(0, 16, 2))
    except Exception:
        return h


def _scan_proc_net():
    """{local_port: set(remote_ip)} for ESTABLISHED TCP, from /proc.

    Returns None when /proc is not available at all, so the caller can tell
    "this OS has no /proc" from "there are simply no connections" - otherwise
    an idle Linux box would fall through and spawn netstat on every refresh."""
    out, seen = {}, False
    for path in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            with open(path) as fh:
                seen = True
                next(fh, None)
                for line in fh:
                    f = line.split()
                    if len(f) < 4 or f[3] != "01":     # 01 = ESTABLISHED
                        continue
                    lp = int(f[1].split(":")[1], 16)
                    rip, rport = f[2].split(":")
                    if int(rport, 16) == 0:
                        continue
                    out.setdefault(lp, set()).add(_hex_ip(rip))
        except Exception:
            continue
    return out if seen else None


def _scan_netstat():
    """Same shape as _scan_proc_net(), for hosts without /proc (dev boxes)."""
    import subprocess
    out = {}
    try:
        txt = subprocess.run(["netstat", "-an"], capture_output=True, text=True,
                             timeout=4).stdout
    except Exception:
        return out
    for line in txt.splitlines():
        f = line.split()
        if len(f) < 4 or not f[0].upper().startswith("TCP"):
            continue
        if "ESTABLISHED" not in line.upper():
            continue
        try:
            lp = int(f[1].rsplit(":", 1)[1])
            rip = f[2].rsplit(":", 1)[0].strip("[]")
        except (ValueError, IndexError):
            continue
        out.setdefault(lp, set()).add(rip)
    return out


_PEER_LOCK = threading.Lock()
_PEER_THREAD = None


def _peer_refresh_loop():
    while True:
        try:
            by_port = _scan_proc_net()
            if by_port is None:          # no /proc on this OS
                by_port = _scan_netstat()
        except Exception:
            by_port = {}
        with _PEER_LOCK:
            _PEER_CACHE["by_port"] = by_port
            _PEER_CACHE["ts"] = time.time()
        time.sleep(_PEER_TTL)


def _start_peer_watch():
    """Refresh the socket table on its own thread.

    _peers_for_port() is called from _build_snapshot(), which runs inside the
    plant tick lock - doing the scan there would stall the whole simulation
    for as long as it takes, and the netstat fallback can take a good fraction
    of a second. The reader now only touches a dict."""
    global _PEER_THREAD
    if _PEER_THREAD is None:
        _PEER_THREAD = threading.Thread(target=_peer_refresh_loop, daemon=True)
        _PEER_THREAD.start()


def _peers_for_port(port):
    """Remote IPs currently holding a TCP connection to `port` (sorted)."""
    with _PEER_LOCK:
        return sorted(_PEER_CACHE["by_port"].get(int(port), ()))


def _port_free(host, port):
    """(ok, reason): can BOTH a TCP and a UDP listener bind this port?

    Checked before a device is moved onto a port - a port owned by a foreign
    process used to be reported as a successful rebind, because the fleet only
    knew about the ports it had handed out itself and the real bind error
    surfaced asynchronously inside the server thread."""
    bind_host = "" if host in ("0.0.0.0", "", None) else host
    for kind, styp in (("TCP", socket.SOCK_STREAM), ("UDP", socket.SOCK_DGRAM)):
        s = socket.socket(socket.AF_INET, styp)
        try:
            s.bind((bind_host, int(port)))
        except OSError as e:
            return False, f"{kind}: {e.strerror or e}"
        finally:
            s.close()
    return True, ""


class DeviceServer:
    def __init__(self, ctx_tcp, ctx_udp, host, port, udp_cfg=None):
        self.ctx_tcp, self.ctx_udp = ctx_tcp, ctx_udp
        self.host, self.port = host, port
        # shared plant-wide dict: {"silent_writes": bool}. The real Kodiak
        # treats UDP setpoint writes as fire-and-forget (wire captures show
        # ZERO UDP responses), so default is silent; toggle in the GUI.
        self.udp_cfg = udp_cfg if udp_cfg is not None else {"silent_writes": True}
        self.loop = self.server = self.userver = self.thread = None
        self.up = set()   # listeners that actually bound: {'TCP', 'UDP'}
        self.running = False
        self.error = None

    def _udp_response_manip(self, response):
        """pymodbus response hook on the UDP listener: suppress replies to
        write requests (FC 05/06/15/16, incl. their exception responses)
        to mimic the real inverter's fire-and-forget UDP behavior.
        Reads over UDP are still answered."""
        if self.udp_cfg.get("silent_writes") and \
                (getattr(response, "function_code", 0) & 0x7F) in (5, 6, 15, 16):
            response.should_respond = False
        return response, False

    def start(self):
        """Start both listeners. Returns True only if at least one of them
        really came up - a bind failure happens asynchronously in the server
        thread, so 'running' used to claim success for a dead port."""
        if self.running:
            return True
        self.error = None
        self.up = set()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        self.running = True
        # _guarded() marks a listener up as it enters serve_forever() and
        # drops it again if the bind raises, so a short settle is enough to
        # tell a working port from a taken one.
        deadline = time.monotonic() + 0.5
        while time.monotonic() < deadline and len(self.up) < 2 and not self.error:
            time.sleep(0.01)
        time.sleep(0.05)
        if not self.up:
            self.running = False
        return self.running

    def _run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._serve())
        except Exception as e:
            self.error = str(e)
            self.running = False
        finally:
            try:
                self.loop.close()
            except Exception:
                pass

    def _note_error(self, msg):
        self.error = f"{self.error} \u00b7 {msg}" if self.error else msg

    async def _guarded(self, name, srv):
        """Serve one listener; a failure (e.g. UDP port in use) is reported
        in the GUI but does NOT take the sibling listener down."""
        try:
            self.up.add(name)
            await srv.serve_forever()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.up.discard(name)
            self._note_error(f"{name} listener failed: {e}")

    async def _serve(self):
        self.server = ModbusTcpServer(self.ctx_tcp, address=(self.host, self.port))
        self.userver = ModbusUdpServer(self.ctx_udp, address=(self.host, self.port),
                                       response_manipulator=self._udp_response_manip)
        await asyncio.gather(self._guarded("TCP", self.server),
                             self._guarded("UDP", self.userver))

    def stop(self):
        if not self.running:
            return
        # give _serve() a moment to have created the server objects (races on
        # a very fast enable->disable click), then shut both listeners down.
        for _ in range(20):
            if getattr(self, "server", None) is not None or self.error:
                break
            time.sleep(0.05)
        for _s in (getattr(self, "server", None), getattr(self, "userver", None)):
            if _s is None:
                continue
            try:
                asyncio.run_coroutine_threadsafe(_s.shutdown(), self.loop).result(timeout=3)
            except Exception:
                pass
        # fallback: make sure the event loop actually exits so the daemon
        # thread dies and the port is really released before a rebind.
        try:
            if self.loop and self.loop.is_running():
                self.loop.call_soon_threadsafe(self.loop.stop)
        except Exception:
            pass
        if self.thread is not None:
            self.thread.join(timeout=3)
        self.running = False
        self.up = set()


# ==========================================================================
# Inverter
# ==========================================================================
class Inverter:
    INV_WINDOWS_BY_FC = {
        3: INV_HR_READ_WINDOWS, 4: INV_IR_WINDOWS,
        6: INV_HR_WRITE_WINDOWS, 16: INV_HR_WRITE_WINDOWS,
    }

    def __init__(self, iid, host, port, rating_kw, serial, tmo=None,
                 startup=None, udp_cfg=None, strict=None,
                 kind="pv", bat_capacity_kwh=None, soc=0.46, scale36=False):
        self.iid = iid
        self.host = host
        self.port = port
        self.rating_kw = float(rating_kw)
        self.serial = int(serial)
        self.kind = kind if kind in ("pv", "bess") else "pv"
        # measurement scale factor: the REAL PV v1135 profile serves kW
        # channels x36 (the .ppc divides back); the battery unit's 1035
        # profile serves them plain. HOWEVER - the Culcairn HYC bench
        # (2026-07) displays the x36 PV values UNDIVIDED (302 MW for two
        # 4.2 MW units), i.e. that HYC does not apply the profile scale.
        # So the x36 behavior is a per-unit toggle (GUI button / --pv-x36),
        # DEFAULT OFF: plain kW is what such an HYC displays correctly and
        # keeps the registers 1:1 with the GUI. Turn it ON only to mimic
        # the raw wire image of the real v1135 Kodiak against an HYC that
        # applies the profile scale. BESS units are always plain (measured).
        self.scale36 = bool(scale36) if self.kind == "pv" else False
        self._msf = 36 if self.scale36 else 1
        # ---- battery model state (kind == "bess" only) ----
        # capture unit: 2680 kW with ~5.5 MWh usable (~2 h) -> default 2 h
        self.bat_capacity_kwh = (float(bat_capacity_kwh)
                                 if bat_capacity_kwh else self.rating_kw * 2.0)
        self.soc = min(1.0, max(0.0, float(soc)))     # 0..1 (capture: 46.0 %)
        self.bat_eta = BESS_ETA
        self._acwh_in_kwh = 193590.0    # lifetime charged energy (capture:
                                        # Cnt.TotAcWhIn 19359 x0.01 MWh)
        # ---- running counters ----
        # Kept as floats and only rounded when written, so a 200 ms tick's
        # worth of energy is not lost to integer truncation every time. Seeded
        # from the REAL_IR_STATIC image at the end of _seed() so they continue
        # from the captured unit's values instead of restarting at zero.
        self._optm_s = 0.0              # Cnt.TotOpTm    (s, powered)
        self._feedtm_s = 826459.0       # Cnt.TotFeedTm  (s, grid-feeding)
        self._acwh_out_kwh = 0.0        # Cnt.TotAcWhOut (lifetime)
        self._acwh_out_day_kwh = 0.0    # Cnt.AcWhOut    (today)

        # real Kodiak answers 0xFFFF (not 0) at every register it does not
        # implement - measured via full-surface census of the live unit
        self.ir = PermissiveBlock(fill=0xFFFF)
        self.hr = PermissiveBlock(fill=0xFFFF)
        self.udp_cfg = udp_cfg if udp_cfg is not None else {"silent_writes": True}
        self.strict = strict if strict is not None else {"on": True}
        # scratch blocks behind unit ids 1/2/3 (real unit answers 0xFFFF
        # there and silently absorbs the HYC's u2/u3 connect-time writes)
        self._alias_ir = PermissiveBlock(fill=0xFFFF)
        self._alias_hr = PermissiveBlock(fill=0xFFFF)
        self.ctx_tcp, self.ctx_udp = _dual_contexts(
            ir=self.ir, hr=self.hr, strict=self.strict,
            windows_by_fc=self.INV_WINDOWS_BY_FC,
            label=f"INV{iid}:{port}", unit_id=102, alias_units=(1, 2, 3),
            alias_blocks=(self._alias_ir, self._alias_hr))
        self.srv = DeviceServer(self.ctx_tcp, self.ctx_udp, host, port,
                                udp_cfg=self.udp_cfg)

        self.enabled = True
        # per-axis HYC tracking: P and Q can independently follow the HYC
        # setpoints or a manual GUI target (e.g. P manual @ 200 kW while Q
        # keeps tracking VArSpt)
        self.tracking_p = True
        self.tracking_q = True
        self.cap_kw = self.rating_kw

        # plant-shared config (dicts so GUI changes apply live to all units)
        self.tmo = tmo if tmo is not None else \
            {"enabled": True, "seconds": 60.0, "mode": 8712}
        self.startup = startup if startup is not None else {"seconds": 20.0}

        # error / state machine
        self.err_no = 0            # active Kodiak ErrNo (0 = none)
        self.err_sev = None        # 'RD' fault | 'YW' warning | None
        self.state = "startup"     # startup | gridfeed | stopped | fault
        self.phase_i = 0
        self.phase_t = 0.0
        self._fststop_active = False
        self.tmo_active = False
        self.last_hyc_write = None  # time of last setpoint write from HYC
        # ErrClr from the controller: the Modbus write lands in the pymodbus
        # server thread, so it only raises a flag here - tick() (sim thread)
        # does the actual clearing, like every other HYC-driven transition.
        # pending ErrClr write as (block, address, idle_value) - tick() does
        # the clearing so the state change happens on the sim thread
        self._errclr_req = None
        self.last_hyc_errclr = None   # time of the last accepted HYC ErrClr
        self._odd_w_seen = {}         # rate limiter for [hyc-write] logging
        self._hyc_run_cmd = False     # HYC has commanded run (FstStop = 1467)
        self._scs_seen = None         # last AuxCtl.SCSOpCmd logged
        self.scs_opcmd = None         # current AuxCtl.SCSOpCmd (None = unset)
        self.hr.on_write = self._on_hyc_write
        # The alias units (1/2/3) used to absorb every write in silence, which
        # is exactly where the SMA-profile "Acknowledge inverter error" lands
        # (unit 3, HR 8). Watch them too.
        self._alias_hr.on_write = self._on_alias_write

        self.dt = 0.2
        self.ramp_enabled = True
        self.rate_p = self.rating_kw / 10.0
        self.rate_q = self.rating_kw / 10.0
        self.p_now = self.q_now = 0.0
        self.p_tgt = self.q_tgt = 0.0
        self.p_tgt_eff = self.q_tgt_eff = 0.0
        self.noise_enabled = True
        self.noise_p = max(2.0, self.rating_kw * 0.001)
        self.noise_q = max(2.0, self.rating_kw * 0.001)

        self.v_term = 690.0
        self.f_hz = 50.0
        # last HYC commands seen on the wire (for GUI display)
        self.hyc_wspt_pct = 0.0     # WSpt   @40023, S16, FIX2 %
        self.hyc_varspt_pct = 0.0   # VArSpt @40022, S16, FIX2 %
        self.hyc_fststop = 0        # FstStop U32 enum (1508 or 40018)
        self.spt_src = None         # '1500-blk' | 'legacy 4xxxx' | None
        self._seed()

    # ---- seeding ----
    @property
    def tracking(self):
        """True only when BOTH axes follow the HYC (legacy single flag)."""
        return self.tracking_p and self.tracking_q

    @tracking.setter
    def tracking(self, on):
        self.tracking_p = self.tracking_q = bool(on)

    def _seed(self):
        # FLEX template reads IR up to 1123; mark the 4 extra s32 channels
        # (1116-1123) unsupported (-1) per SMA convention, like the real
        # firmware does for optional channels.
        for a in (1116, 1118, 1120, 1122):
            set_s32(self.ir, a, -1)
        set_s32(self.ir, H_SERNO, self.serial)
        self._set_opstt(OPSTT_GRIDFEED)
        set_s32(self.ir, H_KEYSW, 308)  # Rio.KeySw enum: 308 = On (real unit)
        set_s32(self.ir, H_ERRNO, 0)
        set_s32(self.ir, H_ERRSTT, ERRSTT_OK)
        set_u16(self.ir, H_SOC, 500)
        set_u16(self.ir, H_SOH, 1000)
        self._update_aval()
        set_s32(self.ir, H_VARAVL, 10000)
        set_s32(self.ir, H_VARAVL2, 10000)
        set_s32(self.ir, H_DCAMP, 0)
        set_s32(self.ir, H_DCVOLT, 13193)   # 1319.3 V - real unit
        # constant block image copied from the real 4.2 MW Kodiak so a
        # 1200x54 / 1000x116 block read validates like real hardware
        for _blk, _img in ((self.ir, REAL_IR_STATIC), (self.hr, REAL_HR_STATIC)):
            for _a, (_v, _k) in _img.items():
                if _k == "u16":
                    set_u16(_blk, _a, _v)
                else:
                    set_s32(_blk, _a, _v)
        set_s32(self.hr, HR_INVOPMOD, 309)
        set_s32(self.hr, HR_REMRDY, 308)
        # identity handshake: a real inverter reports its PPC profile
        # revision; the HYC reads this (HR 0 / HR 1228) to decide whether to
        # use profile addressing (1500-block) or legacy uniqueid fallback
        set_s32(self.hr, 0, PRF_REV)      # Modbusd.PPC.Prf.Rev
        set_s32(self.hr, 1228, PRF_REV)   # Modbusd.PPC.Prf.Rev (HYC group)
        set_s32(self.hr, 2, DEV_ID)       # Modbusd.Dev.Id = 287 (susyid)
        # PPC holding-group mode enums, exactly as the real unit answers
        # (census fc3@0x16): GriMng.VArMod / GriMng.WMod / QoDMod
        set_s32(self.hr, 6, 1072)
        set_s32(self.hr, 8, 1079)
        set_s32(self.hr, 12, 308)
        # persisted setpoint-block contents of the real unit (census
        # fc3@1500x21 / fc3@1530x15): FstStop idles at 1467 (not 0),
        # HzNomSpt 50.000 Hz, VolNomSpt 1.0000 pu, Dcs.SocMax 100.0 %.
        # set_internal leaves no HYC-activity stamp, so the setpoint-source
        # selection logic is unaffected until the HYC really writes.
        self.hr.set_internal(1500, [0, 0])            # VArSpt / WSpt
        self.hr.set_internal(1502, [0, 0, 0, 0, 0, 0])
        self.hr.set_internal(P_FSTSTOP, [0, 1467])    # FstStop idle enum
        self.hr.set_internal(1510, [0, 50000, 10000]) # HzNomSpt / VolNomSpt
        # Poi.DiffW/DiffVAr/Poi.Vol/WFwd/VArFwd (1513-1520): real unit
        # answers zeros there (census), not the unmapped-0xFFFF fill
        self.hr.set_internal(1513, [0] * 8)
        # HYBRID write group tail + P2G group: census-defined zeros
        self.hr.set_internal(1406, [0, 0])
        self.hr.set_internal(1700, [0, 0, 0])
        self.hr.set_internal(1530, [0, 0, 0, 0, 0, 1467, 0, 50000, 10000,
                                    1000, 0, 0, 381, 2, 0])
        self.hr.set_internal(W_FSTSTOP, [0, 0])       # legacy cells: defined
        self.hr.set_internal(W_VARSPT, [0])
        self.hr.set_internal(W_WSPT, [0])
        # ErrClr is a one-shot trigger and idles at 0 in BOTH conventions -
        # it must not read back as the 0xFFFF unmapped fill, or the very
        # first re-arm write would look like a fresh acknowledgement.
        self.hr.set_internal(W_ERRCLR, [0])
        # SMA-profile ErrClr channels on the alias units idle at 973 '---',
        # not at the 0xFFFF unmapped fill - otherwise the very first read or
        # re-arm write would look like an acknowledge command.
        for _a in (SMA_ERRCLR_HR, SMA_ERRCLR_PROERR_HR):
            set_s32(self._alias_hr, _a, SMA_ENUM_IDLE)
        # P2G mirror block (IR 2000+): the HYC never polls it on a PV plant,
        # but the real unit answers it - serve the census image verbatim so
        # any probe sees exactly what the real device answers.
        for _off, _w in enumerate(P2G_IR_2000_IMAGE):
            if _w != 0xFFFF:
                self.ir.set_internal(2000 + _off, [_w])
        self._seed_ratings()
        # Continue the counters from the captured unit's values (REAL_IR_STATIC
        # was applied above) rather than restarting them at zero.
        #   1036 Cnt.TotOpTm    U32 s
        #   1038 Cnt.AcWhOut    S32 scale=100 FIX2 MWh -> raw = kWh / 10
        #   1040 Cnt.TotAcWhOut S32 scale=100 FIX2 MWh -> raw = kWh / 10
        self._optm_s = float(get_u32(self.ir, H_TOTOPTM))
        self._acwh_out_day_kwh = get_s32(self.ir, H_ACWHOUT_DAY) * 10.0
        self._acwh_out_kwh = get_s32(self.ir, H_ACWHOUT_TOT) * 10.0
        if self.kind == "bess":
            self._seed_bess()
        self._write_measurements(0.0, 0.0)

    def _seed_bess(self):
        """Overlay the battery-unit register image (wire capture 2026-07)
        on top of the PV seed. Everything here was measured on the live
        battery Kodiak, not invented."""
        ir, hr = self.ir, self.hr
        # identity: battery unit reports profile revision 1035 (HR 0/1228)
        set_s32(hr, 0, BESS_PRF_REV)
        set_s32(hr, 1228, BESS_PRF_REV)
        # battery temperatures answer 0 on this unit (PV image seeded -1)
        for a in (H_BATTMP_AVG, H_BATTMP_MIN, H_BATTMP_MAX):
            set_s32(ir, a, 0)
        # lifetime charged-energy counter (x0.01 MWh)
        set_s32(ir, H_ACWHIN, int(round(self._acwh_in_kwh / 10.0)))
        # HR statics as answered by the real battery unit
        set_u16(hr, 1206, BESS_RISO_WARN)   # PvGnd.RisIsoWarnLim
        set_u16(hr, 1207, BESS_RISO_ERR)    # PvGnd.RisIsoErrLim
        set_s32(hr, 1230, BESS_SRCSEL)      # Bsc.SrcSel
        set_s32(hr, 1244, BESS_INVSTRMOD)   # Bsc.InvStrMod (HYC re-writes)
        set_s32(hr, 1246, BESS_BATWMOD)     # GriMng.BatWMinMod
        set_s32(hr, 1248, BESS_BATWMOD)     # GriMng.BatWMaxMod
        self._write_bess_regs()

    def _seed_ratings(self):
        if self.kind == "bess":
            # battery Kodiak (profile 1035, capture): PLAIN kW - no x36.
            # Measured on the 2680 kW unit: WRtg 2680, VArRtg 2070,
            # VARtg 3450, WSptMin/Max = -3450/+3450 (= -/+ VA rating).
            r = int(round(self.rating_kw))
            var = int(round(self.rating_kw * BESS_VAR_RATIO))
            va = int(round(self.rating_kw * BESS_VA_RATIO))
            set_s32(self.ir, H_WSPTMIN, -va)
            set_s32(self.ir, H_WSPTMAX, va)
            set_s32(self.hr, HR_WRTG, r)
            set_s32(self.hr, HR_VARRTG, var)
            set_s32(self.hr, HR_VARTG, va)
            set_s32(self.hr, 4, r)      # PPC holding-group mirrors
            set_s32(self.hr, 10, var)
            set_s32(self.hr, 14, va)
            return
        # v1135 firmware serves the rating/setpoint-limit channels
        # pre-multiplied x36 (real 4200 kW unit answers 151200; the v1135
        # .ppc carries scale="36" to divide back). Measured on the wire AND
        # in the census. With the x36 toggle OFF, serve plain kW instead
        # (for HYCs that do not apply the profile scale - see __init__).
        r36 = int(round(self.rating_kw)) * (36 if self.scale36 else 1)
        # NOTE: Bsc.WInMax/WOutMax/VArOxMax/VArUxMax are battery channels -
        # the real PV Kodiak answers -1 (seeded in REAL_IR_STATIC), so they
        # are no longer derived from the rating here.
        set_s32(self.ir, H_WSPTMIN, 0)      # real PV unit reports 0, not -WRtg
        set_s32(self.ir, H_WSPTMAX, r36)
        set_s32(self.hr, HR_WRTG, r36)
        set_s32(self.hr, HR_VARRTG, r36)
        set_s32(self.hr, HR_VARTG, r36)
        # PPC holding-group mirrors (HR 4/10/14) - the real unit DOES serve
        # these (census fc3@0x16), same x36 values as the 1200-block copies
        set_s32(self.hr, 4, r36)    # WRtg
        set_s32(self.hr, 10, r36)   # VArRtg
        set_s32(self.hr, 14, r36)   # VARtg

    def _update_aval(self):
        """WAval (pu, FIX2): available active power as a fraction of WRtg.
        Mirrors the GUI 'cap' so a curtailed inverter reports reduced
        availability to the HYC, like a real derated unit would."""
        pu = 0.0 if self.rating_kw <= 0 else self.cap_kw / self.rating_kw
        raw = int(round(max(0.0, min(1.0, pu)) * 10000))
        set_s32(self.ir, H_WAVAL_P, raw)
        set_s32(self.ir, H_WAVAL2, raw)

    def set_cap(self, kw):
        self.cap_kw = max(0.0, min(float(kw), self.rating_kw))
        self._update_aval()

    # ---- BESS model ----
    def set_soc(self, pct):
        """GUI: set state of charge in %."""
        self.soc = min(1.0, max(0.0, float(pct) / 100.0))
        self._write_bess_regs()

    def set_bat_capacity(self, kwh):
        """GUI: set usable battery capacity in kWh."""
        self.bat_capacity_kwh = max(1.0, float(kwh))
        self._write_bess_regs()

    def _soc_op_limits(self):
        """SOC operating window 0..1: HYC-written AuxCtl.SOCOpMin/Max
        (HR 1402/1404, x0.01 %, observed fc16@1400 on the wire) if it has
        written them, else the full 0..100 % window."""
        lo, hi = 0.0, 1.0
        if self._last_w(HR_SOCOPMAX, 2) is not None:
            hi = get_s32(self.hr, HR_SOCOPMAX) / 10000.0
        if self._last_w(HR_SOCOPMIN, 2) is not None:
            lo = get_s32(self.hr, HR_SOCOPMIN) / 10000.0
        lo = min(1.0, max(0.0, lo))
        hi = min(1.0, max(lo, hi))
        return lo, hi

    def _bess_wspt_window(self):
        """Extra HYC power window from BatWSptMax/Min (%, FIX2) - the
        profile carries the pair in both the 1500 and 1530 write groups;
        whichever was written most recently wins (same rule as the
        setpoints).

        IMPORTANT (sim<->HYC wire capture 2026-07): the HYC writes the
        WHOLE 1500-block every cycle with BatWSptMax/Min = 0 when it
        dispatches via plain WSpt (GriMng.BatWMaxMod = 3). A written 0 is
        therefore 'field unused', NOT 'limit = 0 kW' - treating it as a
        limit froze the battery at 0 kW while the HYC commanded -15.88 %.
        Only nonzero values act as limits; never written -> no limit."""
        hi = lo = None
        amax, _ = self._pick_spt(HR_BATWMAX_A, 2, HR_BATWMAX_B, 2)
        if self._last_w(amax, 2) is not None:
            v = get_s32(self.hr, amax)
            if v != 0:
                hi = v / 100.0 / 100.0 * self.rating_kw
        amin, _ = self._pick_spt(HR_BATWMIN_A, 2, HR_BATWMIN_B, 2)
        if self._last_w(amin, 2) is not None:
            v = get_s32(self.hr, amin)
            if v != 0:
                lo = v / 100.0 / 100.0 * self.rating_kw
        return lo, hi

    def _write_bess_regs(self, running=True):
        """Serve the live battery channels exactly like the captured unit:
        Bat.SOCConn (x0.1 %), Bsc.WhIn/WhOutAvail (x0.1 kWh) against the
        SOC operating window, Bsc.WInMax/WOutMax (x0.1 kW) and
        Bsc.VArOxMax/VArUxMax (x0.1 kVAr) - which collapse to 0 when the
        unit is stopped (measured during the capture's shutdown), and the
        Cnt.TotAcWhIn charged-energy counter (x0.01 MWh)."""
        if self.kind != "bess":
            return
        ir = self.ir
        lo, hi = self._soc_op_limits()
        set_s32(ir, H_SOCCONN, int(round(self.soc * 1000)))
        wh_out = max(0.0, (self.soc - lo)) * self.bat_capacity_kwh
        wh_in = max(0.0, (hi - self.soc)) * self.bat_capacity_kwh
        set_s32(ir, H_WHOUTAVAIL, int(round(wh_out * 10)))
        set_s32(ir, H_WHINAVAIL, int(round(wh_in * 10)))
        if running:
            w_lim = int(round(self.rating_kw * 10))
            var_lim = int(round(self.rating_kw * BESS_VAR_RATIO * 10))
            # full/empty battery: the corresponding direction closes
            set_s32(ir, H_WINMAX, 0 if wh_in <= 0 else w_lim)
            set_s32(ir, H_WOUTMAX, 0 if wh_out <= 0 else w_lim)
        else:
            var_lim = 0
            set_s32(ir, H_WINMAX, 0)
            set_s32(ir, H_WOUTMAX, 0)
        set_s32(ir, H_VAROXMAX, var_lim)
        set_s32(ir, H_VARUXMAX, var_lim)
        set_s32(ir, H_ACWHIN, int(round(self._acwh_in_kwh / 10.0)))

    def _bess_integrate(self):
        """Integrate SOC from the AC power actually flowing this tick.
        Sign convention (capture): positive = discharge to grid, negative
        = charge. One-way efficiency applied on each direction."""
        e_kwh = self.p_now * self.dt / 3600.0
        if e_kwh >= 0.0:      # discharging: cells drain faster than AC out
            self.soc -= e_kwh / (self.bat_eta * self.bat_capacity_kwh)
        else:                 # charging: losses shrink what gets stored
            self._acwh_in_kwh += -e_kwh
            self.soc += (-e_kwh) * self.bat_eta / self.bat_capacity_kwh
        self.soc = min(1.0, max(0.0, self.soc))

    def _integrate_counters(self, feeding):
        """Advance the operating-time and energy counters one tick.

        The captured unit's Cnt.TotOpTm / Cnt.TotFeedTm / fan-hour counters all
        moved +1 per second on the wire, and Cnt.TotWhOut climbed with the
        energy actually delivered. The sim used to serve the frozen constants
        out of REAL_IR_STATIC, so a plant could feed 2 MW for hours and still
        report zero energy and zero uptime.

        Operating time runs whenever the unit is powered (including the connect
        walk); feed time and energy only while it is actually grid-feeding."""
        self._optm_s += self.dt
        if feeding:
            self._feedtm_s += self.dt
            e_kwh = self.p_now * self.dt / 3600.0
            if e_kwh > 0.0:
                self._acwh_out_kwh += e_kwh
                self._acwh_out_day_kwh += e_kwh
            # charging energy (BESS) is integrated by _bess_integrate() into
            # _acwh_in_kwh, which _write_bess_regs() serves at Cnt.TotAcWhIn
        set_s32(self.ir, H_TOTOPTM, int(self._optm_s))
        set_s32(self.ir, H_ACWHOUT_DAY, int(round(self._acwh_out_day_kwh / 10.0)))
        set_s32(self.ir, H_ACWHOUT_TOT, int(round(self._acwh_out_kwh / 10.0)))

    def _set_opstt(self, code):
        # the battery unit reports 1392 when stopped (capture); 3526
        # while feeding is common to both kinds
        if self.kind == "bess" and int(code) == OPSTT_STOP:
            code = OPSTT_STOP_BESS
        set_s32(self.ir, H_OPSTT, int(code))

    def _write_measurements(self, p_kw, q_kvar):
        # v1135 firmware serves the kW/kVAr/kVA/A channels x36 (profile
        # scale="36" divides back). Applies to 1012/1014/1016, the 20 ms
        # mirrors 1042/1044 (FIX3: additional x1000), DcMs.TotAmp 1034 and
        # the PPC input-group mirrors below.
        # (BESS units run profile 1035, which serves these channels PLAIN -
        # self._msf is 1 there and 36 on PV, exactly as measured on both
        # real units.)
        s_kva = math.hypot(p_kw, q_kvar)
        sf = self._msf
        set_s32(self.ir, H_W, p_kw * sf)
        set_s32(self.ir, H_VAR, q_kvar * sf)
        set_s32(self.ir, H_VA, s_kva * sf)
        set_s32(self.ir, 1042, p_kw * sf * 1000)   # InvMs.TotW  20ms (FIX3)
        set_s32(self.ir, 1044, q_kvar * sf * 1000) # InvMs.TotVAr 20ms (FIX3)
        # ---- DC side ----
        # DcMs.TotWatt (uid 603) is scale=10 / FIX1 in kW, so the raw value is
        # kW x 10. It used to be written as W x 10, i.e. 1000x too large - a
        # 2 MW dispatch was served to the HYC as 2 020 460 kW (2 GW).
        # DcMs.TotAmp then follows from DC power and DC voltage instead of
        # sitting at 0 A while the unit feeds megawatts.
        p_dc_kw = p_kw / INV_ETA if p_kw >= 0.0 else p_kw * INV_ETA
        set_s32(self.ir, H_DCWATT, int(round(p_dc_kw * 10)))
        v_dc = get_s32(self.ir, H_DCVOLT) / 10.0
        set_s32(self.ir, H_DCAMP,
                int(round(p_dc_kw * 1000.0 / v_dc)) if v_dc > 1.0 else 0)
        raw_v = int(round(self.v_term * 10))
        for a in (H_VOLT_AB, H_VOLT_BC, H_VOLT_CA):
            set_s32(self.ir, a, raw_v)
        set_s32(self.ir, H_FREQ, int(round(self.f_hz * 100)))
        self._write_ppc_mirrors(p_kw, q_kvar, s_kva, raw_v)
        if self.kind == "bess":
            self._write_bess_regs(running=(self.enabled
                                           and self.state == "gridfeed"))

    def _write_ppc_mirrors(self, p_kw, q_kvar, s_kva, raw_v):
        """PPC input group (IR 0-81 + 360): the real unit serves this whole
        group (census); previous sim versions left it unseeded. Values
        mirror the HYC-group channels with the group's own scaling."""
        ir = self.ir
        # energy/time counters: mirror the HYC-group counter cells
        ir.set_internal(0, ir.get_internal(1040, 2))   # Cnt.TotAcWhOut
        ir.set_internal(2, ir.get_internal(1038, 2))   # Cnt.AcWhOut
        base_optm = get_u32(ir, 1036)
        set_s32(ir, 4, base_optm)                      # Cnt.TotOpTm
        set_s32(ir, 6, int(self._feedtm_s))            # Cnt.TotFeedTm
        sf = self._msf
        set_s32(ir, 32, self.err_no)                   # ErrNoSma
        set_s32(ir, 34, self.q_tgt_eff * sf)           # VArSpt   (x36 on PV)
        set_s32(ir, 36, self.p_tgt_eff * sf)           # WSpt     (x36 on PV)
        set_s32(ir, 50, get_s32(ir, H_DCAMP) * sf)     # DcMs.Amp.Stk.Sum
        for a in (52, 54, 56):
            set_s32(ir, a, raw_v)                      # GriMs.V.Phs*
        set_s32(ir, 58, int(round(self.f_hz * 100)))   # GriMs.Hz
        set_s32(ir, 60, q_kvar * sf)                   # InvMs.TotVAr
        set_s32(ir, 62, s_kva * sf)                    # InvMs.TotVA
        set_s32(ir, 70, int(time.time()))              # Modbusd.Sys.Tm ticks
        ir.set_internal(72, ir.get_internal(H_KEYSW, 2))
        ir.set_internal(74, ir.get_internal(H_OPSTT, 2))
        ir.set_internal(76, ir.get_internal(H_ERRSTT, 2))
        ir.set_internal(78, ir.get_internal(H_DCVOLT, 2))
        set_s32(ir, 80, p_kw * sf)                     # InvMs.TotW
        set_s32(ir, 360, 0)                            # Cnt.GriForm.OvAmp

    # ---- runtime parameter setters ----
    def set_scale36(self, on):
        """Flip the PV x36 register scaling live: re-seeds ratings and
        rewrites the measurement mirrors in the new scale."""
        if self.kind != "pv":
            return
        self.scale36 = bool(on)
        self._msf = 36 if self.scale36 else 1
        self._seed_ratings()
        self._write_measurements(self.p_now, self.q_now)

    def set_rating(self, kw):
        old = self.rating_kw
        self.rating_kw = max(1.0, float(kw))
        # Keep the cap's MEANING (its fraction of the rating) instead of its
        # absolute kW: raising the rating used to leave the old, lower cap in
        # place, so the unit silently refused to follow the HYC and only the
        # WAval register hinted at why.
        frac = 1.0 if old <= 0 else min(1.0, self.cap_kw / old)
        self.cap_kw = min(self.rating_kw, self.rating_kw * frac)
        self._seed_ratings()
        self._update_aval()

    def set_serial(self, serial):
        self.serial = int(serial)
        set_s32(self.ir, H_SERNO, self.serial)

    def set_port(self, new_port):
        """Rebind the server to a new port (drops + reopens the socket).
        Returns False if the new port could not be served."""
        was = self.srv.running
        self.srv.stop()
        self.port = int(new_port)
        self.srv = DeviceServer(self.ctx_tcp, self.ctx_udp, self.host, self.port,
                                udp_cfg=self.udp_cfg)
        if was and self.enabled:
            return self.srv.start()
        return True

    def clear_fststop(self):
        """Reset the HYC fast-stop latch (a real inverter clears FstStop when
        it is restarted after a fast stop). If the HYC is still actively
        commanding the stop it will simply re-write 1749 within a poll cycle,
        which the GUI makes visible."""
        self.hr.set_internal(W_FSTSTOP, [0, 0])  # internal: don't feed watchdog
        self.hr.set_internal(P_FSTSTOP, [0, 0])
        self.hyc_fststop = 0

    # ---- Kodiak error handling ----
    def _write_err_regs(self):
        code = self.err_no
        stt = ERRSTT_OK if not code else \
            (ERRSTT_FAULT if self.err_sev == "RD" else ERRSTT_WARN)
        set_s32(self.ir, H_ERRNO, code)
        set_s32(self.ir, H_ERRSTT, stt)
        set_s32(self.ir, 32, code)                       # PPC ErrNoSma mirror
        self.ir.set_internal(76, self.ir.get_internal(H_ERRSTT, 2))

    def raise_error(self, code):
        """Raise a Kodiak ErrNo. RD = fault: trips the inverter until ErrClr.
        YW = warning: reported, unit keeps running. A fault is never
        downgraded by a later warning."""
        code = int(code)
        tag, desc, sev = KODIAK_ERRORS.get(code, ("?", "unknown", "RD"))
        if self.err_sev == "RD" and sev == "YW":
            return
        self.err_no, self.err_sev = code, sev
        self._write_err_regs()

    def clear_error(self):
        """ErrClr: acknowledge the active error. Clearing a fault sends the
        unit back through the startup/connect sequence, like real hardware."""
        was_fault = self.err_sev == "RD"
        self.err_no, self.err_sev = 0, None
        self._write_err_regs()
        if was_fault:
            self._begin_startup()

    def _begin_startup(self):
        self.state = "startup"
        self.phase_i = 0
        self.phase_t = 0.0

    def _on_hyc_write(self, address, count):
        """Called on every Modbus write from the HYC; feed the grid-management
        comms watchdog when either setpoint block is hit (profile 1500-1512
        or the legacy raw block 40018..40023), and latch an error
        acknowledgement when the controller pokes the ErrClr channel."""
        end = address + count
        if (address <= W_WSPT and end > W_FSTSTOP) or \
                (address <= 1520 and end > 1500):
            self.last_hyc_write = time.time()
        else:
            # NOT the cyclic setpoint block: on a real plant these are rare
            # (connect-time parameters, and operator-triggered commands such
            # as the error acknowledgement). Log them so an unexpected
            # controller firmware reveals which register it actually uses.
            self._log_odd_write(address, count)
        for a in (P_ERRCLR, W_ERRCLR):   # PPC/legacy channels are U16
            if address <= a < end:
                self._errclr_req = (self.hr, a, 0, 1)
                break

    def _on_alias_write(self, address, count):
        """Write to one of the scratch alias units (1/2/3). The real unit
        tolerates these, but unit 3 / HR 8 is the SMA-profile ErrClr channel
        the HYC uses to acknowledge an inverter error, so it is acted on
        here instead of being swallowed. Everything else is just logged."""
        end = address + count
        self._log_odd_write(address, count, blk=self._alias_hr, unit="alias")
        for a in (SMA_ERRCLR_HR, SMA_ERRCLR_PROERR_HR):   # S32 ENUM channels
            if address < a + 2 and end > a:
                self._errclr_req = (self._alias_hr, a, SMA_ENUM_IDLE, 2)
                break
        # The legacy uniqueid setpoints (40018 FstStop, 40022 VArSpt,
        # 40023 WSpt) are written by some HYC firmware on unit id 2 or 3
        # rather than 102. They used to land here and be discarded, so a
        # controller using that convention could not stop or dispatch the
        # unit at all. Mirror them into the real block so the normal
        # setpoint-source selection picks them up, transport stamp and all.
        if end > W_FSTSTOP and address <= W_WSPT:
            lo, hi = max(address, W_FSTSTOP), min(end, W_WSPT + 1)
            self.hr.hyc_set(lo, self._alias_hr.get_internal(lo, hi - lo),
                            self._alias_hr.last_proto)

    def _log_odd_write(self, address, count, blk=None, unit="102"):
        """Console note about an HYC write outside the cyclic setpoint block.

        These are the "only to be sent for changes" groups, i.e. exactly where
        an operator-triggered command like the error acknowledgement lands.
        Logged when the VALUE changes (plus a 60 s heartbeat per request
        shape), so a controller that writes HR 1400 cyclically does not flood
        the console but a genuine command still shows up immediately."""
        blk = self.hr if blk is None else blk
        key = (unit, address, count)
        vals = tuple(blk.get_internal(address, min(count, 8)))
        now = time.time()
        last_vals, last_ts = self._odd_w_seen.get(key, (None, 0.0))
        if vals == last_vals and now - last_ts < 60.0:
            return
        self._odd_w_seen[key] = (vals, now)
        name = (SMA_HR_NAMES if unit == "alias" else INV_HR_NAMES).get(
            address, ("?", 0, ""))[0]
        print(f"[hyc-write] INV{self.iid}:{self.port} unit {unit} "
              f"HR@{address} x{count} ({name}) = {list(vals)}"
              f"{' ...' if count > 8 else ''}")

    def _take_errclr(self):
        """Consume a pending controller ErrClr. The channel is a one-shot
        trigger: a non-zero value acknowledges the active error and is
        re-armed to 0, a written 0 just re-arms. Returns True if an error
        was actually acknowledged."""
        req, self._errclr_req = self._errclr_req, None
        if req is None:
            return False
        blk, addr, idle, nw = req
        # ENUM channels idle at 973 '---'; the FIX0 trigger channels idle at 0.
        # Anything else written there is an acknowledge command. ErrClr /
        # ErrClr.ProErr are S32 (nw=2, high word first); 1544 / the legacy
        # uniqueid cell are single registers.
        val = get_s32(blk, addr) if nw == 2 else blk.get_internal(addr, 1)[0]
        if val in (idle, 0):
            return False                          # re-armed, not a command
        if nw == 2:                               # one-shot: re-arm
            set_s32(blk, addr, idle)
        else:
            blk.set_internal(addr, [idle])
        self.last_hyc_errclr = time.time()
        had = self.err_no
        self.clear_error()
        src = {P_ERRCLR: "1544 Dcs.DcDcErrClr",
               W_ERRCLR: f"legacy uid {W_ERRCLR}",
               SMA_ERRCLR_HR: "unit 3 HR 8 ErrClr",
               SMA_ERRCLR_PROERR_HR: "unit 3 HR 20 ErrClr.ProErr"}.get(
                   addr, f"HR {addr}")
        print(f"[errclr] INV{self.iid}:{self.port} error acknowledged by the "
              f"controller via {src} = {val} (ErrNo {had or 'none'})")
        return bool(had)

    def _last_w(self, addr, n=1):
        """Most recent HYC write timestamp across n words (None = never)."""
        ts = None
        for i in range(n):
            e = self.hr.ext.get(addr + i)
            if e and e[1] is not None and (ts is None or e[1] > ts):
                ts = e[1]
        return ts

    def _pick_fststop(self):
        """FstStop source: profile 1508, HYBRID write group 1534, or the legacy
        uniqueid 40018 - whichever the HYC wrote most recently. The 1534 copy
        was previously ignored, so a controller using the HYBRID group could
        not stop the unit at all."""
        best, src, best_ts = W_FSTSTOP, None, None
        for addr, name in ((P_FSTSTOP, "1500-blk"), (P_FSTSTOP_B, "1530-blk"),
                           (W_FSTSTOP, "legacy 4xxxx")):
            ts = self._last_w(addr, 2)
            if ts is not None and (best_ts is None or ts >= best_ts):
                best, src, best_ts = addr, name, ts
        return best, src

    def _pick_spt(self, prof_addr, prof_n, leg_addr, leg_n):
        """Choose profile (1500-block) vs legacy (40018+) source for one
        setpoint signal: whichever the HYC wrote most recently wins.
        Neither ever written -> legacy address (reads 0)."""
        tp = self._last_w(prof_addr, prof_n)
        tl = self._last_w(leg_addr, leg_n)
        if tp is None and tl is None:
            return leg_addr, None
        if tl is None or (tp is not None and tp >= tl):
            return prof_addr, "1500-blk"
        return leg_addr, "legacy 4xxxx"

    def set_grid(self, v_term, f_hz):
        self.v_term = v_term
        self.f_hz = f_hz

    def current_pq(self):
        if not self.enabled:
            return 0.0, 0.0
        return self.p_now, self.q_now

    def tick(self):
        if not self.enabled:
            self.p_now = self.q_now = 0.0
            # A switched-off unit must not keep REPORTING its last state: the
            # GUI snapshot - and therefore any service recording - reads
            # state/OpStt from here, so an OFF inverter used to be logged as
            # OpStt 3526 "grid feeding" (at 0 kW) for the whole capture.
            if self.state != "stopped":
                self.state = "stopped"
                self._set_opstt(OPSTT_STOP)
                self._write_measurements(0.0, 0.0)
            return
        now = time.time()

        # ---- running counters ----
        # Operating time accrues whenever the unit is powered; feed time and
        # energy only while it is really grid-feeding, which is decided below.
        self._integrate_counters(feeding=(self.state == "gridfeed"))

        # ---- error acknowledgement from the controller (ErrClr @ 1544) ----
        # Must run BEFORE the state machine: clearing a red fault here lets
        # the same tick fall through into the connect sequence, exactly like
        # the GUI's own ErrClr button does.
        self._take_errclr()

        # ---- HYC command registers (profile 1500-block OR legacy 40018+,
        # whichever the HYC wrote most recently - per signal) ----
        # FstStop is a U32 in the profile, but be tolerant of an HYC that
        # writes the enum into a single 16-bit register (either word).
        # FstStop lives in THREE places (profile 1508, HYBRID group 1534, and
        # the legacy uniqueid 40018); the most recently written one wins.
        fst_addr, fst_src = self._pick_fststop()
        fst_hi, fst_lo = self.hr.get_internal(fst_addr, 2)
        fst_u32 = (fst_hi << 16) | fst_lo
        fststop = FSTSTOP_FULLSTOP in (fst_u32, fst_hi, fst_lo)
        self.hyc_fststop = fst_u32
        # A real inverter does not run itself: it stays at OpStt 381 (Stop)
        # until the controller commands run by writing FstStop (1467). The sim
        # used to boot straight into grid-feed with no HYC traffic at all.
        # Latched, so a later HYC restart does not need a fresh write.
        if self._last_w(fst_addr, 2) is not None and not fststop:
            self._hyc_run_cmd = True
        wspt_addr, wspt_src = self._pick_spt(P_WSPT, 1, W_WSPT, 1)
        varspt_addr, _ = self._pick_spt(P_VARSPT, 1, W_VARSPT, 1)
        self.spt_src = wspt_src or fst_src
        self.hyc_wspt_pct = get_s16(self.hr, wspt_addr) / 100.0
        self.hyc_varspt_pct = get_s16(self.hr, varspt_addr) / 100.0

        # fast-stop edges: latch -> Kodiak 9009 "Quick stop";
        # release -> auto-clear + restart sequence
        if fststop and not self._fststop_active:
            self._fststop_active = True
            self.raise_error(ERR_FSTSTOP)
        elif fststop and self.err_no == 0:
            # acknowledged (from the controller or the GUI) while the quick
            # stop is STILL commanded: the condition never went away, so a
            # real unit keeps reporting it - re-raise instead of showing a
            # stopped inverter with ErrNo 0.
            self.raise_error(ERR_FSTSTOP)
        elif not fststop and self._fststop_active:
            self._fststop_active = False
            if self.err_no == ERR_FSTSTOP:
                self.clear_error()

        # ---- grid-management comms watchdog (Kodiak 8712 / 8713) ----
        # Armed by the first HYC write to 40018..40023; a write resets it.
        if self.tmo_active and (not self.tmo["enabled"]
                                or self.last_hyc_write is None):
            # The watchdog was switched OFF (or disarmed) while its error was
            # still latched. Release it HERE - the recovery branch below only
            # runs while the watchdog is enabled, so an 8713 used to leave the
            # unit stuck at 0 kW in 'fault' forever, and resuming the HYC
            # setpoint writes did not help: only a manual ErrClr did.
            self.tmo_active = False
            if self.err_no in (8712, 8713):
                self.clear_error()
        elif self.tmo["enabled"] and self.last_hyc_write is not None:
            age = now - self.last_hyc_write
            if age > max(1.0, float(self.tmo["seconds"])):
                if not self.tmo_active:
                    self.tmo_active = True
                    self.raise_error(8713 if int(self.tmo["mode"]) == 8713
                                     else 8712)
            elif self.tmo_active:
                self.tmo_active = False
                if self.err_no in (8712, 8713):
                    self.clear_error()   # comms back -> ack, restart if 8713

        # ---- SCADA/SCS operation command (AuxCtl.SCSOpCmd, HR 1400) ----
        # Only acted on once the HYC has actually written it.
        scs = None
        if self._last_w(HR_AUXCTL_CMD, 2) is not None:
            scs = get_u32(self.hr, HR_AUXCTL_CMD)
            if scs != self._scs_seen:
                self._scs_seen = scs
                if scs != SCS_STOP and scs not in SCS_TO_OPSTT:
                    print(f"[scs] INV{self.iid}:{self.port} AuxCtl.SCSOpCmd = "
                          f"{scs} - no confirmed OpStt mapping, state machine "
                          f"keeps control")
        self.scs_opcmd = scs

        # ---- state machine ----
        faulted = self.err_sev == "RD"
        # Held at Stop until the controller commands run (FstStop = 1467), and
        # stopped again by SCSOpCmd = 381. Real hardware never self-starts.
        held = self.startup.get("hyc_gated", True) and not self._hyc_run_cmd
        if held or scs == SCS_STOP:
            self.state = "stopped"
            self.p_tgt_eff = self.q_tgt_eff = 0.0
            self.p_now = self.q_now = 0.0
            self._set_opstt(OPSTT_STOP)
            self._write_measurements(0.0, 0.0)
            return
        if fststop or faulted:
            # protection trip: output collapses immediately (no ramp-down)
            self.state = "stopped" if fststop else "fault"
            self.p_tgt_eff = self.q_tgt_eff = 0.0
            self.p_now = self.q_now = 0.0
            self._set_opstt(OPSTT_STOP)
            self._write_measurements(0.0, 0.0)
            return
        if self.state in ("stopped", "fault"):
            # released/cleared without an explicit restart -> connect sequence
            self._begin_startup()

        if self.state == "startup":
            total = max(0.0, float(self.startup["seconds"]))
            self.phase_t += self.dt
            if total <= 0 or self.phase_t >= total:
                self.state = "gridfeed"
            else:
                per = total / len(STARTUP_PHASES)
                self.phase_i = min(int(self.phase_t / per),
                                   len(STARTUP_PHASES) - 1)
                # report the connect phase itself, not a flat 381 Stop
                self._set_opstt(STARTUP_PHASES[self.phase_i][1])
                self.p_now = self.q_now = 0.0
                self._write_measurements(0.0, 0.0)
                return

        # ---- normal grid-feed operation (per-axis HYC / manual) ----
        if self.tracking_p:
            p_want = self.hyc_wspt_pct / 100.0 * self.rating_kw
        else:
            p_want = self.p_tgt
        if self.tracking_q:
            self.q_tgt_eff = self.hyc_varspt_pct / 100.0 * self.rating_kw
        else:
            self.q_tgt_eff = self.q_tgt
        if self.kind == "bess":
            # symmetric window: charge (negative) down to -cap, discharge
            # (positive) up to +cap - further narrowed by an HYC-written
            # BatWSptMax/Min pair, then gated by the SOC operating window
            lo_w, hi_w = self._bess_wspt_window()
            hi = self.cap_kw if hi_w is None else min(self.cap_kw, hi_w)
            lo = -self.cap_kw if lo_w is None else max(-self.cap_kw, lo_w)
            p_want = min(max(p_want, lo), max(lo, hi))
            soc_lo, soc_hi = self._soc_op_limits()
            if self.soc >= soc_hi:
                p_want = max(p_want, 0.0)   # full: no more charging
            if self.soc <= soc_lo:
                p_want = min(p_want, 0.0)   # empty: no more discharging
            self.p_tgt_eff = p_want
        else:
            # A PV unit cannot SINK power, and it advertises WSptMin = 0 kW to
            # the HYC (IR 1112), so a negative WSpt has to clamp at zero. It
            # used to be followed straight down: WSpt -300 % drove a 5 MW unit
            # to -15 MW and took the plant sum and the POI meter negative.
            self.p_tgt_eff = min(max(p_want, 0.0), self.cap_kw)
        # Running OpStt follows the commanded SCS mode where SMA has confirmed
        # the mapping (Power Control -> GridFeed, the GFM modes -> Gridform);
        # anything unconfirmed falls back to plain GridFeed.
        self._set_opstt(SCS_TO_OPSTT.get(self.scs_opcmd, OPSTT_GRIDFEED))

        if self.ramp_enabled:
            self.p_now = _step_toward(self.p_now, self.p_tgt_eff, self.rate_p, self.dt)
            self.q_now = _step_toward(self.q_now, self.q_tgt_eff, self.rate_q, self.dt)
        else:
            self.p_now, self.q_now = self.p_tgt_eff, self.q_tgt_eff

        if self.kind == "bess":
            self._bess_integrate()

        if self.noise_enabled:
            p_rep = self.p_now + random.uniform(-self.noise_p, self.noise_p)
            q_rep = self.q_now + random.uniform(-self.noise_q, self.noise_q)
        else:
            p_rep, q_rep = self.p_now, self.q_now
        self._write_measurements(p_rep, q_rep)


# ==========================================================================
# Grid model
# ==========================================================================
class GridModel:
    def __init__(self, v_nom_ll, s_base_mva):
        self.v_nom_ll = float(v_nom_ll)
        self.s_base_mva = max(0.001, float(s_base_mva))
        self.scr = 5.0
        self.xr = 7.0
        self.v_grid_pu = 1.0
        self.loss_frac_full = 0.015
        self.loss_fixed_frac = 0.002
        self.qloss_frac_full = 0.030

    def compute(self, p_plant_kw, q_plant_kvar):
        s_base_kw = self.s_base_mva * 1000.0
        p_pu = p_plant_kw / s_base_kw
        q_pu = q_plant_kvar / s_base_kw
        z_pu = 1.0 / max(self.scr, 0.1)
        r_pu = z_pu / math.sqrt(1.0 + self.xr ** 2)
        x_pu = z_pu * self.xr / math.sqrt(1.0 + self.xr ** 2)
        v_poi_pu = self.v_grid_pu + p_pu * r_pu + q_pu * x_pu
        v_poi_v = v_poi_pu * self.v_nom_ll
        s_plant = math.hypot(p_plant_kw, q_plant_kvar)
        loading = s_plant / s_base_kw
        p_loss = self.loss_frac_full * s_base_kw * loading ** 2 \
            + self.loss_fixed_frac * s_base_kw
        q_loss = self.qloss_frac_full * s_base_kw * loading ** 2
        return {
            "v_poi_v": v_poi_v, "v_poi_pu": v_poi_pu,
            "p_poi_kw": p_plant_kw - p_loss, "q_poi_kvar": q_plant_kvar - q_loss,
            "p_loss_kw": p_loss, "q_loss_kvar": q_loss,
        }


# ==========================================================================
# Meter
# ==========================================================================
class Meter:
    MET_WINDOWS_BY_FC = {3: MET_WINDOWS, 4: MET_WINDOWS,
                         6: MET_WRITE_WINDOWS, 16: MET_WRITE_WINDOWS}

    def __init__(self, host, port, udp_cfg=None, strict=None):
        self.host, self.port = host, port
        self.udp_cfg = udp_cfg if udp_cfg is not None else {"silent_writes": True}
        self.strict = strict if strict is not None else {"on": True}
        self.reg = PermissiveBlock()
        self.ctx_tcp, self.ctx_udp = _dual_contexts(
            ir=self.reg, hr=self.reg, strict=self.strict,
            windows_by_fc=self.MET_WINDOWS_BY_FC,
            label=f"METER:{port}")
        self.srv = DeviceServer(self.ctx_tcp, self.ctx_udp, host, port,
                                udp_cfg=self.udp_cfg)
        self.last = {}
        self._seed_static()

    def _seed_static(self):
        """Identity/status/energy block 5066-5075 - constant, written once."""
        set_s32(self.reg, 5066, 0)        # EgyConsTotPoi (U32, Wh consumed)
        set_s32(self.reg, 5068, 0)        # EgyDelTotPoi  (U32, Wh delivered)
        set_u16(self.reg, 5070, 51)       # GSP274SyncState (51 = closed/OK)
        # DigIo1/2: on hybrid plants these are typically the POI breaker
        # position feedback. 311 (Open) makes the HYC declare 8033 "POI
        # failure" and hold the plant in FstStop - seed 51 (Closed).
        set_u16(self.reg, 5071, 51)       # DigIo1Raw (51 = closed)
        set_u16(self.reg, 5072, 51)       # DigIo2Raw
        set_u16(self.reg, 5073, 308)      # Gsp274Ena (308 = on)
        set_s32(self.reg, 5074, 19127)    # ModelTag - Power Analyzer id

    def write(self, v_ll, f_hz, p_kw, q_kvar):
        v_ll = max(0.0, v_ll)
        v_ln = v_ll / SQRT3
        p_w, q_var = p_kw * 1000.0, q_kvar * 1000.0
        s_va = math.hypot(p_w, q_var)
        pf = (p_w / s_va) if s_va > 1e-6 else 1.0
        i_a = (s_va / (SQRT3 * v_ll)) if v_ll > 1e-6 else 0.0
        set_s32(self.reg, M_FAC_POI, f_hz * 1000.0)
        set_s32(self.reg, M_VTG_POI, v_ll)
        for a in (M_VTG_L1L2, M_VTG_L2L3, M_VTG_L3L1, M_VTG_AVG_LL):
            set_s32(self.reg, a, v_ll)
        for a in (M_VTG_L1, M_VTG_L2, M_VTG_L3, M_VTG_AVG_LN):
            set_s32(self.reg, a, v_ln)
        set_s32(self.reg, M_PWR_AT_POI, p_w)
        for a in (M_PWR_AT_L1, M_PWR_AT_L2, M_PWR_AT_L3):
            set_s32(self.reg, a, p_w / 3.0)
        set_s32(self.reg, M_PWR_RT_POI, q_var)
        for a in (M_PWR_RT_L1, M_PWR_RT_L2, M_PWR_RT_L3):
            set_s32(self.reg, a, q_var / 3.0)
        set_s32(self.reg, M_PWR_AP_POI, s_va)
        set_s32(self.reg, M_PF_POI, pf * 1000.0)
        for a in (M_IAC_L1, M_IAC_L2, M_IAC_L3):
            set_s32(self.reg, a, i_a * 1000.0)
        self.last = {"p_kw": p_kw, "q_kvar": q_kvar, "v_v": v_ll,
                     "f_hz": f_hz, "pf": pf, "i_a": i_a}


# ==========================================================================
# Register monitor helpers
# ==========================================================================
def _reg_decode(words, typ):
    if typ in ("s32", "u32"):
        u = (words[0] << 16) | words[1]
        if typ == "s32" and u >= 0x80000000:
            u -= 0x100000000
        return u
    v = words[0]
    if typ == "s16" and v >= 0x8000:
        v -= 0x10000
    return v


def _monitor_rows(block, names, now, write_addrs=frozenset(),
                  always=frozenset(), scale=None):
    """One row per register that is ACTIVE (seeded by the sim or touched by
    the HYC), plus the 'always' set (the HYC setpoint addresses - visible
    even before any traffic so they can be pre-written manually).
    Addresses that map onto a profile channel (the hardcoded name
    maps) collapse their word span into a single decoded row. Anything the
    HYC touches OUTSIDE the profile is emitted as a raw u16 row flagged
    unknown ('u': 1) - the GUI hides those unless 'show unknown' is on.
    Category 'c': 'w' = command/setpoint the HYC writes (known setpoint
    address, or any register the HYC has actually written on the wire),
    'r' = data the HYC reads (measurements / status / ratings).
    'p' = transports seen on that register: '' none, 'T' TCP, 'U' UDP, 'TU'.
    scale = {addr: divisor} for channels the device serves pre-multiplied
    (v1135 x36, FIX3 x1000): the VALUE column shows the engineering value
    (raw / divisor) so it matches the GUI; the raw-hex column still shows
    exactly what is on the wire, and the name is tagged 'wire xN'."""
    scale = scale or {}
    # map every covered word back to its profile register's start address
    start_of = {}
    for s, (_n, nw, _t) in names.items():
        for i in range(nw):
            start_of[s + i] = s
    active = set(block.values) | set(block.ext)
    starts = set(always)
    for a in active:
        starts.add(start_of.get(a, a))
    rows = []
    for a in sorted(starts):
        known = a in names
        name, nw, typ = names.get(a, ("", 1, "u16"))
        words = [block.values.get(a + i, 0) for i in range(nw)]
        r_ts = w_ts = None
        protos = set()
        for i in range(nw):
            e = block.ext.get(a + i)
            if e:
                if e[0] is not None and (r_ts is None or e[0] > r_ts):
                    r_ts = e[0]
                if e[1] is not None and (w_ts is None or e[1] > w_ts):
                    w_ts = e[1]
                protos |= e[2]
        is_cmd = w_ts is not None or \
            any((a + i) in write_addrs for i in range(nw))
        div = scale.get(a, 1) if known else 1
        val = _reg_decode(words, typ)
        if div != 1:
            val = val / div
            val = int(val) if float(val).is_integer() else round(val, 2)
            name = f"{name} \u00b7 wire \u00d7{div}"
        rows.append({
            "a": a, "n": name, "t": typ, "w": nw, "u": 0 if known else 1,
            "v": val, "s": div, "raw": words,
            "r": round(now - r_ts, 1) if r_ts is not None else None,
            "wr": round(now - w_ts, 1) if w_ts is not None else None,
            "p": "".join(sorted(protos)),
            "h": any((a + i) in block.holds for i in range(nw)),
            "c": "w" if is_cmd else "r",
        })
    return rows


def _reg_encode(value, typ):
    nw = 2 if typ in ("s32", "u32") else 1
    u = int(round(float(value)))
    if nw == 2:
        u &= 0xFFFFFFFF
        return [(u >> 16) & 0xFFFF, u & 0xFFFF]
    return [u & 0xFFFF]


# ==========================================================================
# Plant orchestrator
# ==========================================================================
# ==========================================================================
# Service recording ("record" button)
# --------------------------------------------------------------------------
# Recorder samples the live plant snapshot at a fixed interval and, on stop,
# builds an SMA "SCC" service bundle: a CSV2 .sma.csv fast-log (one column
# block per device - the POI meter plus every inverter) wrapped in a
# service_*.zip that mirrors the layout of a real SC30COM service dump
# (info.xml, lifecycle.xml, modbus_whitelist.xml, devices_*.spot,
# paramchanges.csv, messages, FL*_1.zip holding the CSV, eventlog_*.zip).
# The bundle is held in memory and served to the browser via /api/download.
# ==========================================================================
REC_METER_SERIAL = 3023056218   # synthetic SC30COM serial for the POI block
REC_DEVICE_TYPE = "KVA4200 V630 IEC V1500 (INV-SIM)"
REC_FW = "9.02.07.R"


def _tz_suffix():
    off = -(time.altzone if time.daylight and time.localtime().tm_isdst
            else time.timezone)
    h, rem = divmod(abs(off), 3600)
    sign = "+" if off >= 0 else "-"
    return f"UTC{sign}{h}" + (f":{rem // 60:02d}" if rem else "")


# uniqueids for recorded channels (real SMA ids where known, else synthetic)
_REC_UID = {
    "OpStt": 332, "InvMs.TotW": 402, "InvMs.TotVAr": 403, "InvMs.TotVA": 401,
    "InvMs.PF": 404, "WSpt": 320, "VArSpt": 321, "GriMs.V.PhsAB": 405,
    "GriMs.Hz": 607, "Bat.SOCConn": 6689,
    "GriMs.TotW.Poi": 90001, "GriMs.TotVAr.Poi": 90002,
    "GriMs.TotVA.Poi": 90003, "GriMs.PF.Poi": 90004, "GriMs.V.Poi": 90005,
    "GriMs.Hz.Poi": 90006, "GriMs.A.Poi": 90007,
}
_REC_UNIT = {
    "GriMs.TotW.Poi": "kW", "GriMs.TotVAr.Poi": "kvar",
    "GriMs.TotVA.Poi": "kVA", "GriMs.PF.Poi": "", "GriMs.V.Poi": "V",
    "GriMs.Hz.Poi": "Hz", "GriMs.A.Poi": "A",
    "OpStt": "", "InvMs.TotW": "kW", "InvMs.TotVAr": "kvar",
    "InvMs.TotVA": "kVA", "InvMs.PF": "", "GriMs.V.PhsAB": "V",
    "GriMs.Hz": "Hz", "WSpt": "kW", "VArSpt": "kvar", "Bat.SOCConn": "%",
}
_MODBUS_WHITELIST = (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
    '<ModbusWhitelist version="1.0" generator="INV-SIM">\n'
    '  <Entry ip="0.0.0.0" mask="0.0.0.0" access="read-write"/>\n'
    '</ModbusWhitelist>\n'
)


class Recorder:
    """Samples plant.snapshot and exports an SMA service bundle."""

    def __init__(self, plant):
        self.plant = plant
        self.lock = threading.Lock()
        self.active = False
        self.interval_ms = 1000
        self.thread = None
        self.rows = []          # (timestamp_str, ms, [values...])
        self.cols = []          # frozen column layout
        self.start_wall = None
        self.start_mono = None
        self.events = []        # eventlog entries captured while recording
        self.params = []        # paramchanges captured while recording
        self.last_zip = None    # (filename, bytes)
        self.last_summary = None

    # ---- column layout (frozen at record start) ----
    def _freeze_columns(self):
        snap = self.plant.snapshot or {}
        cols = []
        for sig in ("GriMs.TotW.Poi", "GriMs.TotVAr.Poi", "GriMs.TotVA.Poi",
                    "GriMs.PF.Poi", "GriMs.V.Poi", "GriMs.Hz.Poi",
                    "GriMs.A.Poi"):
            cols.append({"dev": "SC30COM", "serial": REC_METER_SERIAL,
                         "sig": sig, "src": ("meter", sig)})
        for inv in snap.get("inverters", []):
            sigs = ["OpStt", "InvMs.TotW", "InvMs.TotVAr", "InvMs.TotVA",
                    "InvMs.PF", "GriMs.V.PhsAB", "GriMs.Hz", "WSpt", "VArSpt"]
            if inv["kind"] == "bess":
                sigs.append("Bat.SOCConn")
            for sig in sigs:
                cols.append({"dev": "SC30CONT", "serial": inv["serial"],
                             "sig": sig, "src": ("inv", inv["id"], sig)})
        self.cols = cols

    def _sample(self):
        snap = self.plant.snapshot or {}
        met = snap.get("meter", {})
        freq = snap.get("plant", {}).get("freq", 50.0)
        invmap = {i["id"]: i for i in snap.get("inverters", [])}
        out = []
        for c in self.cols:
            src = c["src"]
            v = ""
            if src[0] == "meter":
                sig = src[1]
                p = met.get("p_kw", 0.0)
                q = met.get("q_kvar", 0.0)
                if sig == "GriMs.TotW.Poi":
                    v = p
                elif sig == "GriMs.TotVAr.Poi":
                    v = q
                elif sig == "GriMs.TotVA.Poi":
                    v = round(math.hypot(p, q), 1)
                elif sig == "GriMs.PF.Poi":
                    v = met.get("pf", 1.0)
                elif sig == "GriMs.V.Poi":
                    v = met.get("v_v", 0.0)
                elif sig == "GriMs.Hz.Poi":
                    v = met.get("f_hz", 0.0)
                elif sig == "GriMs.A.Poi":
                    v = met.get("i_a", 0.0)
            else:
                inv = invmap.get(src[1])
                sig = src[2]
                if inv is not None:
                    p = inv["p_kw"]
                    q = inv["q_kvar"]
                    rat = inv.get("rating_kw", 0) or 0
                    if sig == "OpStt":
                        v = inv["opstt"]
                    elif sig == "InvMs.TotW":
                        v = p
                    elif sig == "InvMs.TotVAr":
                        v = q
                    elif sig == "InvMs.TotVA":
                        v = round(math.hypot(p, q), 1)
                    elif sig == "InvMs.PF":
                        s = math.hypot(p, q)
                        v = round(p / s, 4) if s > 1e-6 else 1.0
                    elif sig == "GriMs.V.PhsAB":
                        v = inv["v_v"]
                    elif sig == "GriMs.Hz":
                        v = freq
                    elif sig == "WSpt":
                        v = round(inv.get("hyc_wspt", 0) / 100.0 * rat, 1)
                    elif sig == "VArSpt":
                        v = round(inv.get("hyc_varspt", 0) / 100.0 * rat, 1)
                    elif sig == "Bat.SOCConn":
                        v = inv["soc"]
            out.append(v)
        return out

    def _run(self):
        next_t = self.start_mono
        while self.active:
            now = time.time()
            lt = time.localtime(now)
            ts = time.strftime("%Y.%m.%d %H:%M:%S", lt)
            ms = int((now % 1) * 1000)
            vals = self._sample()
            with self.lock:
                self.rows.append((ts, ms, vals))
            next_t += self.interval_ms / 1000.0
            dt = next_t - time.monotonic()
            if dt > 0:
                time.sleep(dt)
            else:
                next_t = time.monotonic()

    # ---- control ----
    def start(self, interval_ms=1000):
        with self.lock:
            if self.active:
                return False
            self.interval_ms = max(50, int(interval_ms))
            self.rows = []
            self.events = []
            self.params = []
            self._freeze_columns()
            self.start_wall = time.time()
            self.start_mono = time.monotonic()
            self.active = True
        self.log_event("Info", "event", "SCC",
                       f"Data recording started ({self.interval_ms} ms, "
                       f"{len(self.cols)} channels)")
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        return True

    def stop(self):
        was = self.active
        self.active = False
        if self.thread:
            self.thread.join(timeout=2.0)
        if not was:
            return None
        self.log_event("Info", "event", "SCC", "Data recording stopped")
        return self._build_bundle()

    def status(self):
        with self.lock:
            el = (round(time.time() - self.start_wall, 1)
                  if self.start_wall and self.active else 0)
            return {
                "active": self.active, "interval_ms": self.interval_ms,
                "samples": len(self.rows), "elapsed_s": el,
                "channels": len(self.cols),
                "download": (self.last_zip[0] if self.last_zip else None),
                "summary": self.last_summary,
            }

    # ---- capture hooks (called from Plant._cmd while recording) ----
    def log_event(self, typ, status, cat, msg, level=0):
        now = time.time()
        self.events.append({
            "time": time.strftime("%Y.%m.%d %H:%M:%S", time.localtime(now)),
            "ms": int((now % 1) * 1000), "type": typ, "status": status,
            "source": f"287:{REC_METER_SERIAL}", "cat": cat, "id": 98000,
            "msg": msg, "level": level})

    def log_param(self, device, param, old, new):
        if not self.active:
            return
        self.params.append({
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
            "device": device, "param": param, "old": old, "new": new})

    # ---- formatting ----
    @staticmethod
    def _fmt(v):
        if v == "" or v is None:
            return ""
        if isinstance(v, bool):
            return "1" if v else "0"
        if isinstance(v, float):
            if not math.isfinite(v):
                return "0"
            s = f"{v:.4f}".rstrip("0").rstrip(".")
            return s if s not in ("", "-0") else "0"
        return str(v)

    def _build_sma_csv(self):
        CR = "\r\n"
        L = []
        L.append("Version CSV2|Tool SCC|Linebreaks CR/LF|Delimiter semicolon|"
                 "Decimalpoint dot|Precision dynamic|" + _tz_suffix())
        L.append("Device type;" + REC_DEVICE_TYPE)
        L.append("Serial number;" + str(REC_METER_SERIAL))
        L.append("Firmware version;" + REC_FW)
        L.append("")

        def hdr(getter):
            return ";;" + ";".join(getter(c) for c in self.cols)

        L.append(hdr(lambda c: c["dev"]))                       # device name
        L.append(hdr(lambda c: c["dev"]))                       # device class
        L.append(hdr(lambda c: str(c["serial"])))               # serial
        L.append(hdr(lambda c: str(_REC_UID.get(c["sig"], 90000))))  # uniqueid
        L.append(hdr(lambda c: c["sig"]))                       # signal name
        L.append("YYYY.MM.DD hh:mm:ss;ms;"
                 + ";".join(_REC_UNIT.get(c["sig"], "") for c in self.cols))
        with self.lock:
            rows = list(self.rows)
        for ts, ms, vals in rows:
            L.append(ts + ";" + str(ms) + ";"
                     + ";".join(self._fmt(v) for v in vals))
        return (CR.join(L) + CR).encode("utf-8-sig")

    def _build_eventlog(self):
        CR = "\r\n"
        L = ["Version 1 EventCSV1|Tool SCC|Linebreaks CR/LF|"
             "Delimiter semicolon|" + _tz_suffix(),
             "Device type;" + REC_DEVICE_TYPE,
             "Serial number;" + str(REC_METER_SERIAL),
             "Firmware version;" + REC_FW, "",
             "Time;ms;Type;Status;Source;Category;Event ID;Message;Level"]
        for e in self.events:
            L.append(";".join([e["time"], str(e["ms"]), e["type"],
                               e["status"], e["source"], e["cat"],
                               str(e["id"]), e["msg"], str(e["level"])]))
        return (CR.join(L) + CR).encode("utf-8-sig")

    def _build_paramchanges(self):
        CR = "\r\n"
        L = ["timestamp;device;parameter;old value;new value"]
        for p in self.params:
            L.append(";".join([p["ts"], str(p["device"]), str(p["param"]),
                               str(p["old"]), str(p["new"])]))
        return (CR.join(L) + CR).encode("utf-8-sig")

    def _build_spot(self):
        snap = self.plant.snapshot or {}
        met = snap.get("meter", {})
        x = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
             '<spotexport version="1.0">',
             '  <Timestamp>%s</Timestamp>'
             % time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
             '  <Firmware>',
             '    <Device Name="SC30COM"><SUSyID>287</SUSyID>'
             '<Serial>%d</Serial><FwVersion>09.02.08.R</FwVersion></Device>'
             % REC_METER_SERIAL,
             '  </Firmware>',
             '  <Device DeviceId="0" Name="SC30COM">',
             '    <Spot UniqueId="90001" Name="GriMs.TotW.Poi">%s</Spot>'
             % self._fmt(met.get("p_kw", 0.0)),
             '    <Spot UniqueId="90002" Name="GriMs.TotVAr.Poi">%s</Spot>'
             % self._fmt(met.get("q_kvar", 0.0)),
             '    <Spot UniqueId="90005" Name="GriMs.V.Poi">%s</Spot>'
             % self._fmt(met.get("v_v", 0.0)),
             '    <Spot UniqueId="90006" Name="GriMs.Hz.Poi">%s</Spot>'
             % self._fmt(met.get("f_hz", 0.0)),
             '  </Device>']
        for inv in snap.get("inverters", []):
            nm = "BESS" if inv["kind"] == "bess" else "SC30CONT"
            x.append('  <Device DeviceId="%d" Name="%s">' % (inv["id"], nm))
            x.append('    <Spot UniqueId="332" Name="OpStt">%s</Spot>'
                     % self._fmt(inv["opstt"]))
            x.append('    <Spot UniqueId="402" Name="InvMs.TotW">%s</Spot>'
                     % self._fmt(inv["p_kw"]))
            x.append('    <Spot UniqueId="403" Name="InvMs.TotVAr">%s</Spot>'
                     % self._fmt(inv["q_kvar"]))
            x.append('    <Spot UniqueId="320" Name="WSpt">%s</Spot>'
                     % self._fmt(inv.get("hyc_wspt", 0)))
            if inv["kind"] == "bess":
                x.append('    <Spot UniqueId="6689" Name="Bat.SOCConn">'
                         '%s</Spot>' % self._fmt(inv["soc"]))
            x.append('  </Device>')
        x.append('</spotexport>')
        return ("\n".join(x) + "\n").encode("utf-8")

    def _build_info_xml(self):
        n = len(self.plant.inverters)
        x = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
             '<serviceinfo version="1.0" generator="INV-SIM">',
             '  <Created>%s</Created>'
             % time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
             '  <Plant name="Inverter Simulator">',
             '    <PrimaryDevice type="SC30COM" serial="%d" fw="%s"/>'
             % (REC_METER_SERIAL, REC_FW),
             '    <Devices count="%d"/>' % (n + 1),
             '    <Recording interval_ms="%d" samples="%d" channels="%d"/>'
             % (self.interval_ms, len(self.rows), len(self.cols)),
             '  </Plant>',
             '</serviceinfo>']
        return ("\n".join(x) + "\n").encode("utf-8")

    def _build_lifecycle_xml(self):
        x = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
             '<lifecycle version="1.0" generator="INV-SIM">',
             '  <Device serial="%d" type="SC30COM">' % REC_METER_SERIAL,
             '    <Event ts="%s" kind="service-dump"/>'
             % time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
             '  </Device>',
             '</lifecycle>']
        return ("\n".join(x) + "\n").encode("utf-8")

    def _build_messages(self):
        lt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        return (f"{lt} SCC service dump generated by INV-SIM\n"
                f"{lt} recording: {len(self.rows)} samples, "
                f"{len(self.cols)} channels @ {self.interval_ms} ms\n"
                ).encode("utf-8")

    def _build_bundle(self):
        base = self.start_wall or time.time()
        lt = time.localtime(base)
        et = time.localtime()
        datestr = time.strftime("%y%m%d", lt)       # e.g. 260722
        stampd = time.strftime("%Y%m%d", lt)
        stampt = time.strftime("%H%M", et)
        csv_name = f"{datestr}_{self.interval_ms}ms_1.sma.csv"
        csv_bytes = self._build_sma_csv()

        fl = io.BytesIO()
        with zipfile.ZipFile(fl, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr(csv_name, csv_bytes)

        ev_csv = (f"eventlog_developer_{time.strftime('%Y-%m-%d', lt)}"
                  f"_{time.strftime('%Y-%m-%d', et)}.csv")
        evz = io.BytesIO()
        with zipfile.ZipFile(evz, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr(ev_csv, self._build_eventlog())

        out = io.BytesIO()
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("info.xml", self._build_info_xml())
            z.writestr("lifecycle.xml", self._build_lifecycle_xml())
            z.writestr("modbus_whitelist.xml", _MODBUS_WHITELIST.encode())
            z.writestr("paramchanges.csv", self._build_paramchanges())
            z.writestr(f"devices_{stampd}_{stampt}.spot", self._build_spot())
            z.writestr("messages", self._build_messages())
            z.writestr(f"FL{datestr}_1.zip", fl.getvalue())
            z.writestr(ev_csv[:-4] + ".zip", evz.getvalue())

        fn = f"service_SC30COM_{REC_METER_SERIAL}_{stampd}_{stampt}.zip"
        data = out.getvalue()
        self.last_zip = (fn, data)
        self.last_summary = {"file": fn, "samples": len(self.rows),
                             "channels": len(self.cols), "csv": csv_name,
                             "bytes": len(data)}
        return self.last_summary


class Plant:
    def __init__(self, host, n_inv, base_port, meter_port, rating_kw, v_nom_ll,
                 web_port=None, n_bess=0, bess_capacity_kwh=None,
                 pv_scale36=False, tick_ms=200):
        self.host = host
        self.base_port = base_port
        self.meter_port = meter_port
        self.default_rating = rating_kw
        self.freq = 50.0
        self.inverters = []
        # reserve the web port too, so auto-allocation can never collide
        self.used_ports = {meter_port} | ({web_port} if web_port else set())
        # meter manual override: when enabled, GUI values are written to the
        # meter registers instead of the grid-model result
        self.met_ovr = {"enabled": False, "p_kw": 0.0, "q_kvar": 0.0,
                        "v_v": float(v_nom_ll), "f_hz": 50.0}
        # plant-wide inverter behavior (shared dicts – live for all units)
        self.tmo = {"enabled": True, "seconds": 60.0, "mode": 8712}
        # hyc_gated: real hardware never starts itself - it waits for the
        # controller to command run (FstStop = 1467). Turn OFF to run the sim
        # standalone (bench demo, MoMo capture) with no controller attached.
        self.startup_cfg = {"seconds": 20.0, "hyc_gated": True}
        # UDP write replies: real Kodiak = fire-and-forget (silent)
        self.udp_cfg = {"silent_writes": True}
        # strict addressing: off-profile requests get IllegalDataAddress
        # census 2026-07-16: the REAL unit answers 0xFFFF data even far
        # outside the profile windows (fc3@3000, fc4@1300, ...) - it never
        # raises IllegalDataAddress for an allowed client. Permissive is the
        # measured behavior; strict stays available as a GUI test toggle.
        self.strict = {"on": False}
        # default x36 register scaling for newly created PV units
        self.pv_scale36 = bool(pv_scale36)
        # plant-wide PV irradiance (% of rating): broadcast to every PV
        # unit's WAval cap - limits output even while HYC-tracking
        self.irr_pct = 100.0
        self.next_id = 0
        self.lock = threading.Lock()
        self.meter = Meter(host, meter_port, udp_cfg=self.udp_cfg,
                           strict=self.strict)
        self.grid = GridModel(v_nom_ll, max(0.001, rating_kw * n_inv / 1000.0))
        self.tick_ms = int(tick_ms)
        self.dt = max(0.01, self.tick_ms / 1000.0)
        self._stop = False
        self.snapshot = {}
        # Shared-bench claim: purely informational, nothing in the sim
        # behaves differently. It exists so a colleague opening the GUI
        # can see the rig is already booked before they change a port.
        self.in_use = {"on": False, "who": "", "since": None}
        # One config file per instance, keyed on the web port, so several
        # people can run their own simulator on one box without clobbering
        # each other's saved rig. Overridable with --config.
        self._cfg_path = os.path.join(
            os.path.expanduser("~"), ".inverter_sim",
            f"config_{web_port}.json")
        _start_peer_watch()
        self.web_port = int(web_port or 0)   # used to reject self-peering
        self.fed = self._fed_defaults()
        self.peers = {}          # master: peer address -> live state
        self.recorder = Recorder(self)
        for _ in range(n_inv):
            self._make_inverter(rating_kw)
        for _ in range(n_bess):
            self._make_inverter(rating_kw, kind="bess",
                                bat_capacity_kwh=bess_capacity_kwh)

    # ---- fleet management ----
    def _alloc_port(self):
        p = self.base_port
        while p in self.used_ports:
            p += 1
        return p

    def _make_inverter(self, rating, serial=None, start=False, kind="pv",
                       bat_capacity_kwh=None, scale36=None, port=None,
                       iid=None):
        # an explicit port comes from a restored config; fall back to the
        # next free one if it is already taken by this process
        port = (int(port) if port and int(port) not in self.used_ports
                else self._alloc_port())
        # a restored config brings its own id so GUI selections and any
        # scripted /api/cmd calls keep pointing at the same inverter
        iid = self.next_id if iid is None else int(iid)
        self.next_id = max(self.next_id, iid + 1)
        if serial is None:
            serial = 1234500001 + iid
        inv = Inverter(iid, self.host, port, rating, serial,
                       tmo=self.tmo, startup=self.startup_cfg,
                       udp_cfg=self.udp_cfg, strict=self.strict,
                       kind=kind, bat_capacity_kwh=bat_capacity_kwh,
                       scale36=(self.pv_scale36 if scale36 is None
                                else bool(scale36)))
        inv.dt = self.dt
        self.inverters.append(inv)
        self.used_ports.add(port)
        if start:
            inv.srv.start()
        self._recompute_base()
        return inv

    # ------------------------------------------------------------------
    # Saved configuration
    # ------------------------------------------------------------------
    # Several people run their own copy of the simulator on one box (each
    # with its own --web-port / --base-port), so the config file defaults to
    # a per-instance name keyed on the web port. Save writes the whole rig -
    # fleet layout, per-inverter settings, grid model, system options - and
    # the file is re-applied automatically on the next start.
    CONFIG_VERSION = 1

    def export_config(self):
        invs = []
        for inv in self.inverters:
            invs.append({
                "id": inv.iid,
                "kind": inv.kind, "port": inv.port, "serial": inv.serial,
                "rating_kw": inv.rating_kw, "cap_kw": inv.cap_kw,
                "enabled": inv.enabled, "scale36": inv.scale36,
                "tracking_p": inv.tracking_p, "tracking_q": inv.tracking_q,
                "p_tgt": inv.p_tgt, "q_tgt": inv.q_tgt,
                "ramp_enabled": inv.ramp_enabled,
                "rate_p": inv.rate_p, "rate_q": inv.rate_q,
                "noise_enabled": inv.noise_enabled,
                "noise_p": inv.noise_p, "noise_q": inv.noise_q,
                "bat_capacity_kwh": inv.bat_capacity_kwh, "soc": inv.soc,
            })
        g = self.grid
        return {
            "version": self.CONFIG_VERSION,
            "saved": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
            "owner": self.in_use.get("who", ""),
            "in_use": bool(self.in_use.get("on")),
            "freq": self.freq, "irr_pct": self.irr_pct,
            "fed": dict(self.fed),
            "inverters": invs,
            "grid": {"v_nom_ll": g.v_nom_ll, "scr": g.scr, "xr": g.xr,
                     "v_grid_pu": g.v_grid_pu,
                     "loss_frac_full": g.loss_frac_full,
                     "loss_fixed_frac": g.loss_fixed_frac,
                     "qloss_frac_full": g.qloss_frac_full},
            "sys": {"startup_s": self.startup_cfg["seconds"],
                    "hyc_gated": self.startup_cfg.get("hyc_gated", True),
                    "tick_ms": self.tick_ms,
                    "tmo": dict(self.tmo),
                    "udp_silent": bool(self.udp_cfg.get("silent_writes")),
                    "strict": bool(self.strict.get("on"))},
        }

    def apply_config(self, cfg):
        """Rebuild the rig from a saved config. Returns (ok, message).

        Everything is parsed and coerced BEFORE the running fleet is touched.
        A hand-edited or truncated file used to raise part-way through, after
        the inverters had already been torn down - which left the plant empty
        and, at startup, stopped the simulator booting at all. Bad individual
        values are now skipped and reported; only a structurally wrong file is
        rejected outright, and then nothing has changed."""
        if not isinstance(cfg, dict) or not isinstance(cfg.get("inverters"), list):
            return False, "not a simulator config file"
        try:
            ver = int(cfg.get("version", 0))
        except (TypeError, ValueError):
            return False, "config version is not a number"
        if ver > self.CONFIG_VERSION:
            return False, f"config version {ver} is newer than this build"

        warn = []

        def num(d, key, default=None, cast=float):
            if key not in d or d[key] is None:
                return default
            try:
                return cast(d[key])
            except (TypeError, ValueError):
                warn.append(f"{key}={d[key]!r}")
                return default

        # ---------- parse (nothing is changed yet) ----------
        g_in = cfg.get("grid") if isinstance(cfg.get("grid"), dict) else {}
        grid_vals = {}
        for k in ("v_nom_ll", "scr", "xr", "v_grid_pu", "loss_frac_full",
                  "loss_fixed_frac", "qloss_frac_full"):
            v = num(g_in, k)
            if v is not None:
                grid_vals[k] = v

        s_in = cfg.get("sys") if isinstance(cfg.get("sys"), dict) else {}
        startup_s = num(s_in, "startup_s")
        tick_ms = num(s_in, "tick_ms", cast=int)
        freq = num(cfg, "freq")
        irr = num(cfg, "irr_pct")

        specs = []
        for d in cfg["inverters"]:
            if not isinstance(d, dict):
                warn.append("an inverter entry is not an object")
                continue
            specs.append({
                "id": num(d, "id", cast=int),
                "kind": d.get("kind") if d.get("kind") in ("pv", "bess") else "pv",
                "port": num(d, "port", cast=int),
                "serial": num(d, "serial", cast=int),
                "rating_kw": num(d, "rating_kw", self.default_rating),
                "cap_kw": num(d, "cap_kw"),
                "enabled": bool(d.get("enabled", True)),
                "scale36": d.get("scale36"),
                "tracking_p": bool(d.get("tracking_p", True)),
                "tracking_q": bool(d.get("tracking_q", True)),
                "p_tgt": num(d, "p_tgt", 0.0), "q_tgt": num(d, "q_tgt", 0.0),
                "ramp_enabled": bool(d.get("ramp_enabled", True)),
                "rate_p": num(d, "rate_p"), "rate_q": num(d, "rate_q"),
                "noise_enabled": bool(d.get("noise_enabled", True)),
                "noise_p": num(d, "noise_p"), "noise_q": num(d, "noise_q"),
                "bat_capacity_kwh": num(d, "bat_capacity_kwh"),
                "soc": num(d, "soc"),
            })

        # ---------- apply ----------
        for inv in list(self.inverters):
            inv.srv.stop()
            self.used_ports.discard(inv.port)
        self.inverters = []
        self.next_id = 0

        self.in_use["who"] = str(cfg.get("owner", "") or "")[:60]
        self.in_use["on"] = bool(cfg.get("in_use"))
        self.in_use["since"] = time.time() if self.in_use["on"] else None
        if freq is not None:
            self.freq = freq
        if irr is not None:
            self.irr_pct = irr
        for k, v in grid_vals.items():
            setattr(self.grid, k, v)
        if startup_s is not None:
            self.startup_cfg["seconds"] = max(0.0, startup_s)
        if "hyc_gated" in s_in:
            self.startup_cfg["hyc_gated"] = bool(s_in["hyc_gated"])
        if isinstance(s_in.get("tmo"), dict):
            self.tmo.update(s_in["tmo"])
        if "udp_silent" in s_in:
            self.udp_cfg["silent_writes"] = bool(s_in["udp_silent"])
        if "strict" in s_in:
            self.strict["on"] = bool(s_in["strict"])
        fed_in = cfg.get("fed")
        if isinstance(fed_in, dict):
            f = self._fed_defaults()
            if fed_in.get("mode") in self.FED_MODES:
                f["mode"] = fed_in["mode"]
            if isinstance(fed_in.get("peers"), list):
                f["peers"] = [str(x).strip() for x in fed_in["peers"]
                              if str(x).strip()][:self.FED_MAX_PEERS]
            f["master"] = str(fed_in.get("master", "") or "")[:60]
            for key in ("poll_hz", "timeout_s", "wgraflb_pu_s",
                        "vargraflb_pu_s"):
                v = num(fed_in, key)
                if v is not None:
                    f[key] = v
            self.fed = f
            self.peers = {}
            # the reloaded mode has to reach the meter as well, or a rig
            # reloaded as a satellite keeps serving a partial POI
            self._fed_apply_mode()
        if tick_ms:
            self.set_tick(tick_ms)

        made, failed = 0, []
        for d in specs:
            try:
                inv = self._make_inverter(
                    d["rating_kw"], serial=d["serial"], start=False,
                    kind=d["kind"], bat_capacity_kwh=d["bat_capacity_kwh"],
                    scale36=d["scale36"], port=d["port"], iid=d["id"])
                inv.set_cap(d["cap_kw"] if d["cap_kw"] is not None
                            else inv.rating_kw)
                inv.tracking_p, inv.tracking_q = d["tracking_p"], d["tracking_q"]
                inv.p_tgt, inv.q_tgt = d["p_tgt"], d["q_tgt"]
                inv.ramp_enabled = d["ramp_enabled"]
                if d["rate_p"] is not None:
                    inv.rate_p = d["rate_p"]
                if d["rate_q"] is not None:
                    inv.rate_q = d["rate_q"]
                inv.noise_enabled = d["noise_enabled"]
                if d["noise_p"] is not None:
                    inv.noise_p = d["noise_p"]
                if d["noise_q"] is not None:
                    inv.noise_q = d["noise_q"]
                if inv.kind == "bess" and d["soc"] is not None:
                    inv.set_soc(d["soc"] * 100.0)
                # An inverter saved in the OFF state must come back with its
                # Modbus port CLOSED, the same as switching it off in the GUI.
                inv.enabled = d["enabled"]
                if inv.enabled and not inv.srv.start():
                    failed.append(f"port {inv.port} would not bind")
                made += 1
            except Exception as e:
                failed.append(str(e))
        self._recompute_base()
        msg = f"restored {made} inverter(s)"
        if warn:
            msg += " - ignored bad values: " + ", ".join(warn[:4])
        if failed:
            msg += " - " + "; ".join(failed[:3])
        return True, msg

    def config_path(self):
        return self._cfg_path

    def save_config(self):
        try:
            d = os.path.dirname(self._cfg_path)
            if d:
                os.makedirs(d, exist_ok=True)
            tmp = self._cfg_path + ".tmp"
            with io.open(tmp, "w", encoding="utf-8") as fh:
                json.dump(self.export_config(), fh, indent=1)
            os.replace(tmp, self._cfg_path)     # atomic: never a half file
            return True, self._cfg_path
        except Exception as e:
            return False, str(e)

    def load_config(self, path=None):
        path = path or self._cfg_path
        try:
            with io.open(path, encoding="utf-8") as fh:
                cfg = json.load(fh)
        except FileNotFoundError:
            return False, f"no saved config at {path}"
        except Exception as e:
            return False, str(e)
        try:
            ok, msg = self.apply_config(cfg)
        except Exception as e:
            return False, f"config at {path} could not be applied: {e!r}"
        return ok, (f"{msg} from {path}" if ok else msg)

    # ------------------------------------------------------------------
    # Federated (multi-Pi) mode
    # ------------------------------------------------------------------
    # Some HYC firmware can only be given an inverter's IP and always talks to
    # port 502, so several inverters cannot share one host. The workaround is
    # one Pi per inverter, each serving its own IP:502, with ONE of them acting
    # as master: it runs the POI meter (meters can still be given a separate
    # port, so the meter stays on the master at its own port) and adds the
    # other Pis' output into the plant total.
    #
    # Off by default - a single Pi with per-inverter ports is still the normal
    # way to run this, and federation only earns its complexity on the older
    # firmware.
    #
    # The link reuses the simulator's own HTTP API on both ends: the master
    # GETs /api/state from each peer. Nothing new has to be installed on a
    # satellite - it is an ordinary simulator with its meter switched off.
    FED_MODES = ("off", "master", "satellite")
    FED_MAX_PEERS = 20

    def _fed_defaults(self):
        return {
            "mode": "off",
            "peers": [],              # master: ["172.17.1.31", "...:8080"]
            "master": "",             # satellite: master address, display only
            "poll_hz": 5.0,
            "timeout_s": 2.0,
            # Fallback gradients used when a peer goes quiet. A real plant
            # ramps a lost inverter back rather than dropping it, so the POI
            # shows a ramp instead of a cliff (SMA calls these WGraFlb /
            # VArGraFlb; they are controller-side parameters, expressed here
            # in pu/s of the lost unit's rating).
            "wgraflb_pu_s": 0.1,
            "vargraflb_pu_s": 0.1,
        }

    @staticmethod
    def _peer_url(peer, path):
        peer = str(peer).strip()
        if "://" in peer:
            peer = peer.split("://", 1)[1]
        host, _, port = peer.partition(":")
        return f"http://{host}:{port or 8080}{path}"

    def _is_self(self, peer):
        """True when `peer` points back at this instance's own web GUI."""
        addr = str(peer).strip().rstrip("/")
        if "://" in addr:
            addr = addr.split("://", 1)[1]
        host, _, port = addr.partition(":")
        try:
            port = int(port) if port else 8080
        except ValueError:
            return False
        if port != int(self.web_port or 0):
            return False
        local = {"127.0.0.1", "localhost", "0.0.0.0", "::1", self.host}
        try:
            local.add(socket.gethostbyname(socket.gethostname()))
        except OSError:
            pass
        return host in local

    def _fed_poll_once(self, peer):
        """Fetch one peer's state. Never raises."""
        try:
            req = urllib.request.Request(self._peer_url(peer, "/api/state"))
            with urllib.request.urlopen(req, timeout=1.5) as fh:
                return json.loads(fh.read().decode("utf-8"))
        except Exception:
            return None

    def _fed_loop(self):
        # One long-lived pool rather than a thread per peer per cycle: at 20
        # peers and 5 Hz that was 100 thread creations a second on a Pi, and a
        # slow DNS lookup outlived the join so the threads piled up.
        pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.FED_MAX_PEERS, thread_name_prefix="fedpoll")
        while not self._stop:
            try:
                self._fed_cycle(pool)
            except Exception as e:
                # A raise here used to kill the poll thread permanently:
                # federation stopped dead while the GUI kept showing the last
                # peer values until they aged out. Never let one cycle end it.
                print(f"[fed] poll cycle error: {e!r}")
                time.sleep(1.0)
        pool.shutdown(wait=False)

    def _fed_cycle(self, pool):
        fed = self.fed
        if fed.get("mode") != "master":
            time.sleep(0.5)
            return
        peers = list(fed.get("peers") or [])[:self.FED_MAX_PEERS]
        # Poll every peer AT THE SAME TIME. Sequentially, each unreachable
        # Pi cost a full socket timeout, so two or three dead ones pushed a
        # single cycle past the lost-timeout and the healthy peers were
        # flagged lost and ramped back too.
        futures = {peer: pool.submit(self._fed_poll_once, peer)
                   for peer in peers}
        results = {}
        for peer, fut in futures.items():
            try:
                results[peer] = fut.result(timeout=2.5)
            except Exception:
                results[peer] = None
        for peer in peers:
            state = results.get(peer)
            now = time.time()
            try:
                self._fed_absorb(peer, state, now)
            except Exception as e:
                # one Pi answering nonsense must not stop the others in
                # this cycle from being updated
                self._fed_warn(peer, e)
        time.sleep(max(0.05, 1.0 / max(0.2, float(fed.get("poll_hz", 5.0)))))

    def _fed_warn(self, peer, err):
        key = ("fedwarn", peer)
        now = time.time()
        if now - self._odd_w_seen.get(key, (None, 0.0))[1] < 30.0:
            return
        self._odd_w_seen[key] = (None, now)
        print(f"[fed] {peer}: unusable reply ({err!r}) - peer ignored")

    def _fed_absorb(self, peer, state, now):
        with self.lock:
            st = self.peers.setdefault(peer, {
                "p_kw": 0.0, "q_kvar": 0.0, "rating_kw": 0.0,
                "last_ok": None, "name": "", "inverters": [],
                "hold_p": 0.0, "hold_q": 0.0, "ramping": False,
            })
            prev_rating = st.get("rating_kw") or 0.0
            if state is None:
                return                   # staleness is judged in tick()
            invs = state.get("inverters") or []
            # Sum the peer's OWN inverters, never its plant total: a
            # master's plant total already includes ITS peers, so
            # reading that would double-count through any loop (most
            # easily a master listed as its own peer) and run away.
            # An inverter list only ever holds that Pi's own units.
            def _sum(key):
                total = 0.0
                for i in invs:
                    try:
                        total += float(i.get(key) or 0.0)
                    except (TypeError, ValueError):
                        pass        # ignore one bad channel, not the Pi
                return total
            st["p_kw"] = _sum("p_kw")
            st["q_kvar"] = _sum("q_kvar")
            st["rating_kw"] = _sum("rating_kw")
            st["name"] = ((state.get("in_use") or {}).get("who") or "")
            st["inverters"] = invs
            st["last_ok"] = now
            # a peer that answers again resumes from its live value
            st["ramping"] = False
            st["hold_p"], st["hold_q"] = st["p_kw"], st["q_kvar"]
            if st["rating_kw"] != prev_rating:
                # the plant just got bigger/smaller - the loss model and
                # voltage rise are per-unit of the whole plant
                self._recompute_base()

    def _fed_contribution(self, dt):
        """(p_kw, q_kvar) the remote Pis add to the plant this tick.

        A peer that has gone quiet is not dropped: its last output is ramped
        back toward zero at the fallback gradient, so the POI meter shows the
        same ramp a real plant would when it loses an inverter."""
        if self.fed.get("mode") != "master":
            return 0.0, 0.0
        timeout = max(0.2, float(self.fed.get("timeout_s", 2.0)))
        now = time.time()
        p_tot = q_tot = 0.0
        for peer in list(self.fed.get("peers") or [])[:self.FED_MAX_PEERS]:
            st = self.peers.get(peer)
            if st is None:
                continue
            last = st.get("last_ok")
            fresh = last is not None and (now - last) <= timeout
            if fresh:
                st["hold_p"], st["hold_q"] = st["p_kw"], st["q_kvar"]
                st["ramping"] = False
            else:
                st["ramping"] = last is not None
                rating = max(1.0, float(st.get("rating_kw") or 0.0))
                st["hold_p"] = _step_toward(
                    st["hold_p"], 0.0,
                    float(self.fed.get("wgraflb_pu_s", 0.1)) * rating, dt)
                st["hold_q"] = _step_toward(
                    st["hold_q"], 0.0,
                    float(self.fed.get("vargraflb_pu_s", 0.1)) * rating, dt)
            p_tot += st["hold_p"]
            q_tot += st["hold_q"]
        return p_tot, q_tot

    def _fed_apply_mode(self):
        """A satellite must not serve a POI meter - the master owns the POI."""
        want_meter = self.fed.get("mode") != "satellite"
        if want_meter and not self.meter.srv.running:
            self.meter.srv.start()
        elif not want_meter and self.meter.srv.running:
            self.meter.srv.stop()

    def _recompute_base(self):
        total = sum(i.rating_kw for i in self.inverters)
        if self.fed.get("mode") == "master":
            total += sum(float(st.get("rating_kw") or 0.0)
                         for st in self.peers.values())
        self.grid.s_base_mva = max(0.001, total / 1000.0)

    def set_tick(self, ms):
        ms = max(10, int(ms))
        self.tick_ms = ms
        self.dt = ms / 1000.0
        for inv in self.inverters:
            inv.dt = self.dt

    def _find(self, iid):
        for inv in self.inverters:
            if inv.iid == iid:
                return inv
        return None

    def start(self):
        threading.Thread(target=self._fed_loop, daemon=True).start()
        for inv in self.inverters:
            inv.srv.start()
        # A satellite must never bind the POI meter - the master owns the POI.
        # This has to honour the mode at boot, not only when it is toggled in
        # the GUI, or a Pi restored from a saved satellite config would come
        # back serving a partial POI to the controller.
        if self.fed.get("mode") != "satellite":
            self.meter.srv.start()
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        last = time.monotonic()
        while not self._stop:
            try:
                # REAL elapsed time, not the nominal tick: time.sleep()
                # overshoot plus the per-tick work means a wall second is
                # fewer than 1/dt ticks, so integrating with the nominal dt
                # made ramps, SOC and energy counters run slow (measured
                # ~3 % at a 50 ms tick, worse on a loaded Pi). Clamped to 1 s
                # so a stall or a suspend/resume cannot produce a huge step.
                now_m = time.monotonic()
                dt = min(max(now_m - last, 1e-4), 1.0)
                last = now_m
                with self.lock:
                    for inv in self.inverters:
                        inv.dt = dt
                        inv.tick()
                    p_plant = sum(i.current_pq()[0] for i in self.inverters)
                    q_plant = sum(i.current_pq()[1] for i in self.inverters)
                    # remote Pis (federated mode) contribute to the same POI
                    p_rem, q_rem = self._fed_contribution(dt)
                    p_plant += p_rem
                    q_plant += q_rem
                    g = self.grid.compute(p_plant, q_plant)
                    v_term = g["v_poi_pu"] * 690.0
                    for inv in self.inverters:
                        inv.set_grid(v_term, self.freq)
                    if self.met_ovr["enabled"]:
                        o = self.met_ovr
                        self.meter.write(o["v_v"], o["f_hz"],
                                         o["p_kw"], o["q_kvar"])
                    else:
                        self.meter.write(g["v_poi_v"], self.freq,
                                         g["p_poi_kw"], g["q_poi_kvar"])
                    self._build_snapshot(p_plant, q_plant, g)
            except Exception as e:
                # never let one bad tick kill the whole simulator
                print(f"[loop] tick error: {e!r}")
            time.sleep(self.dt)

    def _build_snapshot(self, p_plant, q_plant, g):
        invs = []
        for inv in self.inverters:
            p, q = inv.current_pq()
            invs.append({
                "id": inv.iid, "port": inv.port, "kind": inv.kind,
                "scale36": inv.scale36,
                "soc": round(inv.soc * 100, 1),
                "bat_capacity_kwh": round(inv.bat_capacity_kwh, 1),
                "enabled": inv.enabled, "running": inv.srv.running,
                # which listeners really bound, so a supervisor (and the
                # GUI) can tell a half-started device from a healthy one -
                # srv.running only means the thread came up
                "transports": sorted(inv.srv.up),
                "tracking": inv.tracking,
                "tracking_p": inv.tracking_p, "tracking_q": inv.tracking_q,
                "rating_kw": round(inv.rating_kw), "cap_kw": round(inv.cap_kw),
                "p_kw": round(p, 1), "q_kvar": round(q, 1),
                "opstt": get_s32(inv.ir, H_OPSTT), "v_v": round(inv.v_term, 1),
                "serial": inv.serial,
                "ramp_enabled": inv.ramp_enabled,
                "rate_p": round(inv.rate_p, 1), "rate_q": round(inv.rate_q, 1),
                "noise_enabled": inv.noise_enabled,
                "noise_p": round(inv.noise_p, 1), "noise_q": round(inv.noise_q, 1),
                "p_tgt": round(inv.p_tgt, 1), "q_tgt": round(inv.q_tgt, 1),
                "hyc_wspt": round(inv.hyc_wspt_pct, 2),
                "hyc_varspt": round(inv.hyc_varspt_pct, 2),
                "hyc_fststop": inv.hyc_fststop,
                "spt_src": inv.spt_src,
                "state": inv.state,
                "phase": (STARTUP_PHASES[inv.phase_i][0]
                          if inv.state == "startup" else ""),
                "err_no": inv.err_no,
                "err_tag": KODIAK_ERRORS[inv.err_no][0] if inv.err_no in KODIAK_ERRORS else "",
                "err_desc": KODIAK_ERRORS[inv.err_no][1] if inv.err_no in KODIAK_ERRORS else "",
                "err_sev": inv.err_sev or "",
                "spt_age": (round(time.time() - inv.last_hyc_write, 1)
                            if inv.last_hyc_write else None),
                "errclr_age": (round(time.time() - inv.last_hyc_errclr, 1)
                               if inv.last_hyc_errclr else None),
                "err": inv.srv.error,
                "peers": _peers_for_port(inv.port),
            })
        s_plant = math.hypot(p_plant, q_plant)
        pf = (p_plant / s_plant) if s_plant > 1e-6 else 1.0
        self.snapshot = {
            "plant": {"freq": self.freq, "p_plant_kw": round(p_plant, 1),
                      "q_plant_kvar": round(q_plant, 1), "pf": round(pf, 3),
                      "n_on": sum(1 for i in self.inverters if i.enabled),
                      "n_total": len(self.inverters)},
            "poi": {"p_kw": round(g["p_poi_kw"], 1), "q_kvar": round(g["q_poi_kvar"], 1),
                    "v_v": round(g["v_poi_v"], 1), "v_pu": round(g["v_poi_pu"], 4),
                    "p_loss_kw": round(g["p_loss_kw"], 1),
                    "q_loss_kvar": round(g["q_loss_kvar"], 1)},
            "grid": {"s_base_mva": round(self.grid.s_base_mva, 3),
                     "v_nom_ll": self.grid.v_nom_ll, "scr": self.grid.scr,
                     "xr": self.grid.xr, "v_grid_pu": self.grid.v_grid_pu,
                     "loss_frac_full": self.grid.loss_frac_full,
                     "loss_fixed_frac": self.grid.loss_fixed_frac,
                     "qloss_frac_full": self.grid.qloss_frac_full,
                     "s_base_mva": round(self.grid.s_base_mva, 2)},
            "fed": {**self.fed,
                    "peer_state": [
                        {"peer": k,
                         "name": v.get("name", ""),
                         "p_kw": round(v.get("hold_p", 0.0), 1),
                         "q_kvar": round(v.get("hold_q", 0.0), 1),
                         "rating_kw": round(v.get("rating_kw", 0.0)),
                         "n_inv": len(v.get("inverters") or []),
                         "ramping": bool(v.get("ramping")),
                         "age": (round(time.time() - v["last_ok"], 1)
                                 if v.get("last_ok") else None)}
                        for k, v in self.peers.items()]},
            "cfg_path": self._cfg_path,
            "in_use": {"on": self.in_use["on"], "who": self.in_use["who"],
                       "age": (round(time.time() - self.in_use["since"])
                               if self.in_use["since"] else None)},
            "meter": {"port": self.meter.port,
                      "running": self.meter.srv.running,
                      "transports": sorted(self.meter.srv.up),
                      "error": self.meter.srv.error,
                      "peers": _peers_for_port(self.meter.port),
                      "override": dict(self.met_ovr),
                      **{k: (round(v, 1) if isinstance(v, float) else v)
                         for k, v in self.meter.last.items()}},
            "sys": {"tmo": dict(self.tmo),
                    "irr_pct": self.irr_pct,
                    "startup_s": self.startup_cfg["seconds"],
                    "hyc_gated": self.startup_cfg.get("hyc_gated", True),
                    "udp_silent": bool(self.udp_cfg["silent_writes"]),
                    "strict": bool(self.strict["on"]),
                    "tick_ms": self.tick_ms},
            "inverters": invs,
            "recording": self.recorder.status(),
        }

    # ---- register monitor ----
    def _monitor_blocks(self, dev):
        """dev = 'meter' | 'inv<N>' -> ([(blkname, block, names)...], info)."""
        if dev == "meter":
            m = self.meter
            return ([("REG", m.reg, MET_NAMES, frozenset(), frozenset(), {})],
                    {"name": "Meter (POI \u00b7 SMA Power Analyzer)",
                     "proto": "Modbus TCP+UDP", "ip": self.host,
                     "port": m.port, "unit": "any", "running": m.srv.running})
        if dev.startswith("inv"):
            try:
                inv = self._find(int(dev[3:]))
            except ValueError:
                inv = None
            if inv is not None:
                msf = inv._msf
                # channels this unit serves pre-multiplied: the v1135 x36
                # kW/kVAr/kVA set (only when the x36 toggle is on) and the
                # 20 ms FIX3 mirrors 1042/1044 (always x1000 on top)
                ir_scale = {1042: 1000 * msf, 1044: 1000 * msf}
                hr_scale = {}
                if msf != 1:
                    for a in (34, 36, 50, 60, 62, 80,
                              1012, 1014, 1016, 1114):
                        ir_scale[a] = msf
                    for a in (4, 10, 14, HR_WRTG, HR_VARRTG, HR_VARTG):
                        hr_scale[a] = msf
                return ([("IR", inv.ir, INV_IR_NAMES, frozenset(),
                          INV_IR_ALWAYS, ir_scale),
                         ("HR", inv.hr, INV_HR_NAMES, INV_HYC_WRITE_ADDRS,
                          INV_HR_ALWAYS, hr_scale)],
                        {"name": (f"{'BESS' if inv.kind == 'bess' else 'INV'}"
                                  f" {inv.iid} (Kodiak \u00b7 SN {inv.serial})"),
                         "proto": "Modbus TCP+UDP", "ip": self.host,
                         "port": inv.port, "unit": "any",
                         "running": inv.srv.running})
        return (None, None)

    def _rec_capture(self, action, p):
        r = self.recorder
        try:
            if action in ("inv_manual", "inv_cap", "inv_rating", "inv_soc",
                          "inv_batcap"):
                dev = f"INV:{p.get('id')}"
                if action == "inv_manual":
                    if p.get("p") is not None:
                        r.log_param(dev, "WSpt", "", p["p"])
                    if p.get("q") is not None:
                        r.log_param(dev, "VArSpt", "", p["q"])
                else:
                    key = {"inv_cap": "WMaxLimNom", "inv_rating": "WRtg",
                           "inv_soc": "Bat.SOCConn",
                           "inv_batcap": "Bat.CapacRtg"}[action]
                    r.log_param(dev, key, "", p.get("value"))
            elif action in ("freq_set", "pv_irradiance"):
                r.log_param("PLANT", action, "", p.get("value"))
            elif action == "meter_set":
                for k in ("p_kw", "q_kvar", "v_v", "f_hz"):
                    if p.get(k) is not None:
                        r.log_param("METER", k, "", p[k])
            elif action in ("inv_enable", "inv_toggle"):
                r.log_event("Info", "event", "SIM",
                            f"Inverter {p.get('id')} switch commanded")
            elif action == "inv_inject_err":
                r.log_event("Error", "outgoing", "SIM",
                            f"Injected ErrNo {p.get('code')} on "
                            f"inverter {p.get('id')}")
            elif action == "inv_clear_err":
                r.log_event("Info", "incoming", "SIM",
                            f"Cleared error on inverter {p.get('id')}")
        except Exception:
            pass

    def regs(self, dev):
        with self.lock:
            blocks, info = self._monitor_blocks(dev)
            if blocks is None:
                return {"ok": False, "error": "no such device"}
            now = time.time()
            return {"ok": True, "dev": dev, "info": info,
                    "blocks": [{"blk": b,
                                "rows": _monitor_rows(blk, nm, now, wa, aw,
                                                      sc)}
                               for b, blk, nm, wa, aw, sc in blocks]}

    # ---- command dispatch ----
    def cmd(self, action, p):
        with self.lock:
            try:
                return self._cmd(action, p)
            except Exception as e:
                return {"ok": False, "error": str(e)}

    def _cmd(self, action, p):
        # ---- service recording ----
        if action == "rec_start":
            ok = self.recorder.start(int(p.get("interval_ms", 1000)))
            return {"ok": ok, **self.recorder.status()}
        if action == "rec_stop":
            summary = self.recorder.stop()
            return {"ok": True, "summary": summary, **self.recorder.status()}
        if action == "rec_status":
            return {"ok": True, **self.recorder.status()}
        # capture setpoint-ish changes + notable events into the recording
        if self.recorder.active:
            self._rec_capture(action, p)

        if action == "add_inverter":
            rating = float(p.get("rating", self.default_rating))
            kind = p.get("kind", "pv")
            cap = p.get("bat_capacity_kwh")   # None -> 2 h default
            inv = self._make_inverter(rating, start=True, kind=kind,
                                      bat_capacity_kwh=cap)
            return {"ok": True, "id": inv.iid, "port": inv.port}
        if action == "remove_inverter":
            inv = self._find(int(p["id"]))
            if inv:
                inv.srv.stop()
                self.used_ports.discard(inv.port)
                self.inverters.remove(inv)
                self._recompute_base()
            return {"ok": True}

        if action == "all_enable":
            for inv in self.inverters:
                self._set_enabled(inv, bool(p["value"]))
            return {"ok": True}
        if action == "all_tracking":
            for inv in self.inverters:
                inv.tracking = bool(p["value"])
            return {"ok": True}
        if action == "pv_irradiance":
            # plant-wide available-power ("irradiance") limit: sets every PV
            # inverter's WAval cap to pct% of its rating. Works in HYC
            # tracking mode - the unit simply cannot deliver more than its
            # available power, and the HYC sees the reduced WAval register,
            # exactly like a real derated/clouded PV unit. BESS untouched.
            pct = max(0.0, min(100.0, float(p["value"])))
            self.irr_pct = pct
            for inv in self.inverters:
                if inv.kind == "pv":
                    inv.set_cap(inv.rating_kw * pct / 100.0)
            return {"ok": True}
        if action == "freq_set":
            self.freq = float(p["value"])
            return {"ok": True}
        if action == "grid_set":
            f = p["field"]
            if hasattr(self.grid, f):
                setattr(self.grid, f, float(p["value"]))
            return {"ok": True}
        if action == "meter_override":
            self.met_ovr["enabled"] = bool(p["value"])
            if self.met_ovr["enabled"]:
                # start the override from the last real meter reading so
                # nothing jumps when the toggle is flipped
                for k in ("p_kw", "q_kvar", "v_v", "f_hz"):
                    if k in self.meter.last:
                        self.met_ovr[k] = float(self.meter.last[k])
            return {"ok": True}
        if action == "meter_set":
            for k in ("p_kw", "q_kvar", "v_v", "f_hz"):
                if p.get(k) is not None:
                    self.met_ovr[k] = float(p[k])
            return {"ok": True}
        if action == "tmo_set":
            if p.get("enabled") is not None:
                self.tmo["enabled"] = bool(p["enabled"])
            if p.get("seconds") is not None:
                self.tmo["seconds"] = max(1.0, float(p["seconds"]))
            if p.get("mode") is not None and int(p["mode"]) in (8712, 8713):
                self.tmo["mode"] = int(p["mode"])
            return {"ok": True}
        if action == "strict_set":
            self.strict["on"] = bool(p.get("on"))
            return {"ok": True}
        if action == "udp_ack_set":
            self.udp_cfg["silent_writes"] = not bool(p.get("ack"))
            return {"ok": True}
        if action == "fed_set":
            f = self.fed
            if p.get("mode") is not None:
                mode = str(p["mode"])
                if mode not in self.FED_MODES:
                    return {"ok": False, "error": f"unknown mode {mode!r}"}
                f["mode"] = mode
                self._fed_apply_mode()
            if p.get("peers") is not None:
                seen, clean, self_refs = set(), [], []
                for item in p["peers"]:
                    a = str(item).strip().rstrip("/")
                    if not a or a in seen:
                        continue
                    if self._is_self(a):
                        # polling ourselves would add our own output twice
                        self_refs.append(a)
                        continue
                    seen.add(a)
                    clean.append(a)
                f["peers"] = clean[:self.FED_MAX_PEERS]
                for gone in [k for k in self.peers if k not in f["peers"]]:
                    self.peers.pop(gone, None)
                if self_refs:
                    self._recompute_base()
                    return {"ok": False, "fed": dict(f),
                            "error": "this Pi cannot be its own peer: "
                                     + ", ".join(self_refs)}
            for key, lo, hi in (("poll_hz", 0.2, 50.0),
                                ("timeout_s", 0.2, 120.0),
                                ("wgraflb_pu_s", 0.001, 10.0),
                                ("vargraflb_pu_s", 0.001, 10.0)):
                if p.get(key) is not None:
                    try:
                        f[key] = min(hi, max(lo, float(p[key])))
                    except (TypeError, ValueError):
                        return {"ok": False, "error": f"{key} must be a number"}
            if p.get("master") is not None:
                f["master"] = str(p["master"]).strip()[:60]
            self._recompute_base()
            return {"ok": True, "fed": dict(f)}
        if action == "config_save":
            ok, msg = self.save_config()
            return {"ok": ok, "msg": msg, "path": self.config_path()}
        if action == "config_load":
            ok, msg = self.load_config(p.get("path") or None)
            return {"ok": ok, "msg": msg, "path": self.config_path()}
        if action == "in_use_set":
            on = bool(p.get("on"))
            who = str(p.get("who", "") or "").strip()[:60]
            if on and not self.in_use["on"]:
                self.in_use["since"] = time.time()
            elif not on:
                self.in_use["since"] = None
            self.in_use["on"], self.in_use["who"] = on, who
            return {"ok": True, "in_use": dict(self.in_use)}
        if action == "startup_set":
            if p.get("seconds") is not None:
                self.startup_cfg["seconds"] = max(0.0, float(p["seconds"]))
            if p.get("hyc_gated") is not None:
                self.startup_cfg["hyc_gated"] = bool(p["hyc_gated"])
            return {"ok": True}
        if action == "tick_set":
            self.set_tick(p.get("ms", 200))
            return {"ok": True, "tick_ms": self.tick_ms}

        # register monitor: manual write / hold-latch on any register
        if action in ("reg_write", "reg_hold"):
            blocks, _info = self._monitor_blocks(str(p.get("dev", "")))
            if not blocks:
                return {"ok": False, "error": "no such device"}
            blk = next((b for n, b, _nm, _wa, _aw, _sc in blocks
                        if n == p.get("blk")), None)
            if blk is None:
                return {"ok": False, "error": "no such block"}
            addr = int(p["addr"])
            typ = str(p.get("type", "u16"))
            nw = 2 if typ in ("s32", "u32") else 1
            if action == "reg_write":
                if p.get("value") is None:
                    return {"ok": False, "error": "no value"}
                blk.force(addr, _reg_encode(p["value"], typ))
            else:  # reg_hold
                if p.get("on"):
                    if p.get("value") is not None:
                        blk.force(addr, _reg_encode(p["value"], typ))
                    blk.hold(addr, nw)
                else:
                    blk.release(addr, nw)
            return {"ok": True}

        # per-inverter
        inv = self._find(int(p["id"]))
        if inv is None:
            return {"ok": False, "error": "no such inverter"}
        if action == "inv_toggle":
            self._set_enabled(inv, not inv.enabled)
        elif action == "inv_enable":
            self._set_enabled(inv, bool(p["value"]))
        elif action == "inv_tracking":
            # axis 'p' / 'q' toggles one signal; no axis = both (legacy)
            axis = p.get("axis")
            if axis == "p":
                inv.tracking_p = bool(p["value"])
            elif axis == "q":
                inv.tracking_q = bool(p["value"])
            else:
                inv.tracking = bool(p["value"])
        elif action == "inv_clear_fststop":
            inv.clear_fststop()
        elif action == "inv_inject_err":
            code = int(p["code"])
            if code not in KODIAK_ERRORS:
                return {"ok": False, "error": "unknown ErrNo"}
            inv.raise_error(code)
        elif action == "inv_clear_err":
            inv.clear_error()
        elif action == "inv_cap":
            inv.set_cap(p["value"])
        elif action == "inv_scale36":
            inv.set_scale36(bool(p["value"]))
        elif action == "inv_soc":
            if inv.kind != "bess":
                return {"ok": False, "error": "not a BESS inverter"}
            inv.set_soc(float(p["value"]))
        elif action == "inv_batcap":
            if inv.kind != "bess":
                return {"ok": False, "error": "not a BESS inverter"}
            inv.set_bat_capacity(float(p["value"]))
        elif action == "inv_manual":
            # setting a manual target switches ONLY that axis to manual;
            # the other axis keeps tracking the HYC untouched
            if p.get("p") is not None:      # None = blank/NaN input in GUI
                inv.p_tgt = float(p["p"])
                inv.tracking_p = False
            if p.get("q") is not None:
                inv.q_tgt = float(p["q"])
                inv.tracking_q = False
        elif action == "inv_rating":
            inv.set_rating(float(p["value"]))
            self._recompute_base()
        elif action == "inv_serial":
            inv.set_serial(int(p["value"]))
        elif action == "inv_port":
            newp = int(p["value"])
            if newp in self.used_ports and newp != inv.port:
                return {"ok": False, "error": "port in use"}
            if newp != inv.port:
                # a port owned by a FOREIGN process is not in used_ports -
                # check the real thing before moving the device there
                free, why = _port_free(self.host, newp)
                if not free:
                    return {"ok": False,
                            "error": f"port {newp} unavailable ({why})"}
            old = inv.port
            self.used_ports.discard(old)
            if not inv.set_port(newp):
                err = inv.srv.error or "bind failed"
                inv.set_port(old)       # fall back to the port that worked
                self.used_ports.add(old)
                return {"ok": False, "error": f"port {newp} rejected: {err}"}
            self.used_ports.add(newp)
        elif action == "inv_ramp":
            if "enabled" in p:
                inv.ramp_enabled = bool(p["enabled"])
            if "rate_p" in p:
                inv.rate_p = max(0.0, float(p["rate_p"]))
            if "rate_q" in p:
                inv.rate_q = max(0.0, float(p["rate_q"]))
        elif action == "inv_noise":
            if "enabled" in p:
                inv.noise_enabled = bool(p["enabled"])
            if "noise_p" in p:
                inv.noise_p = max(0.0, float(p["noise_p"]))
            if "noise_q" in p:
                inv.noise_q = max(0.0, float(p["noise_q"]))
        else:
            return {"ok": False, "error": "unknown action"}
        return {"ok": True}

    def _set_enabled(self, inv, on):
        if on and not inv.enabled:
            inv.enabled = True
            inv.clear_fststop()   # power-cycling a real inverter clears FstStop
            inv._fststop_active = False
            inv.tmo_active = False
            inv.last_hyc_write = None   # re-arm watchdog on next HYC write
            inv.err_no, inv.err_sev = 0, None
            inv._write_err_regs()
            inv._begin_startup()  # walk Init -> ... -> GridFeed like real HW
            if not inv.srv.running:
                inv.srv = DeviceServer(inv.ctx_tcp, inv.ctx_udp, inv.host,
                                       inv.port, udp_cfg=inv.udp_cfg)
                inv.srv.start()
        elif not on and inv.enabled:
            inv.enabled = False
            inv.srv.stop()


# ==========================================================================
# Web GUI (standard library only)
# ==========================================================================
PAGE = r"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Inverter Simulator</title>
<style>
 /* Dark is the default (bench/control-room use). Every surface colour is a
    variable so the light palette below only has to restate the values - no
    rule is duplicated per theme. The toggle stamps data-theme on <html>. */
 :root{--bg:#0f1419;--panel:#1a2129;--line:#2b3742;--fg:#e6edf3;--mut:#8b98a5;
   --on:#2ea043;--off:#6e7681;--acc:#388bfd;--warn:#d29922;--sel:#1f6feb;--bad:#f85149;
   --tile:#141b22;--btn:#21262d;--btn-off:#30363d;--field:#0d1117;
   --thead:#161d25;--rowhov:#18212b;--sect:#131a22;--held:#2a2412;}
 :root[data-theme="light"]{--bg:#f4f6f8;--panel:#ffffff;--line:#d3dae1;--fg:#16202a;
   --mut:#5a6a78;--on:#1a7f37;--off:#8b96a0;--acc:#0a58ca;--warn:#9a6700;
   --sel:#0a58ca;--bad:#c0392b;
   --tile:#f7f9fb;--btn:#eceff3;--btn-off:#dde3e9;--field:#ffffff;
   --thead:#eef1f5;--rowhov:#e8edf2;--sect:#e4e9ef;--held:#fff3cd;}
 *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--fg);
   font:14px/1.4 system-ui,Segoe UI,Roboto,sans-serif}
 header{padding:14px 18px;border-bottom:1px solid var(--line);display:flex;
   align-items:baseline;gap:14px} h1{font-size:18px;margin:0}
 .mut{color:var(--mut)}
 .wrap{padding:16px 18px;display:grid;gap:16px;max-width:1200px;margin:0 auto}
 .row{display:grid;gap:16px;grid-template-columns:1fr 1fr}
 @media(max-width:820px){.row{grid-template-columns:1fr}}
 .card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px}
 .card h2{font-size:13px;text-transform:uppercase;letter-spacing:.04em;color:var(--mut);margin:0 0 10px}
 .big{font-size:26px;font-weight:600} .unit{font-size:13px;color:var(--mut);font-weight:400}
 .kpi{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}
 .kpi .mut{font-size:11px;text-transform:uppercase;letter-spacing:.03em}
 .invgrid{display:grid;gap:10px;grid-template-columns:repeat(auto-fill,minmax(170px,1fr))}
 .inv{background:var(--tile);border:1px solid var(--line);border-radius:8px;padding:10px;cursor:pointer}
 .inv:hover{border-color:var(--acc)} .inv.off{opacity:.5}
 .inv.sel{border-color:var(--sel);box-shadow:0 0 0 1px var(--sel)}
 .dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:6px}
 .dot.on{background:var(--on)} .dot.off{background:var(--off)}
 .inv .nm{font-weight:600} .inv .pq{font-size:18px;margin:6px 0 2px}
 .inv .sub{font-size:11px;color:var(--mut)}
 button{background:var(--btn);color:var(--fg);border:1px solid var(--line);border-radius:6px;
   padding:5px 9px;cursor:pointer;font-size:12px} button:hover{border-color:var(--acc)}
 /* filled buttons keep white text in BOTH themes - inheriting --fg would put
    near-black text on the green/blue fill once the light palette is on */
 button.on{background:var(--on);border-color:var(--on);color:#fff}
 button.off{background:var(--btn-off)}
 button.acc{background:var(--acc);border-color:var(--acc);color:#fff}
 .toolbar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:10px}
 button.mix{border-color:var(--warn);color:var(--warn)}
 .gridform{display:grid;grid-template-columns:auto 100px auto;gap:8px 10px;align-items:center}
 input[type=number]{width:100px;background:var(--field);color:var(--fg);border:1px solid var(--line);
   border-radius:5px;padding:4px 6px} input.sm{width:64px}
 select{background:var(--field);color:var(--fg);border:1px solid var(--line);
   border-radius:5px;padding:4px 6px;font-size:12px;max-width:240px}
 label.lbl{color:var(--mut)} .hint{font-size:11px;color:var(--mut);margin-top:8px}
 .ch{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:11px;
   color:var(--acc);letter-spacing:0}
 .kpi .desc{font-size:10px;color:var(--mut)}
 .kpi2{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-top:12px;
   padding-top:10px;border-top:1px solid var(--line)}
 .kpi2 .v{font-size:16px;font-weight:600}
 .seltog{display:flex;gap:8px;flex-wrap:wrap;margin:4px 0 12px}
 /* --- register monitor --- */
 .regwrap{max-height:440px;overflow:auto;border:1px solid var(--line);border-radius:8px;margin-top:8px}
 table.regs{border-collapse:collapse;width:100%;
   font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:11.5px}
 table.regs th{position:sticky;top:0;background:var(--thead);color:var(--mut);
   text-transform:uppercase;font-size:10px;letter-spacing:.04em;padding:6px 8px;
   text-align:left;border-bottom:1px solid var(--line);z-index:1}
 table.regs td{padding:3px 8px;border-bottom:1px solid #202a34;white-space:nowrap}
 table.regs tr:hover td{background:var(--rowhov)}
 td.act{color:var(--mut);min-width:52px}
 td.act.hot-r{color:var(--acc);font-weight:700}
 td.act.hot-w{color:var(--on);font-weight:700}
 td.val{font-weight:600}
 td.val.chg{color:var(--warn)}
 tr.held td{background:var(--held)}
 tr.sect td{background:var(--sect);font-weight:700;font-size:10px;text-transform:uppercase;
   letter-spacing:.06em;padding:6px 8px;border-bottom:1px solid var(--line);white-space:normal}
 tr.sect.s-w td{color:var(--on)} tr.sect.s-r td{color:var(--acc)}
 input.regset{width:92px;background:var(--field);color:var(--fg);border:1px solid var(--line);
   border-radius:4px;padding:2px 4px;font:inherit}
 button.rbt{padding:2px 7px;font-size:11px}
 input.regfil{background:var(--field);color:var(--fg);border:1px solid var(--line);
   border-radius:5px;padding:4px 8px;font-size:12px;width:190px}
</style>
<script>
// Apply the stored theme BEFORE the body paints, so a light-theme user never
// gets a dark flash on reload.
(function(){var t=null;try{t=localStorage.getItem('invsim_theme');}catch(e){}
 if(!t){try{t=window.matchMedia&&window.matchMedia('(prefers-color-scheme: light)').matches?'light':'dark';}catch(e){t='dark';}}
 document.documentElement.setAttribute('data-theme',t);})();
</script>
</head><body>
<header><h1>Inverter Simulator</h1><span class="mut" id="sub">connecting…</span>
 <span id="inst_name" style="align-self:center;font-size:22px;font-weight:700"></span>
 <span id="inuse_chip" style="align-self:center"></span>
 <input id="inuse_who" placeholder="your name" style="width:130px" title="who is using the rig">
 <button id="inuse_btn" onclick="toggleInUse()" title="claim the bench so colleagues can see it is busy">In Use</button>
 <span style="flex:1"></span>
 <button id="theme_btn" title="switch between the dark and light palette - remembered in this browser" onclick="toggleTheme()">theme</button></header>
<div class="wrap">
 <div class="row">
  <div class="card"><h2>Point of Interconnection (meter · SMA Power Analyzer)</h2>
   <div class="kpi">
    <div><div class="ch">PwrAtPoi</div><div class="big" id="poi_p">–</div>
      <div class="desc">active power at POI</div></div>
    <div><div class="ch">PwrRtPoi</div><div class="big" id="poi_q">–</div>
      <div class="desc">reactive power at POI</div></div>
    <div><div class="ch">VtgPoi</div><div class="big" id="poi_v">–</div>
      <div class="desc">voltage at POI (L-L)</div></div>
    <div><div class="mut">V (pu)</div><div class="big" id="poi_vpu">–</div>
      <div class="desc">sim only – per unit</div></div>
   </div>
   <div class="kpi2">
    <div><div class="ch">PwrApPoi</div><div class="v" id="poi_s">–</div>
      <div class="desc">apparent power</div></div>
    <div><div class="ch">PFPoi</div><div class="v" id="poi_pf">–</div>
      <div class="desc">power factor</div></div>
    <div><div class="ch">FacPoi</div><div class="v" id="poi_f">–</div>
      <div class="desc">grid frequency</div></div>
    <div><div class="ch">IacPoiL1..L3</div><div class="v" id="poi_i">–</div>
      <div class="desc">phase current</div></div>
   </div>
   <div class="hint" id="losshint"></div>
   <div class="toolbar" style="margin:10px 0 0">
    <button id="movr_btn" onclick="cmd('meter_override',{value:!meterOvr})">override: …</button>
    <span class="mut" style="font-size:11px">manual override writes GUI values to the meter registers (grid model bypassed)</span>
   </div>
   <div class="gridform" id="movr_form" style="display:none;margin-top:10px"></div>
  </div>
  <div class="card"><h2>Plant aggregate (inverter sum)</h2>
   <div class="kpi">
    <div><div class="ch">ΣInvMs.TotW</div><div class="big" id="pl_p">–</div>
      <div class="desc">total active power</div></div>
    <div><div class="ch">ΣInvMs.TotVAr</div><div class="big" id="pl_q">–</div>
      <div class="desc">total reactive power</div></div>
    <div><div class="mut">PF</div><div class="big" id="pl_pf">–</div>
      <div class="desc">plant power factor</div></div>
    <div><div class="mut">Online</div><div class="big" id="pl_n">–</div>
      <div class="desc">inverters enabled</div></div>
   </div><div class="hint">POI = this sum minus collector/transformer losses.</div></div>
 </div>

 <div class="card"><h2>Service recording</h2>
  <div class="toolbar">
   <button id="rec_btn" onclick="toggleRec()">● Record</button>
   <label class="lbl" style="margin-left:6px">interval</label>
   <select id="rec_int">
     <option value="50">50 ms</option>
     <option value="100">100 ms</option>
     <option value="200">200 ms</option>
     <option value="500">500 ms</option>
     <option value="1000" selected>1000 ms</option>
     <option value="5000">5 s</option>
   </select>
   <span id="rec_stat" class="mut" style="margin-left:10px">idle</span>
   <span style="flex:1"></span>
   <a id="rec_dl" href="/api/download" style="display:none">
     <button class="acc">⭳ Download last bundle</button></a>
  </div>
  <div class="hint">Records the POI meter + every inverter to an SMA-format
   <code>.sma.csv</code>, wrapped in a <code>service_SC30COM_*.zip</code>
   (info.xml, devices .spot, eventlog, paramchanges) like a real SC30COM
   service dump. Device layout is frozen when recording starts.</div>
 </div>

 <div class="card"><h2>Inverters</h2>
  <div class="toolbar">
   <button onclick="cmd('all_enable',{value:true})">All ON</button>
   <button onclick="cmd('all_enable',{value:false})">All OFF</button>
   <button onclick="cmd('all_tracking',{value:true})">All track HYC</button>
   <button onclick="cmd('all_tracking',{value:false})">All manual</button>
   <span style="margin-left:12px;align-self:center;font-size:12px;color:var(--mut)" title="available PV power (irradiance) as % of each PV inverter's rating - sets every PV unit's WAval cap. Limits output even in HYC-tracking mode, like clouds/derating on a real plant. BESS units are not affected.">☀ PV irradiance %</span>
   <input type="number" id="irr" class="sm" step="5" min="0" max="100" value="100">
   <button onclick="cmd('pv_irradiance',{value:val('irr')})">Set</button>
   <span style="flex:1"></span>
   <input type="number" id="newrating" class="sm" step="0.5" value="5" title="new inverter rating (MW)">
   <button class="acc" onclick="cmd('add_inverter',{rating:val('newrating')*1000})">+ Add PV inverter</button>
   <button class="acc" onclick="cmd('add_inverter',{rating:val('newrating')*1000,kind:'bess'})" title="battery inverter (2h capacity by default - change in the selected panel)">+ Add BESS</button>
   <button onclick="removeSel()">– Remove selected</button>
  </div>
  <div class="invgrid" id="invs"></div>
 </div>

 <div class="card" id="selcard" style="display:none">
  <h2>Selected inverter</h2>
  <div id="seltitle" style="margin-bottom:8px;font-weight:600"></div>
  <div class="seltog" id="selstatus"></div>
  <div class="gridform" id="selinputs"></div>
  <div class="hint">P and Q sources are independent: "Set P manual" switches only P out of HYC tracking (Q keeps following VArSpt) and vice versa - use the P:/Q: buttons to hand an axis back to the HYC. Changing the port briefly drops its socket.</div>
 </div>

 <div class="row">
  <div class="card"><h2>Grid model (sim only – not registers)</h2>
   <div class="gridform" id="gridform"></div>
   <div class="hint">Vpoi_pu ≈ Vgrid + p·R + q·X, Z=1/SCR split by X/R. Lower SCR = weaker grid = bigger voltage swing. Losses ∝ load².</div></div>
  <div class="card"><h2>System</h2>
   <div class="gridform">
    <label class="lbl"><span class="ch">GriMs.Hz / FacPoi</span> (grid frequency, Hz)</label>
    <input type="number" id="freq" step="0.01">
    <button onclick="cmd('freq_set',{value:val('freq')})">Set</button>
    <label class="lbl">Setpoint watchdog timeout (s) <span class="mut">(Kodiak grid-mgmt comms)</span></label>
    <input type="number" id="tmo_s" step="5">
    <button onclick="cmd('tmo_set',{seconds:val('tmo_s')})">Set</button>
    <label class="lbl">On timeout</label>
    <select id="tmo_mode" onchange="cmd('tmo_set',{mode:+this.value})">
      <option value="8712">8712 – warn, keep running</option>
      <option value="8713">8713 – fault, switch off</option>
    </select>
    <button id="tmo_btn" onclick="cmd('tmo_set',{enabled:!tmoOn})">watchdog</button>
    <label class="lbl">UDP write replies <span class="mut">(real Kodiak = silent / fire-and-forget)</span></label>
    <span></span>
    <button id="udp_btn" onclick="cmd('udp_ack_set',{ack:udpSilent})">…</button>
    <label class="lbl">strict addressing <span class="mut">(off-profile requests get a Modbus exception, like real firmware)</span></label>
    <span></span>
    <button id="strict_btn" onclick="cmd('strict_set',{on:!strictOn})">…</button>
    <label class="lbl">Startup sequence duration (s) <span class="mut">(0 = instant)</span></label>
    <input type="number" id="su_s" step="5">
    <button onclick="cmd('startup_set',{seconds:val('su_s')})">Set</button>
    <label class="lbl">Saved configuration <span class="mut">(fleet, ports, grid model and system options - reloaded automatically on the next start)</span></label>
    <span><button onclick="saveCfg()" title="write the current rig to the config file">Save config</button>
    <button onclick="loadCfg()" title="discard the current rig and restore the saved one">Reload saved</button>
    <span class="mut" id="cfg_path"></span></span>
    <label class="lbl">Start command <span class="mut">(real hardware waits for the controller to write <span class="ch">FstStop</span> = 1467; turn off to run standalone with no HYC attached)</span></label>
    <button id="hycgate_btn" onclick="cmd('startup_set',{hyc_gated:!hycGated})">…</button>
    <label class="lbl">Sim tick (ms) <span class="mut">(lower = higher-res data for MoMo / high-speed Modbus polling; more CPU)</span></label>
    <input type="number" id="tick_ms" step="10" min="10">
    <button onclick="cmd('tick_set',{ms:val('tick_ms')})">Set</button></div>
   <div class="hint" id="syshint"></div></div>
  <div class="card"><h2>Multi-Pi mode <span class="mut" style="text-transform:none;font-weight:400">— for HYC firmware that can only target an IP and always uses port 502</span></h2>
   <div class="gridform">
    <label class="lbl">Mode</label>
    <span>
     <button id="fed_off" onclick="cmd('fed_set',{mode:'off'})" title="normal single-Pi operation: every inverter on this host, each on its own port">off</button>
     <button id="fed_master" onclick="cmd('fed_set',{mode:'master'})" title="this Pi serves the POI meter and adds the other Pis' output into the plant total">master</button>
     <button id="fed_sat" onclick="cmd('fed_set',{mode:'satellite'})" title="this Pi serves only its own inverter; the master owns the POI meter">satellite</button>
    </span>
    <span class="mut" id="fed_hint"></span>
   </div>
   <div id="fed_master_box" style="display:none">
    <label class="lbl">Other Pis <span class="mut">(one IP per line, up to 20 — add <span class="ch">:port</span> only if a Pi's web GUI is not on 8080)</span></label>
    <textarea id="fed_peers" rows="4" style="width:100%;background:var(--field);color:var(--fg);border:1px solid var(--line);border-radius:5px;padding:6px;font:12px ui-monospace,Consolas,monospace"></textarea>
    <div class="toolbar" style="margin-top:8px">
     <button class="acc" onclick="saveFedPeers()">Apply list</button>
     <label class="lbl" style="align-self:center">poll Hz</label><input class="sm" type="number" id="fed_hz" step="1" min="1">
     <label class="lbl" style="align-self:center">lost after (s)</label><input class="sm" type="number" id="fed_to" step="0.5" min="0.5">
     <label class="lbl" style="align-self:center" title="fallback active-power gradient: how fast a lost Pi's output is ramped back to zero, in pu/s of its rating">WGraFlb pu/s</label><input class="sm" type="number" id="fed_wg" step="0.05" min="0.01">
     <label class="lbl" style="align-self:center" title="fallback reactive-power gradient">VArGraFlb pu/s</label><input class="sm" type="number" id="fed_vg" step="0.05" min="0.01">
     <button onclick="saveFedRates()">Set</button>
    </div>
    <table class="regs" style="margin-top:10px"><thead><tr>
      <th>Pi</th><th>owner</th><th>inverters</th><th>P (kW)</th><th>Q (kVAr)</th><th>rating</th><th>last seen</th><th>state</th>
    </tr></thead><tbody id="fed_rows"></tbody></table>
    <div class="hint">A Pi that stops answering is not dropped from the plant: its last output is ramped back to zero at the fallback gradients, so the POI meter shows a ramp rather than a step — the same way a real plant reacts to losing an inverter.</div>
   </div>
   <div id="fed_sat_box" style="display:none">
    <label class="lbl">Master Pi <span class="mut">(informational — the master polls this Pi, so nothing needs to be reachable from here)</span></label>
    <span><input type="text" id="fed_master_addr" style="width:180px">
    <button onclick="cmd('fed_set',{master:document.getElementById('fed_master_addr').value})">Set</button></span>
    <div class="hint">This Pi serves only its own inverter. Its POI meter is stopped — the master owns the POI.</div>
   </div>
  </div>
 </div>

 <div class="card"><h2>Register monitor (live Modbus datastore)</h2>
  <div class="toolbar">
   <select id="reg_dev" onchange="regDev=this.value;regSig='';regTick();"></select>
   <select id="reg_cat" onchange="regCat=this.value;regSig='';regTick();">
    <option value="">reads + writes</option>
    <option value="w">written by HYC (commands/setpoints)</option>
    <option value="r">read by HYC (measurements/status)</option>
   </select>
   <input type="text" class="regfil" id="reg_filter" placeholder="filter name / address…" oninput="regFilter()">
   <label style="font-size:12px;color:var(--mut);align-self:center" title="registers the HYC touched that are NOT in the PPC profile">
    <input type="checkbox" id="reg_unknown" onchange="regFilter()"> <span id="reg_unknown_lbl">show unknown</span></label>
   <span style="flex:1"></span>
   <span class="mut" id="reg_info" style="font-size:11px;align-self:center"></span>
  </div>
  <div class="regwrap"><table class="regs"><thead><tr>
    <th>Blk</th><th>Addr</th><th>Name</th><th>Value</th><th>Raw (hex)</th>
    <th>HYC R</th><th>HYC W</th><th>Conn</th><th>Set</th><th></th><th></th>
  </tr></thead><tbody id="reg_body"></tbody></table></div>
  <div class="hint">Registers are grouped by direction: <b>written by HYC</b> = commands &amp;
   setpoints the HYC sends (a register moves here automatically the moment the HYC writes it);
   <b>read by HYC</b> = measurements / status / ratings the sim publishes.
   HYC R / HYC W = seconds since the HYC last read / wrote that register
   (<b>.</b> = no traffic yet · blue/green = active in the last 2.5 s · yellow value = just changed).
   <b>Conn</b> = transport actually seen on that register (MB-TCP / MB-UDP / TCP+UDP · <b>·</b> = no
   traffic yet). <b>Write</b> = one-shot manual write (the sim may overwrite it on the next tick).
   <b>Hold</b> = freeze the register at this value against both the sim and the HYC until released
   (held rows are amber). Only registers defined in the PPC profile (Kodiak v1035) plus the raw HYC
   setpoint addresses 40018/40022/40023 are shown; anything else the HYC touches is hidden behind
   <b>show unknown</b>.</div>
 </div>
</div>
<script>
let selectedId=null, panelFor=null, firstGrid=true, meterOvr=false, movrDrawn=false, tmoOn=true, udpSilent=true, strictOn=true, hycGated=true;
let inUseOn=false;
let regDev=null, regSig='', regCat='', regBusy=false;
const ERRLIB=__ERRLIB__;
const esc=s=>String(s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const rid=(b,a,s)=>`rg_${b}_${a}_${s}`;
function regFilter(){
  const q=(document.getElementById('reg_filter').value||'').toLowerCase();
  const showUnk=document.getElementById('reg_unknown').checked;
  let hidden=0;
  for(const tr of document.querySelectorAll('#reg_body tr')){
    if(tr.classList.contains('sect'))continue;   // section headers always visible
    const unk=tr.dataset.u==='1';
    if(unk&&!showUnk){tr.style.display='none';hidden++;continue;}
    const hit=(!q||tr.dataset.k.includes(q));
    tr.style.display=hit?'':'none';
  }
  document.getElementById('reg_unknown_lbl').textContent=
    'show unknown'+(hidden?` (${hidden} hidden)`:'');
}
async function regTick(){
 if(regDev==null||regBusy)return;
 regBusy=true;
 let r;
 try{r=await(await fetch('/api/regs?dev='+encodeURIComponent(regDev))).json();}
 catch(e){regBusy=false;return;}
 regBusy=false;
 if(!r||!r.ok)return;
 const inf=r.info, conn=`:${inf.port}`;
 // flatten all blocks, apply the read/write category filter, split in two
 const rows=[];
 for(const b of r.blocks)for(const x of b.rows)
   if(!regCat||x.c===regCat)rows.push({b:b.blk,x});
 const wRows=rows.filter(e=>e.x.c==='w'), dRows=rows.filter(e=>e.x.c==='r');
 document.getElementById('reg_info').textContent=
   `${inf.name} · ${inf.proto} · ${inf.ip}${conn} · unit ${inf.unit} · ${inf.running?'serving':'stopped'} · ${rows.length} regs`;
 const sig=regDev+'|'+regCat+'|'+rows.map(e=>e.b+':'+e.x.a+':'+e.x.c+':'+(e.x.s||1)).join(',');
 const tb=document.getElementById('reg_body');
 if(sig!==regSig){                      // row set changed -> rebuild skeleton
   const rowHtml=(b,x)=>`<tr id="${rid(b,x.a,'tr')}" data-k="${(b+' '+x.a+' '+x.n).toLowerCase()}" data-n="${x.n?1:0}" data-u="${x.u?1:0}">
      <td class="mut">${b}</td>
      <td>${x.a}${x.w>1?'–'+(x.a+x.w-1):''}</td>
      <td class="ch">${x.n?esc(x.n):'<span class="mut">– (not in profile)</span>'}</td>
      <td class="val" id="${rid(b,x.a,'v')}"></td>
      <td class="mut" id="${rid(b,x.a,'raw')}"></td>
      <td class="act" id="${rid(b,x.a,'r')}"></td>
      <td class="act" id="${rid(b,x.a,'w')}"></td>
      <td class="mut" id="${rid(b,x.a,'cn')}"></td>
      <td><input class="regset" id="${rid(b,x.a,'in')}" type="number" step="1" title="${(x.s||1)>1?'engineering value - written to the wire \u00d7'+x.s:'raw register value'}"></td>
      <td><button class="rbt" onclick="regWrite('${b}',${x.a},'${x.t}',${x.s||1})">Write</button></td>
      <td><button class="rbt" id="${rid(b,x.a,'h')}" data-h="0"
           onclick="regHold('${b}',${x.a},'${x.t}',${x.s||1})">Hold</button></td>
     </tr>`;
   const sect=(cls,t,n)=>`<tr class="sect ${cls}"><td colspan="11">${t} · ${n}</td></tr>`;
   let h='';
   if(wRows.length){
     h+=sect('s-w','▼ Written by HYC — commands &amp; setpoints',wRows.length);
     for(const e of wRows)h+=rowHtml(e.b,e.x);
   }
   if(dRows.length){
     h+=sect('s-r','▼ Read by HYC — measurements · status · ratings',dRows.length);
     for(const e of dRows)h+=rowHtml(e.b,e.x);
   }
   tb.innerHTML=h; regSig=sig; regFilter();
 }
 for(const e of rows){                  // targeted updates: inputs untouched
   const b={blk:e.b}, x=e.x;
   const v=document.getElementById(rid(b.blk,x.a,'v')); if(!v)continue;
   const s=String(x.v);
   v.classList.toggle('chg',v.textContent!==''&&v.textContent!==s);
   v.textContent=s;
   document.getElementById(rid(b.blk,x.a,'raw')).textContent=
     x.raw.map(w=>w.toString(16).padStart(4,'0')).join(' ');
   const rc=document.getElementById(rid(b.blk,x.a,'r'));
   rc.textContent=x.r==null?'.':x.r.toFixed(1)+'s';
   rc.className='act'+(x.r!=null&&x.r<2.5?' hot-r':'');
   const wc=document.getElementById(rid(b.blk,x.a,'w'));
   wc.textContent=x.wr==null?'.':x.wr.toFixed(1)+'s';
   wc.className='act'+(x.wr!=null&&x.wr<2.5?' hot-w':'');
   const PROTO={T:'MB-TCP',U:'MB-UDP',TU:'TCP+UDP'};
   document.getElementById(rid(b.blk,x.a,'cn')).textContent=
     (PROTO[x.p]||'·')+' '+conn;
   const hb=document.getElementById(rid(b.blk,x.a,'h'));
   hb.textContent=x.h?'Held':'Hold'; hb.className='rbt'+(x.h?' on':''); hb.dataset.h=x.h?'1':'0';
   document.getElementById(rid(b.blk,x.a,'tr')).classList.toggle('held',!!x.h);
 }
}
function regWrite(blk,a,t,s){
  const v=val(rid(blk,a,'in'));
  if(v==null)return;
  cmd('reg_write',{dev:regDev,blk:blk,addr:a,type:t,value:v*(s||1)});
  setTimeout(regTick,150);
}
function regHold(blk,a,t,s){
  const hb=document.getElementById(rid(blk,a,'h'));
  const on=hb.dataset.h!=='1';
  const v=on?val(rid(blk,a,'in')):null;
  cmd('reg_hold',{dev:regDev,blk:blk,addr:a,type:t,on:on,value:v==null?null:v*(s||1)});
  setTimeout(regTick,150);
}
const val=id=>{const el=document.getElementById(id);if(el==null||el.value==='')return null;const v=+el.value;return Number.isFinite(v)?v:null;};
async function cmd(action,p){await fetch('/api/cmd',{method:'POST',
  headers:{'Content-Type':'application/json'},body:JSON.stringify(Object.assign({action},p||{}))});tick();}
// like cmd() but hands back the parsed reply - the config actions report
// whether they worked and where the file went
async function cmdR(action,p){
  let j=null;
  try{
    const res=await fetch('/api/cmd',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify(Object.assign({action},p||{}))});
    j=await res.json();
  }catch(e){ j={ok:false,msg:String(e)}; }
  tick(); return j;}
function select(id){selectedId=id;panelFor=null;tick();}
function removeSel(){if(selectedId==null)return;const id=selectedId;selectedId=null;panelFor=null;cmd('remove_inverter',{id});}
function gridField(label,field,v,step){
  return `<label class="lbl">${label}</label>
   <input type="number" id="g_${field}" step="${step}" value="${v}">
   <button onclick="cmd('grid_set',{field:'${field}',value:val('g_${field}')})">Set</button>`;}

function statusHtml(s){
  const stateChip = s.state==='startup'
    ? `<span style="align-self:center;color:var(--acc);font-weight:600">⟳ starting: ${s.phase}</span>`
    : s.state==='fault'
    ? `<span style="align-self:center;color:var(--bad);font-weight:700">✖ FAULT – tripped</span>`
    : s.state==='stopped'
    ? `<span style="align-self:center;color:var(--warn);font-weight:700">■ STOPPED</span>`:'';
  const errChip = s.err_no
    ? `<span style="align-self:center;color:${s.err_sev==='RD'?'var(--bad)':'var(--warn)'};font-weight:600">
        ⚠ ${s.err_no} <span class="ch">${s.err_tag}</span> ${s.err_desc}</span>
       <button title="local acknowledge - the controller can do the same by writing ErrClr (HR 1544) non-zero" onclick="cmd('inv_clear_err',{id:${s.id}})">ErrClr</button>`:'';
  const ackChip = s.errclr_age!=null && s.errclr_age<10
    ? `<span style="align-self:center;color:var(--on);font-weight:600" title="the controller wrote ErrClr (HR 1544) and this inverter acknowledged its error">✔ ErrClr from controller ${s.errclr_age}s ago</span>`:'';
  return `<span class="mut" style="align-self:center">
    <span class="ch">InvMs.TotW</span> ${s.p_kw} kW ·
    <span class="ch">InvMs.TotVAr</span> ${s.q_kvar} kVAr ·
    <span class="ch">OpStt</span> ${s.opstt} ·
    ${s.kind==='bess'?`<span class="ch">Bat.SOCConn</span> ${s.soc}% · `:''}
    <span class="ch">GriMs.V</span> ${(s.v_v/1).toFixed(0)} V ·
    HYC cmds: <span class="ch">WSpt</span> ${s.hyc_wspt}% ·
    <span class="ch">VArSpt</span> ${s.hyc_varspt}% ·
    <span class="ch">FstStop</span> ${s.hyc_fststop} ·
    last spt write ${s.spt_age==null?'never':s.spt_age+'s ago'}${s.spt_src?' via '+s.spt_src:''}</span>
   <span class="mut" style="align-self:center" title="remote IPs holding a Modbus TCP connection to this inverter's port">
    · connected: ${s.peers&&s.peers.length?'<b style="color:var(--on)">'+s.peers.join(', ')+'</b>':'<span style="color:var(--mut)">nobody</span>'}</span>
   ${stateChip} ${errChip} ${ackChip}
   ${s.hyc_fststop===1749?`<span style="align-self:center;color:var(--warn);font-weight:700">
     ⚠ FAST-STOPPED BY HYC – P/Q forced to 0</span>
     <button onclick="cmd('inv_clear_fststop',{id:${s.id}})">clear FstStop</button>`:''}
   <span style="flex:1"></span>
   <button class="${s.enabled?'on':'off'}" onclick="cmd('inv_enable',{id:${s.id},value:${!s.enabled}})">${s.enabled?'ON':'OFF'}</button>
   <button class="${s.tracking?'on':(s.tracking_p||s.tracking_q?'mix':'')}" title="click: switch BOTH P and Q between HYC tracking and manual (per-axis toggles are next to the manual target fields below)" onclick="cmd('inv_tracking',{id:${s.id},value:${!s.tracking}})">${s.tracking?'HYC track':(s.tracking_p||s.tracking_q?('mixed · P:'+(s.tracking_p?'HYC':'man')+' Q:'+(s.tracking_q?'HYC':'man')):'manual')}</button>
   <button class="${s.ramp_enabled?'on':''}" onclick="cmd('inv_ramp',{id:${s.id},enabled:${!s.ramp_enabled}})">ramp ${s.ramp_enabled?'on':'off'}</button>
   <button class="${s.noise_enabled?'on':''}" onclick="cmd('inv_noise',{id:${s.id},enabled:${!s.noise_enabled}})">noise ${s.noise_enabled?'on':'off'}</button>
   ${s.kind==='pv'?`<button class="${s.scale36?'on':''}" title="serve kW registers pre-multiplied x36 like the real v1135 Kodiak - turn OFF if the HYC displays inflated PV values" onclick="cmd('inv_scale36',{id:${s.id},value:${!s.scale36}})">x36 ${s.scale36?'on':'off'}</button>`:''}`;}

function inputsHtml(s){
  const fld=(id,v,step)=>`<input type="number" id="${id}" step="${step}" value="${v}">`;
  return `
   <label class="lbl"><span class="ch">WAval</span> (available power / irradiance limit, kW <span class="mut">– caps output even while HYC-tracking</span>)</label>${fld('s_cap',s.cap_kw,50)}
     <button onclick="cmd('inv_cap',{id:${s.id},value:val('s_cap')})">Set</button>
   ${s.kind==='bess'?`<label class="lbl"><span class="ch">Bat.SOCConn</span> (state of charge, %)</label>${fld('s_soc',s.soc,5)}
     <button onclick="cmd('inv_soc',{id:${s.id},value:val('s_soc')})">Set</button>
   <label class="lbl"><span class="ch">Bsc.WhAvail</span> (usable battery capacity, kWh)</label>${fld('s_bcap',s.bat_capacity_kwh,100)}
     <button onclick="cmd('inv_batcap',{id:${s.id},value:val('s_bcap')})">Set</button>`:''}
   <label class="lbl"><span class="ch">InvMs.TotW</span> (manual P target, kW${s.kind==='bess'?' – negative = charge':''})</label>${fld('s_mp',s.p_tgt,50)}
     <span><button onclick="cmd('inv_manual',{id:${s.id},p:val('s_mp')})">Set P manual</button>
     <button class="${s.tracking_p?'on':''}" title="P source: HYC WSpt tracking or the manual target on the left" onclick="cmd('inv_tracking',{id:${s.id},axis:'p',value:${!s.tracking_p}})">P: ${s.tracking_p?'HYC':'manual'}</button></span>
   <label class="lbl"><span class="ch">InvMs.TotVAr</span> (manual Q target, kVAr)</label>${fld('s_mq',s.q_tgt,50)}
     <span><button onclick="cmd('inv_manual',{id:${s.id},q:val('s_mq')})">Set Q manual</button>
     <button class="${s.tracking_q?'on':''}" title="Q source: HYC VArSpt tracking or the manual target on the left" onclick="cmd('inv_tracking',{id:${s.id},axis:'q',value:${!s.tracking_q}})">Q: ${s.tracking_q?'HYC':'manual'}</button></span>
   <label class="lbl"><span class="ch">WRtg / VArRtg / VARtg</span> (rated power, kW)</label>${fld('s_rat',s.rating_kw,100)}
     <button onclick="cmd('inv_rating',{id:${s.id},value:val('s_rat')})">Set</button>
   <label class="lbl">Ramp rate P / Q (kW/s) <span class="mut">(sim only)</span></label>
     <span>${fld('s_rp',s.rate_p,10)} ${fld('s_rq',s.rate_q,10)}</span>
     <button onclick="cmd('inv_ramp',{id:${s.id},rate_p:val('s_rp'),rate_q:val('s_rq')})">Set</button>
   <label class="lbl">Noise ± P / Q (kW,kVAr) <span class="mut">(sim only)</span></label>
     <span>${fld('s_np',s.noise_p,1)} ${fld('s_nq',s.noise_q,1)}</span>
     <button onclick="cmd('inv_noise',{id:${s.id},noise_p:val('s_np'),noise_q:val('s_nq')})">Set</button>
   <label class="lbl"><span class="ch">DevInf.SerNo</span> (device serial number)</label>${fld('s_ser',s.serial,1)}
     <button onclick="cmd('inv_serial',{id:${s.id},value:val('s_ser')})">Set</button>
   <label class="lbl">Port <span class="mut">(sim only – Modbus TCP)</span></label>${fld('s_port',s.port,1)}
     <button onclick="cmd('inv_port',{id:${s.id},value:val('s_port')})">Set</button>
   <label class="lbl">Inject Kodiak error <span class="mut">(RD trips unit · YW warns)</span></label>
     <select id="s_errsel">${Object.keys(ERRLIB).map(c=>
       `<option value="${c}">${c} ${ERRLIB[c][2]} – ${ERRLIB[c][1]}</option>`).join('')}</select>
     <button onclick="cmd('inv_inject_err',{id:${s.id},code:+document.getElementById('s_errsel').value})">Inject</button>`;}

async function tick(){
 let r; try{r=await (await fetch('/api/state')).json();}catch(e){return;}
 if(!r.plant)return;
 document.getElementById('sub').textContent =
   `${r.plant.n_on}/${r.plant.n_total} inverters online · meter on port ${r.meter.port}`;
 updateRec(r.recording||{});
 const poi=r.poi, pl=r.plant, met=r.meter;
 // first row shows what is actually in the meter registers (= what the HYC reads)
 document.getElementById('poi_p').innerHTML=met.p_kw+' <span class="unit">kW</span>';
 document.getElementById('poi_q').innerHTML=met.q_kvar+' <span class="unit">kVAr</span>';
 document.getElementById('poi_v').innerHTML=(met.v_v/1000).toFixed(2)+' <span class="unit">kV</span>';
 document.getElementById('poi_vpu').textContent=poi.v_pu;
 document.getElementById('poi_s').innerHTML=Math.hypot(met.p_kw,met.q_kvar).toFixed(1)+' <span class="unit">kVA</span>';
 document.getElementById('poi_pf').textContent=met.pf;
 document.getElementById('poi_f').innerHTML=met.f_hz+' <span class="unit">Hz</span>';
 document.getElementById('poi_i').innerHTML=met.i_a+' <span class="unit">A</span>';
 document.getElementById('losshint').textContent=meterOvr
   ? 'MANUAL OVERRIDE ACTIVE – grid model bypassed'
   : `Losses: ${poi.p_loss_kw} kW / ${poi.q_loss_kvar} kVAr`;

 // meter override toggle + form
 meterOvr=!!(met.override&&met.override.enabled);
 const mb=document.getElementById('movr_btn');
 mb.textContent='override: '+(meterOvr?'ON':'off');
 mb.className=meterOvr?'on':'';
 const mf=document.getElementById('movr_form');
 if(meterOvr&&!movrDrawn){
   const o=met.override, f=(id,v,st)=>`<input type="number" id="${id}" step="${st}" value="${v}">`;
   mf.innerHTML=
    `<label class="lbl"><span class="ch">PwrAtPoi</span> (active power, kW)</label>${f('mo_p',o.p_kw,50)}
      <button onclick="cmd('meter_set',{p_kw:val('mo_p')})">Set</button>
     <label class="lbl"><span class="ch">PwrRtPoi</span> (reactive power, kVAr)</label>${f('mo_q',o.q_kvar,50)}
      <button onclick="cmd('meter_set',{q_kvar:val('mo_q')})">Set</button>
     <label class="lbl"><span class="ch">VtgPoi</span> (voltage L-L, V)</label>${f('mo_v',o.v_v,100)}
      <button onclick="cmd('meter_set',{v_v:val('mo_v')})">Set</button>
     <label class="lbl"><span class="ch">FacPoi</span> (frequency, Hz)</label>${f('mo_f',o.f_hz,0.01)}
      <button onclick="cmd('meter_set',{f_hz:val('mo_f')})">Set</button>`;
   mf.style.display='';movrDrawn=true;
 } else if(!meterOvr){mf.style.display='none';mf.innerHTML='';movrDrawn=false;}

 document.getElementById('pl_p').innerHTML=pl.p_plant_kw+' <span class="unit">kW</span>';
 document.getElementById('pl_q').innerHTML=pl.q_plant_kvar+' <span class="unit">kVAr</span>';
 document.getElementById('pl_pf').textContent=pl.pf;
 document.getElementById('pl_n').textContent=pl.n_on+'/'+pl.n_total;

 let h='';
 for(const v of r.inverters){
  const on=v.enabled, sel=v.id===selectedId;
  h+=`<div class="inv ${on?'':'off'} ${sel?'sel':''}" onclick="select(${v.id})">
    <div class="nm"><span class="dot ${on?'on':'off'}"></span>${v.kind==='bess'?'\u{1F50B} BESS':'INV'} ${v.id} <span class="sub">:${v.port}</span></div>
    <div class="pq">${v.p_kw} <span class="unit">kW</span> <span class="ch">InvMs.TotW</span></div>
    <div class="sub"><span class="ch">InvMs.TotVAr</span> ${v.q_kvar} kVAr · <span class="ch">WRtg</span> ${v.rating_kw/1000} MW · ${v.tracking?'HYC':(v.tracking_p||v.tracking_q?('P:'+(v.tracking_p?'HYC':'man')+' Q:'+(v.tracking_q?'HYC':'man')):'man')}</div>
    ${v.kind==='bess'?`<div class="sub"><span class="ch">Bat.SOCConn</span> ${v.soc}% · ${v.p_kw<0?'charging':(v.p_kw>0?'discharging':'idle')}</div>`:''}
    <div class="ctlrow" style="margin-top:8px">
     <button class="${on?'on':'off'}" onclick="event.stopPropagation();cmd('inv_toggle',{id:${v.id}})">${on?'ON':'OFF'}</button>
    </div>
    ${v.state==='startup'?`<div class="sub" style="color:var(--acc);font-weight:600">⟳ ${v.phase}</div>`:''}
    ${v.err_no?`<div class="sub" style="color:${v.err_sev==='RD'?'var(--bad)':'var(--warn)'};font-weight:700">⚠ ${v.err_no} ${v.err_tag}</div>`:''}
    ${v.hyc_fststop===1749?`<div class="sub" style="color:var(--warn);font-weight:700">⚠ FSTSTOP (HYC)</div>`:''}
    ${v.err?`<div class="sub" style="color:var(--warn)">${v.err}</div>`:''}
   </div>`;}
 document.getElementById('invs').innerHTML=h;

 const sel=r.inverters.find(v=>v.id===selectedId);
 const card=document.getElementById('selcard');
 if(!sel){card.style.display='none';panelFor=null;}
 else{
   card.style.display='';
   document.getElementById('seltitle').innerHTML=
     `${sel.kind==='bess'?'\u{1F50B} BESS':'INV'} ${sel.id} <span class="mut">· port ${sel.port} · serial ${sel.serial} · ${sel.rating_kw/1000} MW${sel.kind==='bess'?' · '+(sel.bat_capacity_kwh/1000).toFixed(1)+' MWh · SOC '+sel.soc+'%':''} · ${sel.running?'serving':'stopped'}</span>`;
   document.getElementById('selstatus').innerHTML=statusHtml(sel);
   if(panelFor!==selectedId){
     document.getElementById('selinputs').innerHTML=inputsHtml(sel);
     panelFor=selectedId;
   }
 }

 if(firstGrid){
  const g=r.grid;
  document.getElementById('gridform').innerHTML=
    gridField('POI nominal (V, L-L)','v_nom_ll',g.v_nom_ll,100)+
    gridField('SCR (grid strength)','scr',g.scr,0.5)+
    gridField('X/R ratio','xr',g.xr,0.5)+
    gridField('Grid voltage (pu)','v_grid_pu',g.v_grid_pu,0.01)+
    gridField('Cu loss @full (frac)','loss_frac_full',g.loss_frac_full,0.005)+
    gridField('Q loss @full (frac)','qloss_frac_full',g.qloss_frac_full,0.005);
  document.getElementById('freq').value=r.plant.freq;
  document.getElementById('irr').value=r.sys.irr_pct;
  document.getElementById('tmo_s').value=r.sys.tmo.seconds;
  document.getElementById('tmo_mode').value=String(r.sys.tmo.mode);
  document.getElementById('su_s').value=r.sys.startup_s;
  const iu=r.in_use||{on:false};
  const chip=document.getElementById('inuse_chip'), ib=document.getElementById('inuse_btn'),
        iw=document.getElementById('inuse_who');
  inUseOn=!!iu.on;
  const nm=document.getElementById('inst_name');
  nm.textContent=iu.who||'';
  nm.style.color=iu.on?'var(--warn)':'var(--acc)';
  chip.innerHTML=iu.on
    ?`<b style="color:var(--warn)">● IN USE — ${iu.who||'someone'}</b>`
     +`<span class="mut"> (${iu.age==null?'':fmtAge(iu.age)})</span>`
    :'';
  ib.textContent=iu.on?'release':'In Use';
  ib.className=iu.on?'on':'';
  if(document.activeElement!==iw) iw.value=iu.who||iw.value;
  document.title=(iu.on?'● ':'')+'Inverter Simulator';
  const cp=document.getElementById('cfg_path');
  if(cp&&!cp.textContent) cp.textContent=r.cfg_path||'';
  drawFed(r.fed);
  hycGated=r.sys.hyc_gated!==false;
  const hg=document.getElementById('hycgate_btn');
  hg.textContent=hycGated?'wait for HYC (like real HW)':'self-start (standalone)';
  hg.className=hycGated?'on':'';
  if(r.sys.tick_ms!=null&&document.activeElement!==document.getElementById('tick_ms'))
    document.getElementById('tick_ms').value=r.sys.tick_ms;
  document.getElementById('syshint').textContent=`Plant base ${g.s_base_mva} MVA · meter port ${r.meter.port}`;
  firstGrid=false;
 } else {
  document.getElementById('syshint').textContent=`Plant base ${r.grid.s_base_mva} MVA · meter port ${r.meter.port}`;
 }
 tmoOn=!!r.sys.tmo.enabled;
 const tb=document.getElementById('tmo_btn');
 tb.textContent='watchdog: '+(tmoOn?'ON':'off');
 tb.className=tmoOn?'on':'';
 udpSilent=!!r.sys.udp_silent;
 const ub=document.getElementById('udp_btn');
 ub.textContent=udpSilent?'silent (like real HW)':'ACK every write';
 ub.className=udpSilent?'on':'';
 strictOn=!!r.sys.strict;
 const sb=document.getElementById('strict_btn');
 sb.textContent=strictOn?'strict windows (test mode)':'permissive+0xFFFF (like real HW)';
 sb.className=strictOn?'on':'';

 // register monitor: keep device list in sync, then refresh the table
 const rd=document.getElementById('reg_dev');
 const opts=r.inverters.map(v=>['inv'+v.id,`INV ${v.id}  :${v.port}`])
                       .concat([['meter',`Meter  :${r.meter.port}`]]);
 const osig=opts.map(o=>o[0]+o[1]).join('|');
 if(rd.dataset.sig!==osig){
   rd.innerHTML=opts.map(o=>`<option value="${o[0]}">${o[1]}</option>`).join('');
   rd.dataset.sig=osig;
   if(regDev==null||!opts.some(o=>o[0]===regDev)){regDev=opts.length?opts[0][0]:null;regSig='';}
   if(regDev!=null)rd.value=regDev;
 }
 regTick();
}
let recActive=false;
async function toggleRec(){
  if(recActive){await cmd('rec_stop',{});}
  else{await cmd('rec_start',{interval_ms:+document.getElementById('rec_int').value});}
}
function updateRec(rc){
  recActive=!!rc.active;
  const b=document.getElementById('rec_btn');
  const st=document.getElementById('rec_stat');
  const dl=document.getElementById('rec_dl');
  const si=document.getElementById('rec_int');
  b.textContent=recActive?'■ Stop':'● Record';
  b.className=recActive?'off':'';
  b.style.color=recActive?'var(--bad)':'';
  si.disabled=recActive;
  if(recActive){
    st.textContent=`REC ${rc.elapsed_s|0}s · ${rc.samples} samples · ${rc.channels} ch`;
    st.style.color='var(--bad)';
  } else if(rc.summary){
    st.textContent=`saved ${rc.summary.file} (${rc.summary.samples} samples, `+
      `${(rc.summary.bytes/1024).toFixed(0)} KB)`;
    st.style.color='var(--on)';
  } else {st.textContent='idle';st.style.color='';}
  dl.style.display=rc.download?'':'none';
}
function saveFedPeers(){
  const lines=document.getElementById('fed_peers').value.split(/[\s,;]+/).filter(x=>x);
  cmd('fed_set',{peers:lines});}
function saveFedRates(){
  cmd('fed_set',{poll_hz:val('fed_hz'),timeout_s:val('fed_to'),
                 wgraflb_pu_s:val('fed_wg'),vargraflb_pu_s:val('fed_vg')});}
function drawFed(f){
  if(!f) return;
  const mode=f.mode||'off';
  for(const [id,m] of [['fed_off','off'],['fed_master','master'],['fed_sat','satellite']]){
    const b=document.getElementById(id); if(b) b.className=(mode===m)?'on':'';}
  document.getElementById('fed_master_box').style.display=(mode==='master')?'':'none';
  document.getElementById('fed_sat_box').style.display=(mode==='satellite')?'':'none';
  const hint=document.getElementById('fed_hint');
  hint.textContent = mode==='off'
    ? 'single Pi, one port per inverter'
    : mode==='master'
    ? 'serving the POI meter for this Pi plus '+((f.peers||[]).length)+' other Pi(s)'
    : 'inverter only; POI meter stopped';
  const pe=document.getElementById('fed_peers');
  if(document.activeElement!==pe) pe.value=(f.peers||[]).join('\n');
  for(const [id,v] of [['fed_hz',f.poll_hz],['fed_to',f.timeout_s],
                       ['fed_wg',f.wgraflb_pu_s],['fed_vg',f.vargraflb_pu_s]]){
    const e=document.getElementById(id);
    if(e&&document.activeElement!==e) e.value=v;}
  const ma=document.getElementById('fed_master_addr');
  if(ma&&document.activeElement!==ma) ma.value=f.master||'';
  const rows=(f.peer_state||[]).map(p=>{
    const lost=p.age==null||p.age>(f.timeout_s||2);
    const st=p.age==null?'<span style="color:var(--mut)">never seen</span>'
      :lost?'<span style="color:var(--bad);font-weight:600">LOST — ramping back</span>'
      :'<span style="color:var(--on)">live</span>';
    return `<tr><td class="ch">${p.peer}</td><td>${p.name||''}</td><td>${p.n_inv}</td>
      <td>${p.p_kw}</td><td>${p.q_kvar}</td><td>${p.rating_kw}</td>
      <td>${p.age==null?'—':p.age+'s'}</td><td>${st}</td></tr>`;}).join('');
  document.getElementById('fed_rows').innerHTML=rows||
    '<tr><td colspan="8" class="mut">no other Pis configured</td></tr>';}
function fmtAge(sec){
  if(sec<60) return sec+'s';
  if(sec<3600) return Math.floor(sec/60)+'m';
  const h=Math.floor(sec/3600), m=Math.floor((sec%3600)/60);
  return h+'h'+(m?' '+m+'m':'');}
function saveCfg(){
  cmdR('config_save',{}).then(r=>{
    const e=document.getElementById('cfg_path');
    e.textContent=(r&&r.ok?'saved  '+(r.path||''):'save failed: '+((r&&r.msg)||'?'));
    e.style.color=(r&&r.ok)?'var(--on)':'var(--bad)';});}
function loadCfg(){
  if(!confirm('Reload the saved configuration? The current inverter fleet will be rebuilt.')) return;
  cmdR('config_load',{}).then(r=>{
    const e=document.getElementById('cfg_path');
    e.textContent=(r&&r.msg)||'';
    e.style.color=(r&&r.ok)?'var(--on)':'var(--bad)';
    firstGrid=true;});}
function toggleInUse(){
  const who=document.getElementById('inuse_who').value.trim();
  cmd('in_use_set',{on:!inUseOn, who:who});}
function currentTheme(){
  return document.documentElement.getAttribute('data-theme')==='light'?'light':'dark';}
function applyTheme(t){
  document.documentElement.setAttribute('data-theme',t);
  const b=document.getElementById('theme_btn');
  // the button offers the OTHER theme, so it reads as an action not a state
  if(b) b.textContent = t==='light' ? 'dark theme' : 'light theme';
  try{localStorage.setItem('invsim_theme',t);}catch(e){}}
function toggleTheme(){applyTheme(currentTheme()==='light'?'dark':'light');}
applyTheme(currentTheme());
tick(); setInterval(tick,1000);
</script></body></html>"""
PAGE = PAGE.replace("__ERRLIB__", json.dumps(KODIAK_ERRORS))


class WebHandler(BaseHTTPRequestHandler):
    plant = None

    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/json"):
        b = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/index"):
            self._send(200, PAGE, "text/html; charset=utf-8")
        elif self.path == "/api/state":
            self._send(200, json.dumps(self.plant.snapshot))
        elif self.path.startswith("/api/regs"):
            q = parse_qs(urlparse(self.path).query)
            dev = (q.get("dev") or ["meter"])[0]
            self._send(200, json.dumps(self.plant.regs(dev)))
        elif self.path.startswith("/api/download"):
            lz = self.plant.recorder.last_zip
            if not lz:
                self._send(404, json.dumps({"error": "no recording yet"}))
                return
            fn, data = lz
            self.send_response(200)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Disposition",
                             f'attachment; filename="{fn}"')
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            self._send(404, json.dumps({"error": "not found"}))

    def do_POST(self):
        if self.path != "/api/cmd":
            self._send(404, json.dumps({"error": "not found"}))
            return
        n = int(self.headers.get("Content-Length", 0))
        try:
            data = json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            self._send(400, json.dumps({"error": "bad json"}))
            return
        action = data.pop("action", None)
        self._send(200, json.dumps(self.plant.cmd(action, data)))


def main():
    ap = argparse.ArgumentParser(description="SMA Inverter Simulator (configurable inverter fleet + POI meter + web GUI)")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--inverters", type=int, default=3, help="number of PV inverters to spawn at startup (add/remove more in the GUI)")
    ap.add_argument("--bess", type=int, default=0, help="number of BESS (battery) inverters to spawn at startup")
    ap.add_argument("--bess-capacity", type=float, default=None, help="usable battery capacity per BESS unit (kWh; default 2 x rating = 2 h)")
    ap.add_argument("--pv-plain", action="store_true", help="(deprecated - plain is now the default) serve PV kW registers PLAIN instead of x36")
    ap.add_argument("--pv-x36", action="store_true", help="serve PV kW registers pre-multiplied x36 like the real v1135 Kodiak firmware (only useful against an HYC that applies the profile scale=36 - otherwise it displays 36x-inflated PV values)")
    ap.add_argument("--base-port", type=int, default=1502, help="first inverter port (allocated upward)")
    ap.add_argument("--meter-port", type=int, default=1600)
    ap.add_argument("--rating", type=float, default=5000.0, help="default per-inverter rating (kW)")
    ap.add_argument("--voltage", type=float, default=33000.0, help="POI nominal voltage (V, L-L)")
    ap.add_argument("--web-port", type=int, default=8080)
    ap.add_argument("--tick-ms", type=int, default=200, help="sim recompute period in ms (lower = higher-resolution data for high-speed MoMo/Modbus polling; more CPU). Default 200; try 50 for meaningful 100 ms captures.")
    ap.add_argument("--name", default="", help="instance/owner name shown at the top of the GUI - use it when several people each run their own simulator on one box")
    ap.add_argument("--config", default=None, help="path to the saved configuration (default: ~/.inverter_sim/config_<web-port>.json)")
    ap.add_argument("--no-restore", action="store_true", help="ignore the saved configuration and start from the command line options only")
    args = ap.parse_args()

    plant = Plant(args.host, args.inverters, args.base_port, args.meter_port,
                  args.rating, args.voltage, web_port=args.web_port,
                  n_bess=args.bess, bess_capacity_kwh=args.bess_capacity,
                  pv_scale36=(args.pv_x36 and not args.pv_plain),
                  tick_ms=args.tick_ms)
    if args.config:
        plant._cfg_path = os.path.abspath(args.config)
    restored = None
    if not args.no_restore:
        # Re-apply the saved rig BEFORE the servers come up, so ports and the
        # fleet layout are the ones the operator last saved.
        try:
            ok, msg = plant.load_config()
        except Exception as e:      # never let a bad file stop the sim booting
            ok, msg = False, f"could not apply saved config: {e!r}"
        restored = msg if ok else None
        if not ok and "no saved config" not in msg:
            print(f"  [config] {msg}")
    # after the restore, so an explicit --name always wins over the saved owner
    if args.name:
        plant.in_use["who"] = args.name[:60]
    plant._fed_apply_mode()
    plant.start()
    WebHandler.plant = plant
    web = ThreadingHTTPServer(("0.0.0.0", args.web_port), WebHandler)
    print("Inverter Simulator running:")
    print(f"  {args.inverters} PV + {args.bess} BESS inverter(s) -> {args.host} ports {args.base_port}+")
    print(f"  meter (POI)     -> {args.host} port {args.meter_port}")
    print(f"  POI {args.voltage/1000:.0f} kV, default inverter {args.rating/1000:.1f} MW")
    print(f"  sim tick        -> {args.tick_ms} ms ({1000.0/max(1,args.tick_ms):.0f} Hz)")
    print(f"  web GUI         -> http://<this-host>:{args.web_port}")
    print(f"  config          -> {plant.config_path()}"
          + (f" ({restored})" if restored else " (none saved yet)"))
    if plant.in_use.get("who"):
        print(f"  instance        -> {plant.in_use['who']}")
    try:
        web.serve_forever()
    except KeyboardInterrupt:
        plant._stop = True
        print("\nshutting down")


if __name__ == "__main__":
    main()

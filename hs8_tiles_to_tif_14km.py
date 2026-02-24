# ============================================================
# HS8 14km core / 1km overlap (16km buffered)
# FAST + LOCK-SAFE WEEKEND RUN:
#   - Parallel tile workers
#   - Within each tile: parallel hillshades (subprocess)
#   - NO FileGDB writes during the parallel run
#   - Final output per tile is an 8-band GeoTIFF (core area)
#   - Per-tile temp folder deleted after completion
#
# Outputs:
#   E:\DHM_Hillshade8_Tiles\HS8CORE_14km_TIF\HS8CORE_14km_r###c###.tif
# ============================================================

import os
import math
import sys
import time
import subprocess
import datetime as dt
import arcpy

arcpy.CheckOutExtension("Spatial")

# =========================
# USER SETTINGS
# =========================
DEM = r"C:\Data\DHM_2023_40cm2.tif"

# Tile settings
TILE_SIZE_M = 14000
OVERLAP_M   = 1000  # buffered = 16 km

# Parallelism (recommend keeping this; your license is the limiting factor)
TILE_WORKERS = 4
HS_WORKERS_PER_TILE = 5   # if you see license init errors, drop to 4

# Hillshade params
ALTITUDE = 45
Z_FACTOR = 1
AZIMUTHS = [0, 45, 90, 135, 180, 225, 270, 315]

# Paths
D_ROOT = r"D:\DHM_HS8_WORK"
D_SCRATCH_GDB = os.path.join(D_ROOT, "Scratch.gdb")
D_TMP_ROOT = os.path.join(D_ROOT, "tile_tmp_14km")  # per tile scratch

E_ROOT = r"E:\DHM_Hillshade8_Tiles"
E_TIF_OUT_DIR = os.path.join(E_ROOT, "HS8CORE_14km_TIF")
TILE_CORE_PREFIX = "HS8CORE_14km"

# Retry behavior (helps with occasional ArcGIS license init flakiness)
HILLSHADE_RETRIES = 3
RETRY_BACKOFF_SEC = 6  # grows each retry

# =========================
# Helpers
# =========================
def ensure_gdb(gdb_path: str):
    folder = os.path.dirname(gdb_path)
    os.makedirs(folder, exist_ok=True)
    if not arcpy.Exists(gdb_path):
        arcpy.management.CreateFileGDB(folder, os.path.basename(gdb_path))

def fmt_td(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h}h {m:02d}m {s:02d}s"
    return f"{m}m {s:02d}s"

def fmt_eta(seconds_from_now: float) -> str:
    eta = dt.datetime.now() + dt.timedelta(seconds=max(0, seconds_from_now))
    return eta.strftime("%Y-%m-%d %H:%M")

def clip_rect_to_tif(in_ras, rect_tuple, out_tif):
    xmin, ymin, xmax, ymax = rect_tuple
    rect = f"{xmin} {ymin} {xmax} {ymax}"
    arcpy.management.Clip(
        in_raster=in_ras,
        rectangle=rect,
        out_raster=out_tif,
        in_template_dataset="#",
        nodata_value="#",
        clipping_geometry="NONE",
        maintain_clipping_extent="MAINTAIN_EXTENT"
    )
    return out_tif

# =========================
# Write worker scripts
# =========================
os.makedirs(D_ROOT, exist_ok=True)
HILLSHADE_WORKER_PY = os.path.join(D_ROOT, "hillshade_worker.py")
TILE_WORKER_PY      = os.path.join(D_ROOT, "tile_worker.py")

HILLSHADE_WORKER_CODE = r'''
import sys, time

# Small stagger helps prevent license-init races when many workers start at once
time.sleep(0.25)

import arcpy
from arcpy.sa import Hillshade

def main():
    dem_tif = sys.argv[1]
    out_tif = sys.argv[2]
    az      = float(sys.argv[3])
    alt     = float(sys.argv[4])
    zf      = float(sys.argv[5])
    snap    = sys.argv[6]

    # Warm up product/license
    _ = arcpy.ProductInfo()

    arcpy.CheckOutExtension("Spatial")
    arcpy.env.overwriteOutput = True
    arcpy.env.snapRaster = snap
    arcpy.env.cellSize = snap
    arcpy.env.extent = dem_tif

    if not arcpy.Exists(dem_tif):
        raise RuntimeError(f"DEM tif does not exist: {dem_tif}")

    hs = Hillshade(dem_tif, az, alt, "NO_SHADOWS", zf)
    hs.save(out_tif)

if __name__ == "__main__":
    main()
'''

TILE_WORKER_CODE = r'''
import os, sys, time, shutil, subprocess
import arcpy

def clip_rect_to_tif(in_ras, rect_tuple, out_tif):
    xmin, ymin, xmax, ymax = rect_tuple
    rect = f"{xmin} {ymin} {xmax} {ymax}"
    arcpy.management.Clip(
        in_raster=in_ras,
        rectangle=rect,
        out_raster=out_tif,
        in_template_dataset="#",
        nodata_value="#",
        clipping_geometry="NONE",
        maintain_clipping_extent="MAINTAIN_EXTENT"
    )
    return out_tif

def main():
    dem                 = sys.argv[1]
    out_dir             = sys.argv[2]
    tile_core_prefix     = sys.argv[3]
    tmp_root            = sys.argv[4]
    hillshade_worker_py  = sys.argv[5]
    tile_size_m          = float(sys.argv[6])
    overlap_m            = float(sys.argv[7])
    altitude             = float(sys.argv[8])
    z_factor             = float(sys.argv[9])
    hs_workers           = int(sys.argv[10])
    hs_retries           = int(sys.argv[11])
    retry_backoff        = float(sys.argv[12])
    tile_r               = int(sys.argv[13])
    tile_c               = int(sys.argv[14])

    azimuths = [0,45,90,135,180,225,270,315]

    # Warm up product/license in the tile process too
    _ = arcpy.ProductInfo()
    arcpy.CheckOutExtension("Spatial")

    arcpy.env.overwriteOutput = True
    arcpy.env.snapRaster = dem
    arcpy.env.cellSize = dem

    desc = arcpy.Describe(dem)
    ext = desc.extent
    xmin0, ymin0, xmax0, ymax0 = ext.XMin, ext.YMin, ext.XMax, ext.YMax

    tile_id = f"r{tile_r:03d}c{tile_c:03d}"
    core_out_tif = os.path.join(out_dir, f"{tile_core_prefix}_{tile_id}.tif")

    # Skip if final tif exists
    if os.path.exists(core_out_tif):
        print(f"{tile_id}: exists (tif), skipping.")
        return

    tile_w = tile_size_m
    tile_h = tile_size_m
    ov = overlap_m

    core_xmin = xmin0 + tile_c * tile_w
    core_xmax = xmin0 + (tile_c + 1) * tile_w
    core_ymin = ymin0 + tile_r * tile_h
    core_ymax = ymin0 + (tile_r + 1) * tile_h

    buf_xmin = core_xmin - ov
    buf_xmax = core_xmax + ov
    buf_ymin = core_ymin - ov
    buf_ymax = core_ymax + ov

    # Clamp to DEM extent
    core_xmin = max(core_xmin, xmin0); core_ymin = max(core_ymin, ymin0)
    core_xmax = min(core_xmax, xmax0); core_ymax = min(core_ymax, ymax0)
    buf_xmin  = max(buf_xmin,  xmin0); buf_ymin  = max(buf_ymin,  ymin0)
    buf_xmax  = min(buf_xmax,  xmax0); buf_ymax  = min(buf_ymax,  ymax0)

    tile_tmp = os.path.join(tmp_root, tile_id)
    os.makedirs(tile_tmp, exist_ok=True)

    dem_buf_tif = os.path.join(tile_tmp, "DEM_BUF.tif")
    hs_tifs = [os.path.join(tile_tmp, f"HS_{az:03d}.tif") for az in azimuths]
    comp_tif = os.path.join(tile_tmp, "HS8_COMP.tif")

    t0 = time.time()
    try:
        # 1) Clip buffered DEM
        clip_rect_to_tif(dem, (buf_xmin, buf_ymin, buf_xmax, buf_ymax), dem_buf_tif)
        if not arcpy.Exists(dem_buf_tif):
            raise RuntimeError(f"{tile_id}: DEM_BUF.tif missing after clip")

        # 2) Parallel hillshades with retries on license-init flakiness
        def run_one(az, out_tif):
            cmd = [sys.executable, hillshade_worker_py, dem_buf_tif, out_tif, str(az), str(altitude), str(z_factor), dem]
            return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Track attempts per azimuth
        attempts = {az: 0 for az in azimuths}
        pending = [(az, out_tif) for az, out_tif in zip(azimuths, hs_tifs)]
        failures_final = []

        while pending:
            # Launch up to hs_workers at once
            procs = []
            queue = pending
            pending = []

            while queue or procs:
                while queue and len(procs) < hs_workers:
                    az, out_tif = queue.pop(0)
                    # Skip if already exists
                    if os.path.exists(out_tif):
                        continue
                    procs.append((az, out_tif, run_one(az, out_tif)))

                still = []
                for az, out_tif, p in procs:
                    ret = p.poll()
                    if ret is None:
                        still.append((az, out_tif, p))
                        continue
                    out, err = p.communicate()
                    msg = (err or out or "").strip()

                    if ret != 0:
                        attempts[az] += 1
                        # Retry if license init error (or generic init)
                        if ("Product License has not been initialized" in msg or
                            "The Product License has not been initialized" in msg) and attempts[az] <= hs_retries:
                            time.sleep(retry_backoff * attempts[az])
                            pending.append((az, out_tif))
                        else:
                            failures_final.append((az, ret, msg))
                    # else success
                procs = still
                if procs:
                    time.sleep(0.1)

            # carry over any still-missing outputs that weren't launched (rare)
            for az, out_tif in queue:
                if not os.path.exists(out_tif):
                    pending.append((az, out_tif))

        if failures_final:
            lines = []
            for az, code, msg in failures_final:
                lines.append(f"az {az} exit {code}: {msg[:250]}")
            raise RuntimeError(f"{tile_id}: hillshade failures:\n" + "\n".join(lines))

        # 3) Composite to TIFF
        arcpy.management.CompositeBands(hs_tifs, comp_tif)

        # 4) Core crop DIRECTLY to final output TIF (outside temp folder)
        clip_rect_to_tif(comp_tif, (core_xmin, core_ymin, core_xmax, core_ymax), core_out_tif)

        if not os.path.exists(core_out_tif):
            raise RuntimeError(f"{tile_id}: core output tif missing after crop")

        dt_sec = time.time() - t0
        print(f"{tile_id}: DONE in {dt_sec:.1f}s")

    finally:
        # Clean up temp folder to avoid filling D:
        if os.path.isdir(tile_tmp):
            shutil.rmtree(tile_tmp, ignore_errors=True)

if __name__ == "__main__":
    main()
'''

with open(HILLSHADE_WORKER_PY, "w", encoding="utf-8") as f:
    f.write(HILLSHADE_WORKER_CODE)

with open(TILE_WORKER_PY, "w", encoding="utf-8") as f:
    f.write(TILE_WORKER_CODE)

# =========================
# Setup env + grid
# =========================
ensure_gdb(D_SCRATCH_GDB)
os.makedirs(D_TMP_ROOT, exist_ok=True)
os.makedirs(E_TIF_OUT_DIR, exist_ok=True)

arcpy.env.scratchWorkspace = D_SCRATCH_GDB
arcpy.env.overwriteOutput = True
arcpy.env.snapRaster = DEM
arcpy.env.cellSize = DEM

desc = arcpy.Describe(DEM)
ext = desc.extent
xmin0, ymin0, xmax0, ymax0 = ext.XMin, ext.YMin, ext.XMax, ext.YMax

ncols = int(math.ceil((xmax0 - xmin0) / float(TILE_SIZE_M)))
nrows = int(math.ceil((ymax0 - ymin0) / float(TILE_SIZE_M)))
total = ncols * nrows

print("============================================================")
print("HS8 14km core -> FINAL 8-band GeoTIFF tiles (NO GDB WRITES) ")
print("============================================================")
print(f"Core: {TILE_SIZE_M/1000:.1f} km | Overlap: {OVERLAP_M/1000:.1f} km | Buffered: {(TILE_SIZE_M+2*OVERLAP_M)/1000:.1f} km")
print(f"Grid: {nrows} x {ncols} = {total}")
print(f"TILE_WORKERS: {TILE_WORKERS} | HS_WORKERS_PER_TILE: {HS_WORKERS_PER_TILE}")
print(f"Temp (D:): {D_TMP_ROOT}")
print(f"Final tiles (E:): {E_TIF_OUT_DIR}")
print("============================================================\n")

# =========================
# Controller: run N tiles at once
# =========================
t0_all = time.time()
tile_queue = [(r, c) for r in range(nrows) for c in range(ncols)]
running = []
done = 0
failed = 0

def launch_tile(r, c):
    cmd = [
        sys.executable, TILE_WORKER_PY,
        DEM,
        E_TIF_OUT_DIR,
        TILE_CORE_PREFIX,
        D_TMP_ROOT,
        HILLSHADE_WORKER_PY,
        str(TILE_SIZE_M),
        str(OVERLAP_M),
        str(ALTITUDE),
        str(Z_FACTOR),
        str(HS_WORKERS_PER_TILE),
        str(HILLSHADE_RETRIES),
        str(RETRY_BACKOFF_SEC),
        str(r),
        str(c)
    ]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

while tile_queue or running:
    while tile_queue and len(running) < TILE_WORKERS:
        r, c = tile_queue.pop(0)
        running.append(((r, c), launch_tile(r, c)))
        print(f"LAUNCH tile r{r:03d}c{c:03d} | running={len(running)} | remaining={len(tile_queue)}")

    still = []
    for (r, c), p in running:
        ret = p.poll()
        if ret is None:
            still.append(((r, c), p))
            continue

        out, err = p.communicate()
        if out.strip():
            print(out.strip())

        if ret != 0:
            failed += 1
            print(f"ERROR tile r{r:03d}c{c:03d} exit={ret}")
            if err.strip():
                print(err.strip()[:800])

        done += 1

        elapsed = time.time() - t0_all
        avg = elapsed / max(1, done)
        remaining = total - done
        eta = avg * remaining
        print(f"PROGRESS: {done}/{total} | failed {failed} | avg/tile {fmt_td(avg)} | ETA {fmt_eta(eta)} ({fmt_td(eta)})")

    running = still
    if running:
        time.sleep(0.2)

print("\nDONE (TIF production).")
print(f"Elapsed: {fmt_td(time.time()-t0_all)}")
print(f"Final tiles folder: {E_TIF_OUT_DIR}")
if failed:
    print(f"WARNING: {failed} tiles failed. Re-run script to retry; it will skip existing TIFFs.")
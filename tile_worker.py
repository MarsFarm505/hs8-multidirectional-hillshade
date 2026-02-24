
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

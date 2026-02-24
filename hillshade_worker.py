
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

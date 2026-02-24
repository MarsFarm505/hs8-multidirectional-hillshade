import os
import time
import arcpy

arcpy.env.overwriteOutput = False

tif_dir = r"E:\DHM_Hillshade8_Tiles\HS8CORE_14km_TIF"
out_gdb = r"E:\DHM_Hillshade8_Tiles\HillshadeTiles_14km.gdb"
delete_tif_after = False

def ensure_gdb(gdb_path: str):
    folder = os.path.dirname(gdb_path)
    os.makedirs(folder, exist_ok=True)
    if not arcpy.Exists(gdb_path):
        arcpy.management.CreateFileGDB(folder, os.path.basename(gdb_path))

def copy_with_retry(src, dst, retries=20, sleep_sec=3):
    for i in range(retries):
        try:
            arcpy.management.CopyRaster(src, dst)
            return True
        except Exception as e:
            msg = str(e)
            if ("Cannot acquire a lock" in msg) or ("schema lock" in msg.lower()):
                time.sleep(sleep_sec)
                continue
            raise
    return False

ensure_gdb(out_gdb)

tifs = sorted([f for f in os.listdir(tif_dir) if f.lower().endswith(".tif")])

print(f"Found {len(tifs)} tif tiles in: {tif_dir}")
print(f"Importing into: {out_gdb}")

imported = skipped = failed = 0

for i, fn in enumerate(tifs, start=1):
    src = os.path.join(tif_dir, fn)
    name = os.path.splitext(fn)[0]
    dst = os.path.join(out_gdb, name)

    if arcpy.Exists(dst):
        skipped += 1
        continue

    ok = copy_with_retry(src, dst)
    if ok:
        imported += 1
        if i % 25 == 0:
            print(f"[{i}/{len(tifs)}] imported={imported} skipped={skipped} failed={failed}")
        if delete_tif_after:
            try:
                os.remove(src)
            except:
                pass
    else:
        failed += 1
        print(f"[{i}/{len(tifs)}] FAILED: {name}")

print("\nDONE")
print(f"Imported: {imported}")
print(f"Skipped : {skipped}")
print(f"Failed  : {failed}")
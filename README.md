\# HS8 Multidirectional Hillshade Pipeline



Parallel preprocessing workflow for generating 8-directional hillshade composites from high-resolution DEMs for machine learning applications.



\## Features



\- 14 km tiling with buffered edges

\- 8 azimuth hillshades (0°–315°)

\- Two-level parallelism (tile-level + intra-tile)

\- Lock-free GeoTIFF output architecture

\- Safe serial import to FileGDB



\## Requirements



\- ArcGIS Pro (Spatial Analyst license)

\- Python (ArcGIS Pro environment)

\- High-speed local storage recommended



\## Outputs



\- 8-band GeoTIFF tiles

\- Optional serial import to FGDB



\## Citation



If used in academic work, please cite:



Williams et al., in prep.


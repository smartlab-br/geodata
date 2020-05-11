#!/bin/bash
# SETUP
# The first parameter is a flag whether the already existing files should be skipped
SKIP_EXISTING=$1 # True/False

# STEP 1 - Import geojson files from IBGE.
python3 geoimporter.py "$SKIP_EXISTING"

# STEP 2 - Convert shapefiles in the shapes directory into geojson.
# WARNING - You should validate and fix the crossing vectors, otherwise
# the conversion to topojson will automatically remove some polygons.
python3 shp2geo.py "$SKIP_EXISTING"

# STEP 3 - Convert any geojson file in the geojson directory to topojson.
# WARNING - All shapes with conversion issues will be logged to files. Check
# the log directory for more information. Some issues, such as crossing vectors,
# should be fixed in the shape file, thus allowing a smooth conversion to
# geojson an then topojson.
python3 geo2topo.py "$SKIP_EXISTING"
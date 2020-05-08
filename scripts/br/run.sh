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

# STEP 3 - Generate new geojsons by combining levels the new 
# analysis units introduced in step 2
# python3 create_subgeo.py "$SKIP_EXISTING"

# STEP 4 - Convert any geojson file in the geojson directory to topojson.
python3 geo2topo.py "$SKIP_EXISTING"
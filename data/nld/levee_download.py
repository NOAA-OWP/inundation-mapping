#!/usr/bin/env python3
import sys
import geopandas as gpd

sys.path.append('/foss_fim/data')
from esri import ESRI_REST

# Query REST service to download levee 'system routes'
nld_url = "https://ags03.sec.usace.army.mil/server/rest/services/NLD2_PUBLIC/FeatureServer/15/query"
levees = ESRI_REST.query(nld_url)

# Load WBD to do a spatial join


# Add levee protected polygons to this file??
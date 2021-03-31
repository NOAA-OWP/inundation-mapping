#!/usr/bin/env python3

import os

# Projections.
#PREP_PROJECTION = "+proj=aea +datum=NAD83 +x_0=0.0 +y_0=0.0 +lon_0=96dW +lat_0=23dN +lat_1=29d30'N +lat_2=45d30'N +towgs84=-0.9956000824677655,1.901299877314078,0.5215002840524426,0.02591500053005733,0.009425998542707753,0.01159900118427752,-0.00062000005129903 +no_defs +units=m"
PREP_PROJECTION = 'PROJCS["USA_Contiguous_Albers_Equal_Area_Conic_USGS_version",GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.2572221010042,AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4269"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",29.5],PARAMETER["standard_parallel_2",45.5],PARAMETER["latitude_of_center",23],PARAMETER["longitude_of_center",-96],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]'
VIZ_PROJECTION ='PROJCS["WGS_1984_Web_Mercator_Auxiliary_Sphere",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Mercator_Auxiliary_Sphere"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",0.0],PARAMETER["Standard_Parallel_1",0.0],PARAMETER["Auxiliary_Sphere_Type",0.0],UNIT["Meter",1.0]]'
# -- Data URLs-- #
NHD_URL_PARENT = r'https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHDPlusHR/Beta/GDB/'
NWM_HYDROFABRIC_URL = r'http://www.nohrsc.noaa.gov/pub/staff/keicher/NWM_live/web/data_tools/NWM_channel_hydrofabric.tar.gz'  # Temporary
WBD_NATIONAL_URL = r'https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/WBD/National/GDB/WBD_National_GDB.zip'
WBD_HU2_URL_PARENT = r'http://prd-tnm.s3-website-us-west-2.amazonaws.com/?prefix=StagedProducts/Hydrography/WBD/HU2/GDB'

# -- Prefixes and Suffixes -- #
NHD_URL_PREFIX = 'NHDPLUS_H_'
NHD_RASTER_URL_SUFFIX = '_HU4_RASTER.7z'
NHD_VECTOR_URL_SUFFIX = '_HU4_GDB.zip'
NHD_RASTER_EXTRACTION_PREFIX = 'HRNHDPlusRasters'
NHD_RASTER_EXTRACTION_SUFFIX = 'elev_cm.tif'

NHD_VECTOR_EXTRACTION_PREFIX = 'NHDPLUS_H_'
NHD_VECTOR_EXTRACTION_SUFFIX = '_HU4_GDB.zip'

# -- Field Names -- #
FIM_ID = 'fimid'

# -- Other -- #
CONUS_STATE_LIST = {"AL", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
                    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
                    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
                    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "PR", "RI", "SC",
                    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"}

OVERWRITE_WBD = 'OVERWRITE_WBD'
OVERWRITE_NHD = 'OVERWRITE_NHD'
OVERWRITE_ALL = 'OVERWRITE_ALL'

## Input Paths and Directories
# Directories
src_dir = '/foss_fim/src'
input_dir ='data/inputs'
nhdplus_rasters_dir = os.path.join(input_dir,'nhdplus_rasters')
nhdplus_vectors_dir = os.path.join(input_dir,'nhdplus_vectors')
nwm_hydrofabric_dir = os.path.join(input_dir,'nwm_hydrofabric')
wbd_dir = os.path.join(input_dir,'wbd')
ahps_dir = os.path.join(input_dir,'ahp_sites')
nhdplus_vectors_aggregate_dir = os.path.join(input_dir,'nhdplus_vectors_aggregate')

# File Paths
wbd_filename = os.path.join(wbd_dir, 'WBD_National.gpkg')
nwm_streams_fr_filename = os.path.join(nwm_hydrofabric_dir,'nwm_flows.gpkg')
nwm_streams_ms_filename = os.path.join(nwm_hydrofabric_dir,'nwm_flows_ms.gpkg')
nwm_headwaters_filename = os.path.join(nwm_hydrofabric_dir,'nwm_headwaters.gpkg')
nwm_huc4_intersections_ms_filename = os.path.join(nwm_hydrofabric_dir,'nwm_ms_huc4_intersections.gpkg')
nwm_huc4_intersections_fr_filename = os.path.join(nwm_hydrofabric_dir,'nwm_fr_huc4_intersections.gpkg')
ahps_headwaters_filename = os.path.join(ahps_dir,'nws_lid.gpkg')

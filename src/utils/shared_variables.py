import os

from dotenv import load_dotenv
from pyproj import CRS


load_dotenv('/foss_fim/src/bash_variables.env')

# -- Projections: EPSG 5070-- #
PREP_PROJECTION = (
    'PROJCRS["NAD83 / Conus Albers",BASEGEOGCRS["NAD83",DATUM["North American Datum 1983",'
    'ELLIPSOID["GRS 1980",6378137,298.257222101,LENGTHUNIT["metre",1]]],PRIMEM["Greenwich",'
    '0,ANGLEUNIT["degree",0.0174532925199433]],ID["EPSG",4269]],CONVERSION["Conus Albers",'
    'METHOD["Albers Equal Area",ID["EPSG",9822]],PARAMETER["Latitude of false origin",23,'
    'ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8821]],PARAMETER["Longitude of false '
    'origin",-96,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8822]],PARAMETER["Latitude '
    'of 1st standard parallel",29.5,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8823]],'
    'PARAMETER["Latitude of 2nd standard parallel",45.5,ANGLEUNIT["degree",0.0174532925199433]'
    ',ID["EPSG",8824]],PARAMETER["Easting at false origin",0,LENGTHUNIT["metre",1],ID["EPSG",'
    '8826]],PARAMETER["Northing at false origin",0,LENGTHUNIT["metre",1],ID["EPSG",8827]]],'
    'CS[Cartesian,2],AXIS["easting (X)",east,ORDER[1],LENGTHUNIT["metre",1]],'
    'AXIS["northing (Y)",north,ORDER[2],LENGTHUNIT["metre",1]],USAGE[SCOPE["Data analysis and '
    'small scale data presentation for contiguous lower 48 states."],AREA["United States (USA) '
    '- CONUS onshore - Alabama; Arizona; Arkansas; California; Colorado; Connecticut; Delaware'
    '; Florida; Georgia; Idaho; Illinois; Indiana; Iowa; Kansas; Kentucky; Louisiana; Maine; '
    'Maryland; Massachusetts; Michigan; Minnesota; Mississippi; Missouri; Montana; Nebraska; '
    'Nevada; New Hampshire; New Jersey; New Mexico; New York; North Carolina; North Dakota; '
    'Ohio; Oklahoma; Oregon; Pennsylvania; Rhode Island; South Carolina; South Dakota; '
    'Tennessee; Texas; Utah; Vermont; Virginia; Washington; West Virginia; Wisconsin; '
    'Wyoming."],BBOX[24.41,-124.79,49.38,-66.91]],ID["EPSG",5070]]'
)
DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')
ALASKA_CRS = os.getenv('ALASKA_CRS')
PREP_CRS = CRS(PREP_PROJECTION)
VIZ_PROJECTION = (
    'PROJCS["WGS_1984_Web_Mercator_Auxiliary_Sphere",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",'
    'SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",'
    '0.0174532925199433]],PROJECTION["Mercator_Auxiliary_Sphere"],PARAMETER["False_Easting",0.0],'
    'PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",0.0],PARAMETER["Standard_Parallel_1",0.0],'
    'PARAMETER["Auxiliary_Sphere_Type",0.0],UNIT["Meter",1.0]]'
)  # 3857

# -- Data URLs-- #
NHD_URL_PARENT = r'https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHDPlusHR/Beta/GDB/'
# Temporary
NWM_HYDROFABRIC_URL = (
    r'http://www.nohrsc.noaa.gov/pub/staff/keicher/NWM_live/web/data_tools/NWM_channel_hydrofabric.tar.gz'
)
WBD_NATIONAL_URL = (
    r'https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/WBD/National/GDB/WBD_National_GDB.zip'
)


# -- Prefixes and Suffixes -- #
NHD_URL_PREFIX = 'NHDPLUS_H_'
NHD_RASTER_URL_SUFFIX = '_HU4_RASTER.7z'
NHD_VECTOR_URL_SUFFIX = '_HU4_GDB.zip'

# NHD_RASTER_EXTRACTION_PREFIX = 'HRNHDPlusRasters'
# NHD_RASTER_EXTRACTION_SUFFIX = 'elev_cm.tif'

NHD_VECTOR_EXTRACTION_PREFIX = 'NHDPLUS_H_'
NHD_VECTOR_EXTRACTION_SUFFIX = '_HU4_GDB.zip'

## added here bc prefixs and suffixes just aren't necessary in python3
nhd_raster_url_template = (
    "https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHDPlusHR/Beta/GDB/"
    "NHDPLUS_H_{}_HU4_RASTER.7z"
)
nhd_vector_url_template = (
    "https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHDPlusHR/Beta/GDB/NHDPLUS_H_{}_HU4_GDB.zip"
)

# --- Values ---- #
elev_raster_ndv = -9999.0

UNIT_ERRORS_MIN_NUMBER_THRESHOLD = 10
UNIT_ERRORS_MIN_PERCENT_THRESHOLD = 10  # as in 10% (should be a whole number)

# -- Field Names -- #
FIM_ID = 'fimid'

# -- High stream density HUCs that require mitigation (currently just in Alaska) -- #
HIGH_STREAM_DENSITY_HUCS = {'19020602', '19020503', '19020402', '19020104'}  # stream density 1.5+

MEDIUM_HIGH_STREAM_DENSITY_HUCS = {  # stream density between 0.5 and 1.5
    '19020504',
    '19020502',
    '19020601',
    '19020505',
    '19020101',  # 1.0-1.5
    '19020102',
    '19020501',
    '19020301',
    '19020401',
    '19020302',
    '19020103',
    '19020202',  # 0.5-1.0
}


# Rating Curve Adjustment (local calibration) variables
USGS_CALB_TRACE_DIST = 8.0  # distance in km to walk the network and perform USGS rating calibation for SRCs
DOWNSTREAM_THRESHOLD = 8.0  # distance in km to propogate new roughness values downstream (group mean)
ROUGHNESS_MAX_THRESH = 0.8  # max allowable adjusted roughness value (void values larger than this)
ROUGHNESS_MIN_THRESH = 0.001  # min allowable adjusted roughness value (void values smaller than this)

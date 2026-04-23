import os


# Environmental variables and constants.
TEST_CASES_DIR = r"/data/test_cases/"
PREVIOUS_FIM_DIR = r"/data/previous_fim"
OUTPUTS_DIR = os.environ["outputsDir"]
INPUTS_DIR = os.environ["inputsDir"]
AHPS_BENCHMARK_CATEGORIES = ["usgs", "nws"]
FR_BENCHMARK_CATEGORIES = ["ble", "ifc"]
BLE_MAGNITUDE_LIST = ["100yr", "500yr"]
IFC_MAGNITUDE_LIST = ["2yr", "5yr", "10yr", "25yr", "50yr", "100yr", "200yr", "500yr"]
AHPS_MAGNITUDE_LIST = ["action", "minor", "moderate", "major"]
RAS2FIM_MAGNITUDE_LIST = ["2yr", "5yr", "10yr", "25yr", "50yr", "100yr"]

MAGNITUDE_DICT = {
    "ble": BLE_MAGNITUDE_LIST,
    "ifc": IFC_MAGNITUDE_LIST,
    "usgs": AHPS_MAGNITUDE_LIST,
    "nws": AHPS_MAGNITUDE_LIST,
    "ras2fim": RAS2FIM_MAGNITUDE_LIST,
}
PRINTWORTHY_STATS = [
    "CSI",
    "TPR",
    "TNR",
    "FAR",
    "MCC",
    "TP_area_km2",
    "FP_area_km2",
    "TN_area_km2",
    "FN_area_km2",
    "contingency_tot_area_km2",
    "TP_perc",
    "FP_perc",
    "TN_perc",
    "FN_perc",
]
GO_UP_STATS = ["CSI", "TPR", "MCC", "TN_area_km2", "TP_area_km2", "TN_perc", "TP_perc", "TNR"]
GO_DOWN_STATS = ["FAR", "FN_area_km2", "FP_area_km2", "FP_perc", "FN_perc", "PND"]

# Variables for eval_plots.py
BAD_SITES = [
    "baki3",  # USGS: ratio of evaluated vs domain is very low
    "cart2",  # NWS:  added by request
    "eagi1",  # NWS:  ratio of evaluated vs domain is very low
    "efdn7",  # NWS:  mainstems does not extend sufficiently upstream (~30%)
    "erwn6",  # NWS:  ratio of evaluated vs domain is very low
    "grfi2",  # USGS: incorrect location
    "kcdm7",  # USGS: incorrect location
    "ksdm7",  # USGS: masked
    "levk1",  # NWS:  Incorrect feature_id assigned from WRDS, this has been corrected
    "lkcm7",  # NWS:  masked
    "loun7",  # NWS:  benchmark is not consistent between thresholds
    "lrlm7",  # NWS:  masked
    "mcri2",  # USGS: incorrect location
    "mtao1",  # USGS: ratio of evaluated vs domain is very low
    "roun6",  # USGS: ratio of evaluated vs domain is very low
    "rwdn4",  # Both: no mainstems in vicinity
    "vcni3",  # USGS: ratio of evaluated vs domain is very low
]
DISCARD_AHPS_QUERY = "not flow.isnull() & masked_perc<97 & not nws_lid in @BAD_SITES"

elev_raster_ndv = -9999.0

# Colors.
ENDC = "\033[m"
TGREEN_BOLD = "\033[32;1m"
TGREEN = "\033[32m"
TRED_BOLD = "\033[31;1m"
TWHITE = "\033[37m"
WHITE_BOLD = "\033[37;1m"
CYAN_BOLD = "\033[36;1m"

# USGS gages acceptance criteria. Likely not constants, so not using all caps.
# ANY CHANGE TO THESE VALUES SHOULD WARRANT A CODE VERSION CHANGE
# https://help.waterdata.usgs.gov/code/coord_acy_cd_query?fmt=html
acceptable_coord_acc_code_list = ["H", "1", "5", "S", "R", "B", "C", "D", "E", 5, 1]
# https://help.waterdata.usgs.gov/code/coord_meth_cd_query?fmt=html
acceptable_coord_method_code_list = ["C", "D", "W", "X", "Y", "Z", "N", "M", "L", "G", "R", "F", "S"]
# https://help.waterdata.usgs.gov/codes-and-parameters/codes#SI
acceptable_alt_acc_thresh = 1.0
# https://help.waterdata.usgs.gov/code/alt_meth_cd_query?fmt=html
acceptable_alt_meth_code_list = ["A", "D", "F", "G", "I", "J", "L", "N", "R", "U", "W", "X", "Y", "Z"]
# https://help.waterdata.usgs.gov/code/site_tp_query?fmt=html
acceptable_site_type_list = ["ST"]


# Lists of acceptable spellings for CRSs and VCSs, for use in tools_shared_functions.py when checking for typos in CRSs and VCSs.
# These lists are not exhaustive, but are based on the most common typos found in the test cases.

UNKNOWN_DATUM_SPELLINGS = ['UNKNOWN', 'UNK', 'UNKN', 'NONE', '', 'UN']

ACCEPTED_NAD27_SPELLINGS = [
    '1927',
    'NAD27',
    'NAAD27',
    'NAD 27',
    'NAD-27',
    'NA 1927',
    'NAD1927',
    'NAD 1927',
    'NAD29',  # NAD + wrong year
    'NAD1929',
    'NAD 1929',
    '4267',  # References to EPSG:4267, which means NAD27
    'EPSG:4267',
]

ACCEPTED_NGVD29_SPELLINGS = [
    '1929',
    'NVGD',
    'NGVD',
    'NGVD 29',
    'NGVD29',
    'NVD 1929',
    'NGVD1929',
    'NGVD1927',
    '1929 NGV',
    '1929 NGVD',
    'NGDV 1929',
    'NGVD 1929',
    '1927 NGVD',
    'NGVD 1929',
    'NGVD,1929',
    'NGVD OF 1929',
    'USGS NAD 1929',
]


ACCEPTED_NAD83_SPELLINGS = [
    '1983',
    'NAD83',
    'NAD 83',
    'NAD1983',
    'NAD 1983',
    'NADA 1983',
    'NAD88',  # NAD + wrong year
    'NAD87',
    'NAD893',
    'NAD 88',
    'NAD 1988',
    '4269',  # References to EPSG:4269, which means NAD83
    'EPSG:4269',
]

ACCEPTED_NAVD88_SPELLINGS = [
    '1988',
    'NAD88',
    'NAV83',
    'NAV-88',
    'NAVD83',
    'NAVD88',
    'NAVD 88',
    'NAVD-88',
    'NAVD 1988',
]

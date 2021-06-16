import os

# Environmental variables and constants.
TEST_CASES_DIR = r'/data/test_cases/'
PREVIOUS_FIM_DIR = r'/data/previous_fim'
OUTPUTS_DIR = os.environ['outputDataDir']
INPUTS_DIR = r'/data/inputs'
AHPS_BENCHMARK_CATEGORIES = ['usgs', 'nws']
FR_BENCHMARK_CATEGORIES = ['ble', 'ifc']
BLE_MAGNITUDE_LIST = ['100yr', '500yr']
IFC_MAGNITUDE_LIST = ['2yr', '5yr', '10yr', '25yr', '50yr', '100yr', '200yr', '500yr']
PRINTWORTHY_STATS = ['CSI', 'TPR', 'TNR', 'FAR', 'MCC', 'TP_area_km2', 'FP_area_km2', 'TN_area_km2', 'FN_area_km2', 'contingency_tot_area_km2', 'TP_perc', 'FP_perc', 'TN_perc', 'FN_perc']
GO_UP_STATS = ['CSI', 'TPR', 'MCC', 'TN_area_km2', 'TP_area_km2', 'TN_perc', 'TP_perc', 'TNR']
GO_DOWN_STATS = ['FAR', 'FN_area_km2', 'FP_area_km2', 'FP_perc', 'FN_perc']

# Variables for eval_plots.py
BAD_SITES = ['grfi2',# Bad crosswalk from USGS to AHPS
             'ksdm7',# Large areas entirely within levee protected area (CSI of 0)
             'hohn4',# Mainstems did not extend far enough upstream
             'rwdn4',# Mainstems did not extend far enough upstream.
             'efdn7',# Limited upstream mapping
             'kilo1',# Limited upstream mapping
             'chin7',# Limited upstream mapping
             'segt2',# Limited upstream mapping
             'eagi1',# Missing large portions of FIM
             'levk1',# Missing large portions of FIM
             'trbf1']# Limited upstream mapping
DISCARD_AHPS_QUERY = "not flow.isnull() & masked_perc<97 & not nws_lid in @BAD_SITES"

# Colors.
ENDC = '\033[m'
TGREEN_BOLD = '\033[32;1m'
TGREEN = '\033[32m'
TRED_BOLD = '\033[31;1m'
TWHITE = '\033[37m'
WHITE_BOLD = '\033[37;1m'
CYAN_BOLD = '\033[36;1m'

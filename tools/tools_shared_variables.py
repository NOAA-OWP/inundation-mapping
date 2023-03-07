import os

# Environmental variables and constants.
TEST_CASES_DIR = r'/data/test_cases/'
PREVIOUS_FIM_DIR = r'/data/previous_fim'
OUTPUTS_DIR = os.environ['outputsDir']
INPUTS_DIR = r'/data/inputs'
AHPS_BENCHMARK_CATEGORIES = ['usgs', 'nws']
FR_BENCHMARK_CATEGORIES = ['ble', 'ifc']
BLE_MAGNITUDE_LIST = ['100yr', '500yr']
IFC_MAGNITUDE_LIST = ['2yr', '5yr', '10yr', '25yr', '50yr', '100yr', '200yr', '500yr']
AHPS_MAGNITUDE_LIST = ['action', 'minor', 'moderate', 'major']
RAS2FIM_MAGNITUDE_LIST = ['2yr', '5yr', '10yr', '25yr', '50yr', '100yr']

MAGNITUDE_DICT = {'ble': BLE_MAGNITUDE_LIST, 'ifc': IFC_MAGNITUDE_LIST, 'usgs': AHPS_MAGNITUDE_LIST, 'nws': AHPS_MAGNITUDE_LIST, 'ras2fim': RAS2FIM_MAGNITUDE_LIST}
PRINTWORTHY_STATS = ['CSI', 'TPR', 'TNR', 'FAR', 'MCC', 'TP_area_km2', 'FP_area_km2', 'TN_area_km2', 'FN_area_km2', 'contingency_tot_area_km2', 'TP_perc', 'FP_perc', 'TN_perc', 'FN_perc']
GO_UP_STATS = ['CSI', 'TPR', 'MCC', 'TN_area_km2', 'TP_area_km2', 'TN_perc', 'TP_perc', 'TNR']
GO_DOWN_STATS = ['FAR', 'FN_area_km2', 'FP_area_km2', 'FP_perc', 'FN_perc', 'PND']

# Variables for eval_plots.py
BAD_SITES = [
            'baki3', #USGS: ratio of evaluated vs domain is very low
            'cpei3', #USGS: mainstems does not extend sufficiently upstream (~35%), significant masking upstream
            'eagi1', #NWS:  ratio of evaluated vs domain is very low
            'efdn7', #NWS:  mainstems does not extend sufficiently upstream (~30%)
            'erwn6', #NWS:  ratio of evaluated vs domain is very low
            'grfi2', #USGS: incorrect location
            'hohn4', #Both: no mainstems in vicinity
            'kcdm7', #USGS: incorrect location
            'kilo1', #Both: mainstems does not extend sufficiently upstream (~20%)
            'ksdm7', #USGS: masked
            'levk1', #NWS:  Incorrect feature_id assigned from WRDS, this has been corrected
            'lkcm7', #NWS:  masked 
            'loun7', #NWS:  benchmark is not consistent between thresholds
            'lrlm7', #NWS:  masked
            'mcri2', #USGS: incorrect location
            'monv1', #NWS:  mainstems does not extend sufficiently upstream (~30%)
            'mtao1', #USGS: ratio of evaluated vs domain is very low
            'nhso1', #USGS: mainstems does not extend sufficiently upstream (45%)
            'nmso1', #Both: mainstems does not extend sufficiently upstream
            'pori3', #USGS: mainstems does not extend sufficiently upstream
            'ptvn6', #Both: mainstems does not extend sufficiently upstream (50%)
            'roun6', #USGS: ratio of evaluated vs domain is very low
            'rwdn4', #Both: no mainstems in vicinity
            'selt2', #NWS:  mainstems does not extend sufficiently upstream (~30%)
            'sweg1', #Both: mainstems does not extend sufficiently upstream (~30%)
            'vcni3', #USGS: ratio of evaluated vs domain is very low
            'watw3', #NWS:  mainstems does not extend sufficiently upstream (~30%)
            'weat2', #NWS:  mainstems does not extend sufficiently upstream (~50%)
            'wkew3' #NWS:  mainstems does not extend sufficiently upstream (~45%)    
            ]
DISCARD_AHPS_QUERY = "not flow.isnull() & masked_perc<97 & not nws_lid in @BAD_SITES"

elev_raster_ndv = -9999.0

# Colors.
ENDC = '\033[m'
TGREEN_BOLD = '\033[32;1m'
TGREEN = '\033[32m'
TRED_BOLD = '\033[31;1m'
TWHITE = '\033[37m'
WHITE_BOLD = '\033[37;1m'
CYAN_BOLD = '\033[36;1m'

# USGS gages acceptance criteria. Likely not constants, so not using all caps.
# ANY CHANGE TO THESE VALUES SHOULD WARRANT A CODE VERSION CHANGE
# https://help.waterdata.usgs.gov/code/coord_acy_cd_query?fmt=html
acceptable_coord_acc_code_list = ['H','1','5','S','R','B','C','D','E']
# https://help.waterdata.usgs.gov/code/coord_meth_cd_query?fmt=html
acceptable_coord_method_code_list = ['C','D','W','X','Y','Z','N','M','L','G','R','F','S']
# https://help.waterdata.usgs.gov/codes-and-parameters/codes#SI
acceptable_alt_acc_thresh = 1.0
# https://help.waterdata.usgs.gov/code/alt_meth_cd_query?fmt=html
acceptable_alt_meth_code_list = ['A','D','F','I','J','L','N','R','W','X','Y','Z']
# https://help.waterdata.usgs.gov/code/site_tp_query?fmt=html
acceptable_site_type_list = ['ST']

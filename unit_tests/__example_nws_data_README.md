
## Sample unit tests - for demo purposes only#

    Used to demonstrate __example_nws_data_unittests.py only.
	 The __example_nws_data_unittest.py files shows how success and fails method look like as well as things like using actual results, expected results and asserts.

# Here is a unit test being used in LISFLOOD

`Here are some example code changes that would be in place if the __example files were valid (doesn't actually exist).`

- There is a new variable added to src/utils/shared_variables -> DATA_SOURCE_PATH_NWS_LID_GPKG = "/data/inputs/ahp_sites/01_27_22/nws_lid.gpkg"

- A python file exists in src called nws_data.py (again.. doesn't actually)

Here are the contents of that pretend file (for this repo)

+++++++++++++++++++++++++++++++++++++++++++

import geopandas as gpd
import utils.shared_variables

def get_huc8_by_nws_lid(nws_lid):

    '''
    OVERVIEW:
        By passing in a nws_lid (also called a ahps id), it will open the ahps_sites/nws_lid.gpkg, find the record and return the huc8 value.

    INPUT PARAMETERS:
        - nws_lid: a string value (case sensitive) which should match the nws_lids.gpkg -> nws_lid column.

    PROCESSING / VALIDATION:
        - IF the nws record is not found, an exception will be raised.
        - The nws_lid.gpkg path is defined by the new src/utils/data_sources.env parameter.
        - If more than one nws record was found by nws_lid, it is assumed as bad data and an exception will be raised.
        - nws_lid will be changed to upper case to match the database

    RETURNS:
       The discovered HUC8 value.
    '''

    # -----------------------------
    # VALIDATION OF INCOMING VALUES

    # Ensure the nws_lid value is not "None" or empty
    if (not nws_lid):
        raise Exception("nw_lid parameter appears to be empty or invalid")

    # Ensure there are no spaces inside or on either end of the nws_lid value
    if (" " in nws_lid):
        raise Exception("nws_lid parameter appears to have invalid spaces either in or around the value. Please check the incoming param value.")


    # -----------------------------
    # PROCESSING:

    nws_lid = nws_lid.upper()

    # get the huc from the nws_lid.gpkg
    nws_lid_data = gpd.read_file(utils.shared_variables.DATA_SOURCE_PATH_NWS_LID_GPKG)

    nws_record = nws_lid_data.loc[nws_lid_data['nws_lid'] == nws_lid]

    if nws_record.empty:
        raise Exception("No nws_lid record found for nws_id of " + nws_lid)

    if len(nws_record) > 1:
        raise Exception("More than one nws_lid record was found for nws_id of " + nws_lid + ". Check the nws_lid.gpkg as there may be some bad data.")

    huc8 = nws_record['HUC8'].values[0]
    if (not huc8):
        raise Exception("nws_lid record found but no huc value found. Check nws_lid.gpkg")

    return(huc8)


+++++++++++++++++++++++++++++++++++++++++++

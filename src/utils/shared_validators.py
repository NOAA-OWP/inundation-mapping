#!/usr/bin/env python3

####################################################################
"""
This script is for validation of inputs and objects that can be used in py files.
"""


# -------------------------------------------------
def is_valid_crs(crs):
    """
    Processing:
        - not counting case, the first for chars must be either EPSG or ESRI
        - crs number must be between 4 and 6 digits
    Inputs:
        - crs (str): in pattern such as EPSG:2277

    Returns three values
        - First is bool (True or False). True meaning it is a valid crs
        - Second is a string, which might be empty, but if failed, then this will be the reason for failure
        - Third is the raw crs number in case you need it. ie) 2277 or 107239

        It is up to the calling code to decide if an exception should be raised.

    Usage:
        valid_crs, err_msg, crs_number = is_valid_crs(arg_crs)

        (if you choose to hand it this way....)
        if (valid_crs == False):
            raise ValueError(err_msg)

    """

    err_msg = ""

    if crs == "":
        err_msg = "The crs value can not be blank or empty"
        return False, err_msg, ""

    if ":" not in crs:
        err_msg = "crs appears to be invalid (missing colon)"
        return False, err_msg, ""

    crs_seg = crs.split(":")

    if len(crs_seg) != 2:
        err_msg = "crs appears to be invalid"
        return False, err_msg, ""

    crs_type = str(crs_seg[0]).upper()
    if (crs_type != "ESRI") and (crs_type != "EPSG"):
        err_msg = "crs type is not EPSG OR ESRI (not case-senstive)"
        return False, err_msg, ""

    crs_number = str(crs_seg[1])

    if crs_number.isnumeric() is False:
        err_msg = "value after the colon is not a number. ie) 2277"
        return False, err_msg, ""

    if (len(crs_number) < 4) or (len(crs_number) > 6):
        err_msg = "crs_number portion is expected to be between 4 and 6 digits"
        return False, err_msg, ""

    if crs_number[0] == "0":
        err_msg = "crs_number portion can not start with a zero"
        return False, err_msg, ""

    return True, "", crs_number

#!/usr/bin/env python3

import os
import json
import rasterio
import pandas as pd
import geopandas as gpd
import requests
import numpy as np
import pathlib
from pathlib import Path
import rasterio.shutil
from rasterio.warp import calculate_default_transform, reproject, Resampling
import rasterio.crs
from rasterio import features
from shapely.geometry import shape
from shapely.geometry import Polygon
from shapely.geometry import MultiPolygon


def check_for_regression(stats_json_to_test, previous_version, previous_version_stats_json_path, regression_test_csv=None):

    difference_dict = {}

    # Compare stats_csv to previous_version_stats_file
    stats_dict_to_test = json.load(open(stats_json_to_test))
    previous_version_stats_dict = json.load(open(previous_version_stats_json_path))

    for stat, value in stats_dict_to_test.items():
        previous_version_value = previous_version_stats_dict[stat]
        stat_value_diff = value - previous_version_value
        difference_dict.update({stat + '_diff': stat_value_diff})

    return difference_dict


def compute_contingency_stats_from_rasters(predicted_raster_path, benchmark_raster_path, agreement_raster=None, stats_csv=None, stats_json=None, mask_values=None, stats_modes_list=['total_area'], test_id='', mask_dict={}):
    """
    This function contains FIM-specific logic to prepare raster datasets for use in the generic get_contingency_table_from_binary_rasters() function.
    This function also calls the generic compute_stats_from_contingency_table() function and writes the results to CSV and/or JSON, depending on user input.

    Args:
        predicted_raster_path (str): The path to the predicted, or modeled, FIM extent raster.
        benchmark_raster_path (str): The path to the benchmark, or truth, FIM extent raster.
        agreement_raster (str): Optional. An agreement raster will be written to this path. 0: True Negatives, 1: False Negative, 2: False Positive, 3: True Positive.
        stats_csv (str): Optional. Performance statistics will be written to this path. CSV allows for readability and other tabular processes.
        stats_json (str): Optional. Performance statistics will be written to this path. JSON allows for quick ingestion into Python dictionary in other processes.

    Returns:
        stats_dictionary (dict): A dictionary of statistics produced by compute_stats_from_contingency_table(). Statistic names are keys and statistic values are the values.
    """

    # Get cell size of benchmark raster.
    raster = rasterio.open(predicted_raster_path)
    t = raster.transform
    cell_x = t[0]
    cell_y = t[4]
    cell_area = abs(cell_x*cell_y)

    # Get contingency table from two rasters.
    contingency_table_dictionary = get_contingency_table_from_binary_rasters(benchmark_raster_path, predicted_raster_path, agreement_raster, mask_values=mask_values, mask_dict=mask_dict)

    stats_dictionary = {}

    for stats_mode in contingency_table_dictionary:
        true_negatives = contingency_table_dictionary[stats_mode]['true_negatives']
        false_negatives = contingency_table_dictionary[stats_mode]['false_negatives']
        false_positives = contingency_table_dictionary[stats_mode]['false_positives']
        true_positives = contingency_table_dictionary[stats_mode]['true_positives']
        masked_count = contingency_table_dictionary[stats_mode]['masked_count']
        file_handle = contingency_table_dictionary[stats_mode]['file_handle']

        # Produce statistics from continency table and assign to dictionary. cell_area argument optional (defaults to None).
        mode_stats_dictionary = compute_stats_from_contingency_table(true_negatives, false_negatives, false_positives, true_positives, cell_area, masked_count)

        # Write the mode_stats_dictionary to the stats_csv.
        if stats_csv != None:
            stats_csv = os.path.join(os.path.split(stats_csv)[0], file_handle + '_stats.csv')
            df = pd.DataFrame.from_dict(mode_stats_dictionary, orient="index", columns=['value'])
            df.to_csv(stats_csv)

        # Write the mode_stats_dictionary to the stats_json.
        if stats_json != None:
            stats_json = os.path.join(os.path.split(stats_csv)[0], file_handle + '_stats.json')
            with open(stats_json, "w") as outfile:
                json.dump(mode_stats_dictionary, outfile)

        stats_dictionary.update({stats_mode: mode_stats_dictionary})

    return stats_dictionary


def profile_test_case_archive(archive_to_check, magnitude, stats_mode):
    """
    This function searches multiple directories and locates previously produced performance statistics.

    Args:
        archive_to_check (str): The directory path to search.
        magnitude (str): Because a benchmark dataset may have multiple magnitudes, this argument defines
                               which magnitude is to be used when searching for previous statistics.
    Returns:
        archive_dictionary (dict): A dictionary of available statistics for previous versions of the domain and magnitude.
                                  {version: {agreement_raster: agreement_raster_path, stats_csv: stats_csv_path, stats_json: stats_json_path}}
                                  *Will only add the paths to files that exist.

    """

    archive_dictionary = {}

    # List through previous version and check for available stats and maps. If available, add to dictionary.
    available_versions_list = os.listdir(archive_to_check)

    if len(available_versions_list) == 0:
        print("Cannot compare with -c flag because there are no data in the previous_versions directory.")
        return

    for version in available_versions_list:
        version_magnitude_dir = os.path.join(archive_to_check, version, magnitude)
        stats_json = os.path.join(version_magnitude_dir, stats_mode + '_stats.json')

        if os.path.exists(stats_json):
            archive_dictionary.update({version: {'stats_json': stats_json}})

    return archive_dictionary


def compute_stats_from_contingency_table(true_negatives, false_negatives, false_positives, true_positives, cell_area=None, masked_count=None):
    """
    This generic function takes contingency table metrics as arguments and returns a dictionary of contingency table statistics.
    Much of the calculations below were taken from older Python files. This is evident in the inconsistent use of case.

    Args:
        true_negatives (int): The true negatives from a contingency table.
        false_negatives (int): The false negatives from a contingency table.
        false_positives (int): The false positives from a contingency table.
        true_positives (int): The true positives from a contingency table.
        cell_area (float or None): This optional argument allows for area-based statistics to be calculated, in the case that
                                   contingency table metrics were derived from areal analysis.

    Returns:
        stats_dictionary (dict): A dictionary of statistics. Statistic names are keys and statistic values are the values.
                                 Refer to dictionary definition in bottom of function for statistic names.

    """

    import numpy as np

    total_population = true_negatives + false_negatives + false_positives + true_positives

    # Basic stats.
#    Percent_correct = ((true_positives + true_negatives) / total_population) * 100
#    pod             = true_positives / (true_positives + false_negatives)

    try:
        FAR = false_positives / (true_positives + false_positives)
    except ZeroDivisionError:
        FAR = "NA"

    try:
        CSI = true_positives / (true_positives + false_positives + false_negatives)
    except ZeroDivisionError:
        CSI = "NA"

    try:
        BIAS = (true_positives + false_positives) / (true_positives + false_negatives)
    except ZeroDivisionError:
        BIAS = "NA"

    # Compute equitable threat score (ETS) / Gilbert Score.
    try:
        a_ref = ((true_positives + false_positives)*(true_positives + false_negatives)) / total_population
        EQUITABLE_THREAT_SCORE = (true_positives - a_ref) / (true_positives - a_ref + false_positives + false_negatives)
    except ZeroDivisionError:
        EQUITABLE_THREAT_SCORE = "NA"

    if total_population == 0:
        TP_perc, FP_perc, TN_perc, FN_perc = "NA", "NA", "NA", "NA"
    else:
        TP_perc = (true_positives / total_population) * 100
        FP_perc = (false_positives / total_population) * 100
        TN_perc = (true_negatives / total_population) * 100
        FN_perc = (false_negatives / total_population) * 100

    predPositive = true_positives + false_positives
    predNegative = true_negatives + false_negatives
    obsPositive = true_positives + false_negatives
    obsNegative = true_negatives + false_positives

    TP = float(true_positives)
    TN = float(true_negatives)
    FN = float(false_negatives)
    FP = float(false_positives)
    try:
        MCC = (TP*TN - FP*FN)/ np.sqrt((TP+FP)*(TP+FN)*(TN+FP)*(TN+FN))
    except ZeroDivisionError:
        MCC = "NA"

    if masked_count != None:
        total_pop_and_mask_pop = total_population + masked_count
        if total_pop_and_mask_pop == 0:
            masked_perc = "NA"
        else:
            masked_perc = (masked_count / total_pop_and_mask_pop) * 100
    else:
        masked_perc = None

    # This checks if a cell_area has been provided, thus making areal calculations possible.
    sq_km_converter = 1000000

    if cell_area != None:
        TP_area = (true_positives * cell_area) / sq_km_converter
        FP_area = (false_positives * cell_area) / sq_km_converter
        TN_area = (true_negatives * cell_area) / sq_km_converter
        FN_area = (false_negatives * cell_area) / sq_km_converter
        area = (total_population * cell_area) / sq_km_converter

        predPositive_area = (predPositive * cell_area) / sq_km_converter
        predNegative_area = (predNegative * cell_area) / sq_km_converter
        obsPositive_area =  (obsPositive * cell_area) / sq_km_converter
        obsNegative_area =  (obsNegative * cell_area) / sq_km_converter
        positiveDiff_area = predPositive_area - obsPositive_area

        if masked_count != None:
            masked_area = (masked_count * cell_area) / sq_km_converter
        else:
            masked_area = None

    # If no cell_area is provided, then the contingeny tables are likely not derived from areal analysis.
    else:
        TP_area = None
        FP_area = None
        TN_area = None
        FN_area = None
        area = None

        predPositive_area = None
        predNegative_area = None
        obsPositive_area =  None
        obsNegative_area =  None
        positiveDiff_area = None
        MCC = None

    if total_population == 0:
        predPositive_perc, predNegative_perc, obsPositive_perc, obsNegative_perc , positiveDiff_perc = "NA", "NA", "NA", "NA", "NA"
    else:
        predPositive_perc = (predPositive / total_population) * 100
        predNegative_perc = (predNegative / total_population) * 100
        obsPositive_perc = (obsPositive / total_population) * 100
        obsNegative_perc = (obsNegative / total_population) * 100

        positiveDiff_perc = predPositive_perc - obsPositive_perc

    if total_population == 0:
        prevalence = "NA"
    else:
        prevalence = (true_positives + false_negatives) / total_population

    try:
        PPV = true_positives / predPositive
    except ZeroDivisionError:
        PPV = "NA"

    try:
        NPV = true_negatives / predNegative
    except ZeroDivisionError:
        NPV = "NA"

    try:
        TNR = true_negatives / obsNegative
    except ZeroDivisionError:
        TNR = "NA"

    try:
        TPR = true_positives / obsPositive

    except ZeroDivisionError:
        TPR = "NA"

    try:
        Bal_ACC = np.mean([TPR,TNR])
    except TypeError:
        Bal_ACC = "NA"

    if total_population == 0:
        ACC = "NA"
    else:
        ACC = (true_positives + true_negatives) / total_population

    try:
        F1_score = (2*true_positives) / (2*true_positives + false_positives + false_negatives)
    except ZeroDivisionError:
        F1_score = "NA"

    stats_dictionary = {'true_negatives_count': int(true_negatives),
                        'false_negatives_count': int(false_negatives),
                        'true_positives_count': int(true_positives),
                        'false_positives_count': int(false_positives),
                        'contingency_tot_count': int(total_population),
                        'cell_area_m2': cell_area,

                        'TP_area_km2': TP_area,
                        'FP_area_km2': FP_area,
                        'TN_area_km2': TN_area,
                        'FN_area_km2': FN_area,

                        'contingency_tot_area_km2': area,
                        'predPositive_area_km2': predPositive_area,
                        'predNegative_area_km2': predNegative_area,
                        'obsPositive_area_km2': obsPositive_area,
                        'obsNegative_area_km2': obsNegative_area,
                        'positiveDiff_area_km2': positiveDiff_area,

                        'CSI': CSI,
                        'FAR': FAR,
                        'TPR': TPR,
                        'TNR': TNR,

                        'PPV': PPV,
                        'NPV': NPV,
                        'ACC': ACC,
                        'Bal_ACC': Bal_ACC,
                        'MCC': MCC,
                        'EQUITABLE_THREAT_SCORE': EQUITABLE_THREAT_SCORE,
                        'PREVALENCE': prevalence,
                        'BIAS': BIAS,
                        'F1_SCORE': F1_score,

                        'TP_perc': TP_perc,
                        'FP_perc': FP_perc,
                        'TN_perc': TN_perc,
                        'FN_perc': FN_perc,
                        'predPositive_perc': predPositive_perc,
                        'predNegative_perc': predNegative_perc,
                        'obsPositive_perc': obsPositive_perc,
                        'obsNegative_perc': obsNegative_perc,
                        'positiveDiff_perc': positiveDiff_perc,

                        'masked_count': int(masked_count),
                        'masked_perc': masked_perc,
                        'masked_area_km2': masked_area,

                        }

    return stats_dictionary


def get_contingency_table_from_binary_rasters(benchmark_raster_path, predicted_raster_path, agreement_raster=None, mask_values=None, mask_dict={}):
    """
    Produces contingency table from 2 rasters and returns it. Also exports an agreement raster classified as:
        0: True Negatives
        1: False Negative
        2: False Positive
        3: True Positive

    Args:
        benchmark_raster_path (str): Path to the binary benchmark raster. 0 = phenomena not present, 1 = phenomena present, NoData = NoData.
        predicted_raster_path (str): Path to the predicted raster. 0 = phenomena not present, 1 = phenomena present, NoData = NoData.

    Returns:
        contingency_table_dictionary (dict): A Python dictionary of a contingency table. Key/value pair formatted as:
                                            {true_negatives: int, false_negatives: int, false_positives: int, true_positives: int}

    """
    from rasterio.warp import reproject, Resampling
    import rasterio
    import numpy as np
    import os
    import rasterio.mask
    import geopandas as gpd
    from shapely.geometry import box

#    print("-----> Evaluating performance across the total area...")
    # Load rasters.
    benchmark_src = rasterio.open(benchmark_raster_path)
    predicted_src = rasterio.open(predicted_raster_path)
    predicted_array = predicted_src.read(1)

    benchmark_array_original = benchmark_src.read(1)

    if benchmark_array_original.shape != predicted_array.shape:
        benchmark_array = np.empty(predicted_array.shape, dtype=np.int8)

        reproject(benchmark_array_original,
              destination = benchmark_array,
              src_transform = benchmark_src.transform,
              src_crs = benchmark_src.crs,
              src_nodata = benchmark_src.nodata,
              dst_transform = predicted_src.transform,
              dst_crs = predicted_src.crs,
              dst_nodata = benchmark_src.nodata,
              dst_resolution = predicted_src.res,
              resampling = Resampling.nearest)

    predicted_array_raw = predicted_src.read(1)

    # Align the benchmark domain to the modeled domain.
    benchmark_array = np.where(predicted_array==predicted_src.nodata, 10, benchmark_array)

    # Ensure zeros and ones for binary comparison. Assume that positive values mean flooding and 0 or negative values mean dry.
    predicted_array = np.where(predicted_array==predicted_src.nodata, 10, predicted_array)  # Reclassify NoData to 10
    predicted_array = np.where(predicted_array<0, 0, predicted_array)
    predicted_array = np.where(predicted_array>0, 1, predicted_array)

    benchmark_array = np.where(benchmark_array==benchmark_src.nodata, 10, benchmark_array)  # Reclassify NoData to 10

    agreement_array = np.add(benchmark_array, 2*predicted_array)
    agreement_array = np.where(agreement_array>4, 10, agreement_array)

    del benchmark_src, benchmark_array, predicted_array, predicted_array_raw

    # Loop through exclusion masks and mask the agreement_array.
    if mask_dict != {}:
        for poly_layer in mask_dict:

            operation = mask_dict[poly_layer]['operation']

            if operation == 'exclude':

                poly_path = mask_dict[poly_layer]['path']
                buffer_val = mask_dict[poly_layer]['buffer']

                reference = predicted_src

                bounding_box = gpd.GeoDataFrame({'geometry': box(*reference.bounds)}, index=[0], crs=reference.crs)
                #Read layer using the bbox option. CRS mismatches are handled if bbox is passed a geodataframe (which it is).
                poly_all = gpd.read_file(poly_path, bbox = bounding_box)

                # Make sure features are present in bounding box area before projecting. Continue to next layer if features are absent.
                if poly_all.empty:
                    continue

#                print("-----> Masking at " + poly_layer + "...")
                #Project layer to reference crs.
                poly_all_proj = poly_all.to_crs(reference.crs)
                # check if there are any lakes within our reference raster extent.
                if poly_all_proj.empty:
                    #If no features within reference raster extent, create a zero array of same shape as reference raster.
                    poly_mask = np.zeros(reference.shape)
                else:
                    #Check if a buffer value is passed to function.
                    if buffer_val is None:
                        #If features are present and no buffer is passed, assign geometry to variable.
                        geometry = poly_all_proj.geometry
                    else:
                        #If  features are present and a buffer is passed, assign buffered geometry to variable.
                        geometry = poly_all_proj.buffer(buffer_val)

                    #Perform mask operation on the reference raster and using the previously declared geometry geoseries. Invert set to true as we want areas outside of poly areas to be False and areas inside poly areas to be True.
                    in_poly,transform,c = rasterio.mask.raster_geometry_mask(reference, geometry, invert = True)
                    #Write mask array, areas inside polys are set to 1 and areas outside poly are set to 0.
                    poly_mask = np.where(in_poly == True, 1,0)

                    # Perform mask.
                    masked_agreement_array = np.where(poly_mask == 1, 4, agreement_array)

                    # Get rid of masked values outside of the modeled domain.
                    agreement_array = np.where(agreement_array == 10, 10, masked_agreement_array)

    contingency_table_dictionary = {}  # Initialize empty dictionary.

    # Only write the agreement raster if user-specified.
    if agreement_raster != None:
        with rasterio.Env():
            profile = predicted_src.profile
            profile.update(nodata=10)
            with rasterio.open(agreement_raster, 'w', **profile) as dst:
                dst.write(agreement_array, 1)

        # Write legend text file
        legend_txt = os.path.join(os.path.split(agreement_raster)[0], 'read_me.txt')

        from datetime import datetime

        now = datetime.now()
        current_time = now.strftime("%m/%d/%Y %H:%M:%S")

        with open(legend_txt, 'w') as f:
            f.write("%s\n" % '0: True Negative')
            f.write("%s\n" % '1: False Negative')
            f.write("%s\n" % '2: False Positive')
            f.write("%s\n" % '3: True Positive')
            f.write("%s\n" % '4: Masked area (excluded from contingency table analysis). Mask layers: {mask_dict}'.format(mask_dict=mask_dict))
            f.write("%s\n" % 'Results produced at: {current_time}'.format(current_time=current_time))

    # Store summed pixel counts in dictionary.
    contingency_table_dictionary.update({'total_area':{'true_negatives': int((agreement_array == 0).sum()),
                                                      'false_negatives': int((agreement_array == 1).sum()),
                                                      'false_positives': int((agreement_array == 2).sum()),
                                                      'true_positives': int((agreement_array == 3).sum()),
                                                      'masked_count': int((agreement_array == 4).sum()),
                                                      'file_handle': 'total_area'

                                                      }})

    # After agreement_array is masked with default mask layers, check for inclusion masks in mask_dict.
    if mask_dict != {}:
        for poly_layer in mask_dict:

            operation = mask_dict[poly_layer]['operation']

            if operation == 'include':

                poly_path = mask_dict[poly_layer]['path']
                buffer_val = mask_dict[poly_layer]['buffer']

                reference = predicted_src

                bounding_box = gpd.GeoDataFrame({'geometry': box(*reference.bounds)}, index=[0], crs=reference.crs)
                #Read layer using the bbox option. CRS mismatches are handled if bbox is passed a geodataframe (which it is).
                poly_all = gpd.read_file(poly_path, bbox = bounding_box)

                # Make sure features are present in bounding box area before projecting. Continue to next layer if features are absent.
                if poly_all.empty:
                    continue

#                print("-----> Evaluating performance at " + poly_layer + "...")
                #Project layer to reference crs.
                poly_all_proj = poly_all.to_crs(reference.crs)
                # check if there are any lakes within our reference raster extent.
                if poly_all_proj.empty:
                    #If no features within reference raster extent, create a zero array of same shape as reference raster.
                    poly_mask = np.zeros(reference.shape)
                else:
                    #Check if a buffer value is passed to function.
                    if buffer_val is None:
                        #If features are present and no buffer is passed, assign geometry to variable.
                        geometry = poly_all_proj.geometry
                    else:
                        #If  features are present and a buffer is passed, assign buffered geometry to variable.
                        geometry = poly_all_proj.buffer(buffer_val)

                    #Perform mask operation on the reference raster and using the previously declared geometry geoseries. Invert set to true as we want areas outside of poly areas to be False and areas inside poly areas to be True.
                    in_poly,transform,c = rasterio.mask.raster_geometry_mask(reference, geometry, invert = True)
                    #Write mask array, areas inside polys are set to 1 and areas outside poly are set to 0.
                    poly_mask = np.where(in_poly == True, 1, 0)

                    # Perform mask.
                    masked_agreement_array = np.where(poly_mask == 0, 4, agreement_array)  # Changed to poly_mask == 0

                    # Get rid of masked values outside of the modeled domain.
                    temp_agreement_array = np.where(agreement_array == 10, 10, masked_agreement_array)

                    if buffer_val == None:  # The buffer used is added to filename, and 0 is easier to read than None.
                        buffer_val = 0

                    poly_handle = poly_layer + '_b' + str(buffer_val) + 'm'

                    # Write the layer_agreement_raster.
                    layer_agreement_raster = os.path.join(os.path.split(agreement_raster)[0], poly_handle + '_agreement.tif')
                    with rasterio.Env():
                        profile = predicted_src.profile
                        profile.update(nodata=10)
                        with rasterio.open(layer_agreement_raster, 'w', **profile) as dst:
                            dst.write(temp_agreement_array, 1)


                    # Store summed pixel counts in dictionary.
                    contingency_table_dictionary.update({poly_handle:{'true_negatives': int((temp_agreement_array == 0).sum()),
                                                                     'false_negatives': int((temp_agreement_array == 1).sum()),
                                                                     'false_positives': int((temp_agreement_array == 2).sum()),
                                                                     'true_positives': int((temp_agreement_array == 3).sum()),
                                                                     'masked_count': int((temp_agreement_array == 4).sum()),
                                                                     'file_handle': poly_handle
                                                                      }})

    return contingency_table_dictionary
########################################################################
########################################################################
#Functions related to categorical fim and ahps evaluation
########################################################################
def get_metadata(metadata_url, select_by, selector, must_include = None, upstream_trace_distance = None, downstream_trace_distance = None ):
    '''
    Retrieve metadata for a site or list of sites.

    Parameters
    ----------
    metadata_url : STR
        metadata base URL.
    select_by : STR
        Location search option.
    selector : LIST
        Value to match location data against. Supplied as a LIST.
    must_include : STR, optional
        What attributes are required to be valid response. The default is None.
    upstream_trace_distance : INT, optional
        Distance in miles upstream of site to trace NWM network. The default is None.
    downstream_trace_distance : INT, optional
        Distance in miles downstream of site to trace NWM network. The default is None.

    Returns
    -------
    metadata_list : LIST
        Dictionary or list of dictionaries containing metadata at each site.
    metadata_dataframe : Pandas DataFrame
        Dataframe of metadata for each site.

    '''

    #Format selector variable in case multiple selectors supplied
    format_selector = '%2C'.join(selector)
    #Define the url
    url = f'{metadata_url}/{select_by}/{format_selector}/'
    #Assign optional parameters to a dictionary
    params = {}
    params['must_include'] = must_include
    params['upstream_trace_distance'] = upstream_trace_distance
    params['downstream_trace_distance'] = downstream_trace_distance
    #Request data from url
    response = requests.get(url, params = params)
    if response.ok:
        #Convert data response to a json
        metadata_json = response.json()
        #Get the count of returned records
        location_count = metadata_json['_metrics']['location_count']
        #Get metadata
        metadata_list = metadata_json['locations']
        #Add timestamp of WRDS retrieval
        timestamp = response.headers['Date']
        #Add timestamp of sources retrieval
        timestamp_list = metadata_json['data_sources']['metadata_sources']
        
        # Default timestamps to "Not available" and overwrite with real values if possible.
        nwis_timestamp, nrldb_timestamp = "Not available", "Not available"
        for timestamp in timestamp_list:
            if "NWIS" in timestamp:
                nwis_timestamp = timestamp
            if "NRLDB" in timestamp:
                nrldb_timestamp = timestamp
            
#        nrldb_timestamp, nwis_timestamp = metadata_json['data_sources']['metadata_sources']        
        #get crosswalk info (always last dictionary in list)
        crosswalk_info = metadata_json['data_sources']
        #Update each dictionary with timestamp and crosswalk info also save to DataFrame.
        for metadata in metadata_list:
            metadata.update({"wrds_timestamp": timestamp})
            metadata.update({"nrldb_timestamp":nrldb_timestamp})
            metadata.update({"nwis_timestamp":nwis_timestamp})
            metadata.update(crosswalk_info)
        metadata_dataframe = pd.json_normalize(metadata_list)
        #Replace all periods with underscores in column names
        metadata_dataframe.columns = metadata_dataframe.columns.str.replace('.','_')
    else:
        #if request was not succesful, print error message.
        print(f'Code: {response.status_code}\nMessage: {response.reason}\nURL: {response.url}')
        #Return empty outputs
        metadata_list = []
        metadata_dataframe = pd.DataFrame()
    return metadata_list, metadata_dataframe

########################################################################
#Function to assign HUC code using the WBD spatial layer using a spatial join
########################################################################
def aggregate_wbd_hucs(metadata_list, wbd_huc8_path, retain_attributes = False):
    '''
    Assigns the proper FIM HUC 08 code to each site in the input DataFrame.
    Converts input DataFrame to a GeoDataFrame using lat/lon attributes
    with sites containing null nws_lid/lat/lon removed. Reprojects GeoDataFrame
    to same CRS as the HUC 08 layer. Performs a spatial join to assign the
    HUC 08 layer to the GeoDataFrame. Sites that are not assigned a HUC
    code removed as well as sites in Alaska and Canada.

    Parameters
    ----------
    metadata_list: List of Dictionaries
        Output list from get_metadata
    wbd_huc8_path : pathlib Path
        Path to HUC8 wbd layer (assumed to be geopackage format)
    retain_attributes ; Bool OR List
        Flag to define attributes of output GeoDataBase. If True, retain
        all attributes. If False, the site metadata will be trimmed to a
        default list. If a list of desired attributes is supplied these
        will serve as the retained attributes.
    Returns
    -------
    dictionary : DICT
        Dictionary with HUC (key) and corresponding AHPS codes (values).
    all_gdf: GeoDataFrame
        GeoDataFrame of all NWS_LID sites.

    '''
    #Import huc8 layer as geodataframe and retain necessary columns
    huc8 = gpd.read_file(wbd_huc8_path, layer = 'WBDHU8')
    huc8 = huc8[['HUC8','name','states', 'geometry']]
    #Define EPSG codes for possible latlon datum names (default of NAD83 if unassigned)
    crs_lookup ={'NAD27':'EPSG:4267', 'NAD83':'EPSG:4269', 'WGS84': 'EPSG:4326'}
    #Create empty geodataframe and define CRS for potential horizontal datums
    metadata_gdf = gpd.GeoDataFrame()
    #Iterate through each site
    for metadata in metadata_list:
        #Convert metadata to json
        df = pd.json_normalize(metadata)
        #Columns have periods due to nested dictionaries
        df.columns = df.columns.str.replace('.', '_')
        #Drop any metadata sites that don't have lat/lon populated
        df.dropna(subset = ['identifiers_nws_lid','usgs_preferred_latitude', 'usgs_preferred_longitude'], inplace = True)
        #If dataframe still has data
        if not df.empty:
            #Get horizontal datum
            h_datum = df['usgs_preferred_latlon_datum_name'].item()
            #Look up EPSG code, if not returned Assume NAD83 as default. 
            dict_crs = crs_lookup.get(h_datum,'EPSG:4269_ Assumed')
            #We want to know what sites were assumed, hence the split.
            src_crs, *message = dict_crs.split('_')
            #Convert dataframe to geodataframe using lat/lon (USGS). Add attribute of assigned crs (label ones that are assumed)
            site_gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['usgs_preferred_longitude'], df['usgs_preferred_latitude']), crs =  src_crs)
            #Field to indicate if a latlon datum was assumed
            site_gdf['assigned_crs'] = src_crs + ''.join(message)
            
            #Reproject to huc 8 crs
            site_gdf = site_gdf.to_crs(huc8.crs)
            #Append site geodataframe to metadata geodataframe
            metadata_gdf = metadata_gdf.append(site_gdf, ignore_index = True)

    #Trim metadata to only have certain fields.
    if not retain_attributes:
        metadata_gdf = metadata_gdf[['identifiers_nwm_feature_id', 'identifiers_nws_lid', 'identifiers_usgs_site_code', 'geometry']]
    #If a list of attributes is supplied then use that list.
    elif isinstance(retain_attributes,list):
        metadata_gdf = metadata_gdf[retain_attributes]

    #Perform a spatial join to get the WBD HUC 8 assigned to each AHPS
    joined_gdf = gpd.sjoin(metadata_gdf, huc8, how = 'inner', op = 'intersects', lsuffix = 'ahps', rsuffix = 'wbd')
    joined_gdf = joined_gdf.drop(columns = 'index_wbd')

    #Remove all Alaska HUCS (Not in NWM v2.0 domain)
    joined_gdf = joined_gdf[~joined_gdf.states.str.contains('AK')]

    #Create a dictionary of huc [key] and nws_lid[value]
    dictionary = joined_gdf.groupby('HUC8')['identifiers_nws_lid'].apply(list).to_dict()

    return dictionary, joined_gdf

########################################################################
def mainstem_nwm_segs(metadata_url, list_of_sites):
    '''
    Define the mainstems network. Currently a 4 pass approach that probably needs refined.
    Once a final method is decided the code can be shortened. Passes are:
        1) Search downstream of gages designated as upstream. This is done to hopefully reduce the issue of mapping starting at the nws_lid. 91038 segments
        2) Search downstream of all LID that are rfc_forecast_point = True. Additional 48,402 segments
        3) Search downstream of all evaluated sites (sites with detailed FIM maps) Additional 222 segments
        4) Search downstream of all sites in HI/PR (locations have no rfc_forecast_point = True) Additional 408 segments

    Parameters
    ----------
    metadata_url : STR
        URL of API.
    list_of_sites : LIST
        List of evaluated sites.

    Returns
    -------
    ms_nwm_segs_set : SET
        Mainstems network segments as a set.

    '''

    #Define the downstream trace distance
    downstream_trace_distance = 'all'

    #Trace downstream from all 'headwater' usgs gages
    select_by = 'tag'
    selector = ['usgs_gages_ii_ref_headwater']
    must_include = None
    gages_list, gages_dataframe = get_metadata(metadata_url = metadata_url, select_by = select_by, selector = selector, must_include = must_include, upstream_trace_distance = None, downstream_trace_distance = downstream_trace_distance )

    #Trace downstream from all rfc_forecast_point.
    select_by = 'nws_lid'
    selector = ['all']
    must_include = 'nws_data.rfc_forecast_point'
    fcst_list, fcst_dataframe = get_metadata(metadata_url = metadata_url, select_by = select_by, selector = selector, must_include = must_include, upstream_trace_distance = None, downstream_trace_distance = downstream_trace_distance )

    #Trace downstream from all evaluated ahps sites.
    select_by = 'nws_lid'
    selector = list_of_sites
    must_include = None
    eval_list, eval_dataframe = get_metadata(metadata_url = metadata_url, select_by = select_by, selector = selector, must_include = must_include, upstream_trace_distance = None, downstream_trace_distance = downstream_trace_distance )

    #Trace downstream from all sites in HI/PR.
    select_by = 'state'
    selector = ['HI','PR']
    must_include = None
    islands_list, islands_dataframe = get_metadata(metadata_url = metadata_url, select_by = select_by, selector = selector, must_include = must_include, upstream_trace_distance = None, downstream_trace_distance = downstream_trace_distance )

    #Combine all lists of metadata dictionaries into a single list.
    combined_lists = gages_list + fcst_list + eval_list + islands_list
    #Define list that will contain all segments listed in metadata.
    all_nwm_segments = []
    #For each lid metadata dictionary in list
    for lid in combined_lists:
        #get all downstream segments
        downstream_nwm_segs = lid.get('downstream_nwm_features')
        #Append downstream segments
        if downstream_nwm_segs:
            all_nwm_segments.extend(downstream_nwm_segs)
        #Get the nwm feature id associated with the location
        location_nwm_seg = lid.get('identifiers').get('nwm_feature_id')
        if location_nwm_seg:
            #Append nwm segment (conver to list)
            all_nwm_segments.extend([location_nwm_seg])
    #Remove duplicates by assigning to a set.
    ms_nwm_segs_set = set(all_nwm_segments)

    return ms_nwm_segs_set

##############################################################################
#Function to create list of NWM segments
###############################################################################
def get_nwm_segs(metadata):
    '''
    Using the metadata output from "get_metadata", output the NWM segments.

    Parameters
    ----------
    metadata : DICT
        Dictionary output from "get_metadata" function.

    Returns
    -------
    all_segments : LIST
        List of all NWM segments.

    '''

    nwm_feature_id = metadata.get('identifiers').get('nwm_feature_id')
    upstream_nwm_features = metadata.get('upstream_nwm_features')
    downstream_nwm_features = metadata.get('downstream_nwm_features')

    all_segments = []
    #Convert NWM feature id segment to a list (this is always a string or empty)
    if nwm_feature_id:
        nwm_feature_id = [nwm_feature_id]
        all_segments.extend(nwm_feature_id)
    #Add all upstream segments (always a list or empty)
    if upstream_nwm_features:
        all_segments.extend(upstream_nwm_features)
    #Add all downstream segments (always a list or empty)
    if downstream_nwm_features:
        all_segments.extend(downstream_nwm_features)

    return all_segments

#######################################################################
#Thresholds
#######################################################################
def get_thresholds(threshold_url, select_by, selector, threshold = 'all'):
    '''
    Get nws_lid threshold stages and flows (i.e. bankfull, action, minor,
    moderate, major). Returns a dictionary for stages and one for flows.

    Parameters
    ----------
    threshold_url : STR
        WRDS threshold API.
    select_by : STR
        Type of site (nws_lid, usgs_site_code etc).
    selector : STR
        Site for selection. Must be a single site.
    threshold : STR, optional
        Threshold option. The default is 'all'.

    Returns
    -------
    stages : DICT
        Dictionary of stages at each threshold.
    flows : DICT
        Dictionary of flows at each threshold.

    '''
    params = {}
    params['threshold'] = threshold
    url = f'{threshold_url}/{select_by}/{selector}'
    response = requests.get(url, params = params)
    if response.ok:
        thresholds_json = response.json()
        #Get metadata
        thresholds_info = thresholds_json['value_set']
        #Initialize stages/flows dictionaries
        stages = {}
        flows = {}
        #Check if thresholds information is populated. If site is non-existent thresholds info is blank
        if thresholds_info:
            #Get all rating sources and corresponding indexes in a dictionary
            rating_sources = {i.get('calc_flow_values').get('rating_curve').get('source'): index for index, i in enumerate(thresholds_info)}
            #Get threshold data use USGS Rating Depot (priority) otherwise NRLDB.
            if 'USGS Rating Depot' in rating_sources:
                threshold_data = thresholds_info[rating_sources['USGS Rating Depot']]
            elif 'NRLDB' in rating_sources:
                threshold_data = thresholds_info[rating_sources['NRLDB']]
            #If neither USGS or NRLDB is available use first dictionary to get stage values.
            else:
                threshold_data = thresholds_info[0]
            #Get stages and flows for each threshold
            if threshold_data:
                stages = threshold_data['stage_values']
                flows = threshold_data['calc_flow_values']
                #Add source information to stages and flows. Flows source inside a nested dictionary. Remove key once source assigned to flows.
                stages['source'] = threshold_data.get('metadata').get('threshold_source')
                flows['source'] = flows.get('rating_curve', {}).get('source')
                flows.pop('rating_curve', None)
                #Add timestamp WRDS data was retrieved.
                stages['wrds_timestamp'] = response.headers['Date']
                flows['wrds_timestamp'] = response.headers['Date']
                #Add Site information
                stages['nws_lid'] = threshold_data.get('metadata').get('nws_lid')
                flows['nws_lid'] = threshold_data.get('metadata').get('nws_lid')
                stages['usgs_site_code'] = threshold_data.get('metadata').get('usgs_site_code')
                flows['usgs_site_code'] = threshold_data.get('metadata').get('usgs_site_code')
                stages['units'] = threshold_data.get('metadata').get('stage_units')
                flows['units'] = threshold_data.get('metadata').get('calc_flow_units')
    return stages, flows

########################################################################
# Function to write flow file
########################################################################
def flow_data(segments, flows, convert_to_cms = True):
    '''
    Given a list of NWM segments and a flow value in cfs, convert flow to
    cms and return a DataFrame that is set up for export to a flow file.

    Parameters
    ----------
    segments : LIST
        List of NWM segments.
    flows : FLOAT
        Flow in CFS.
    convert_to_cms : BOOL
        Flag to indicate if supplied flows should be converted to metric.
        Default value is True (assume input flows are CFS).

    Returns
    -------
    flow_data : DataFrame
        Dataframe ready for export to a flow file.

    '''
    if convert_to_cms:
        #Convert cfs to cms
        cfs_to_cms = 0.3048**3
        flows_cms = round(flows * cfs_to_cms,2)
    else:
        flows_cms = round(flows,2)

    flow_data = pd.DataFrame({'feature_id':segments, 'discharge':flows_cms})
    flow_data = flow_data.astype({'feature_id' : int , 'discharge' : float})
    return flow_data
#######################################################################
#Function to get datum information
#######################################################################
def get_datum(metadata):
    '''
    Given a record from the metadata endpoint, retrieve important information
    related to the datum and site from both NWS and USGS sources. This information
    is saved to a dictionary with common keys. USGS has more data available so
    it has more keys. 

    Parameters
    ----------
    metadata : DICT
        Single record from the get_metadata function. Must iterate through
        the get_metadata output list. 

    Returns
    -------
    nws_datums : DICT
        Dictionary of NWS data.
    usgs_datums : DICT
        Dictionary of USGS Data.

    '''
    #Get site and datum information from nws sub-dictionary. Use consistent naming between USGS and NWS sources.
    nws_datums = {}
    nws_datums['nws_lid'] = metadata['identifiers']['nws_lid']
    nws_datums['usgs_site_code'] = metadata['identifiers']['usgs_site_code']
    nws_datums['state'] = metadata['nws_data']['state']
    nws_datums['datum'] = metadata['nws_data']['zero_datum']
    nws_datums['vcs'] = metadata['nws_data']['vertical_datum_name']
    nws_datums['lat'] = metadata['nws_data']['latitude']
    nws_datums['lon'] = metadata['nws_data']['longitude']
    nws_datums['crs'] = metadata['nws_data']['horizontal_datum_name']
    nws_datums['source'] = 'nws_data'
    
    #Get site and datum information from usgs_data sub-dictionary. Use consistent naming between USGS and NWS sources.
    usgs_datums = {}
    usgs_datums['nws_lid'] = metadata['identifiers']['nws_lid']
    usgs_datums['usgs_site_code'] = metadata['identifiers']['usgs_site_code']
    usgs_datums['active'] = metadata['usgs_data']['active']
    usgs_datums['state'] = metadata['usgs_data']['state']
    usgs_datums['datum'] = metadata['usgs_data']['altitude']
    usgs_datums['vcs'] = metadata['usgs_data']['alt_datum_code']
    usgs_datums['datum_acy'] = metadata['usgs_data']['alt_accuracy_code']
    usgs_datums['datum_meth'] = metadata['usgs_data']['alt_method_code']
    usgs_datums['lat'] = metadata['usgs_data']['latitude']
    usgs_datums['lon'] = metadata['usgs_data']['longitude']
    usgs_datums['crs'] = metadata['usgs_data']['latlon_datum_name']
    usgs_datums['source'] = 'usgs_data' 
    
    return nws_datums, usgs_datums
########################################################################
#Function to convert horizontal datums
########################################################################
def convert_latlon_datum(lat,lon,src_crs,dest_crs):
    '''
    Converts latitude and longitude datum from a source CRS to a dest CRS 
    using geopandas and returns the projected latitude and longitude coordinates.

    Parameters
    ----------
    lat : FLOAT
        Input Latitude.
    lon : FLOAT
        Input Longitude.
    src_crs : STR
        CRS associated with input lat/lon. Geopandas must recognize code.
    dest_crs : STR
        Target CRS that lat/lon will be projected to. Geopandas must recognize code.

    Returns
    -------
    new_lat : FLOAT
        Reprojected latitude coordinate in dest_crs.
    new_lon : FLOAT
        Reprojected longitude coordinate in dest_crs.

    '''
    
    #Create a temporary DataFrame containing the input lat/lon.
    temp_df = pd.DataFrame({'lat':[lat],'lon':[lon]})
    #Convert dataframe to a GeoDataFrame using the lat/lon coords. Input CRS is assigned.
    temp_gdf = gpd.GeoDataFrame(temp_df, geometry=gpd.points_from_xy(temp_df.lon, temp_df.lat), crs =  src_crs)
    #Reproject GeoDataFrame to destination CRS.
    reproject = temp_gdf.to_crs(dest_crs)
    #Get new Lat/Lon coordinates from the geometry data.
    new_lat,new_lon = [reproject.geometry.y.item(), reproject.geometry.x.item()]
    return new_lat, new_lon
#######################################################################
#Function to get conversion adjustment NGVD to NAVD in FEET
#######################################################################
def ngvd_to_navd_ft(datum_info, region = 'contiguous'):
    '''
    Given the lat/lon, retrieve the adjustment from NGVD29 to NAVD88 in feet. 
    Uses NOAA tidal API to get conversion factor. Requires that lat/lon is
    in NAD27 crs. If input lat/lon are not NAD27 then these coords are 
    reprojected to NAD27 and the reproject coords are used to get adjustment.
    There appears to be an issue when region is not in contiguous US.

    Parameters
    ----------
    lat : FLOAT
        Latitude.
    lon : FLOAT
        Longitude.

    Returns
    -------
    datum_adj_ft : FLOAT
        Vertical adjustment in feet, from NGVD29 to NAVD88, and rounded to nearest hundredth.

    '''
    #If crs is not NAD 27, convert crs to NAD27 and get adjusted lat lon
    if datum_info['crs'] != 'NAD27':
        lat, lon = convert_latlon_datum(datum_info['lat'],datum_info['lon'],datum_info['crs'],'NAD27')
    else:
        #Otherwise assume lat/lon is in NAD27.
        lat = datum_info['lat']
        lon = datum_info['lon']
    
    #Define url for datum API
    datum_url = 'https://vdatum.noaa.gov/vdatumweb/api/tidal'     
    
    #Define parameters. Hard code most parameters to convert NGVD to NAVD.    
    params = {}
    params['lat'] = lat
    params['lon'] = lon
    params['region'] = region
    params['s_h_frame'] = 'NAD27'     #Source CRS
    params['s_v_frame'] = 'NGVD29'    #Source vertical coord datum
    params['s_vertical_unit'] = 'm'   #Source vertical units
    params['src_height'] = 0.0        #Source vertical height
    params['t_v_frame'] = 'NAVD88'    #Target vertical datum
    params['tar_vertical_unit'] = 'm' #Target vertical height
    
    #Call the API
    response = requests.get(datum_url, params = params)
    #If succesful get the navd adjustment
    if response:
        results = response.json()
        #Get adjustment in meters (NGVD29 to NAVD88)
        adjustment = results['tar_height']
        #convert meters to feet
        adjustment_ft = round(float(adjustment) * 3.28084,2)                
    else:
        adjustment_ft = None
    return adjustment_ft       
#######################################################################
#Function to download rating curve from API
#######################################################################
def get_rating_curve(rating_curve_url, location_ids):
    '''
    Given list of location_ids (nws_lids, usgs_site_codes, etc) get the 
    rating curve from WRDS API and export as a DataFrame.

    Parameters
    ----------
    rating_curve_url : STR
        URL to retrieve rating curve
    location_ids : LIST
        List of location ids. Can be nws_lid or usgs_site_codes.

    Returns
    -------
    all_curves : pandas DataFrame
        Rating curves from input list as well as other site information.

    '''
    #Define DataFrame to contain all returned curves.
    all_curves = pd.DataFrame()
    
    #Define call to retrieve all rating curve information from WRDS.
    joined_location_ids = '%2C'.join(location_ids)
    url = f'{rating_curve_url}/{joined_location_ids}'
    
    #Call the API 
    response = requests.get(url)

    #If successful
    if response.ok:

        #Write return to json and extract the rating curves
        site_json = response.json()
        rating_curves_list = site_json['rating_curves']

        #For each curve returned
        for curve in rating_curves_list:
            #Check if a curve was populated (e.g wasn't blank)
            if curve:

                #Write rating curve to pandas dataframe as well as site attributes
                curve_df = pd.DataFrame(curve['rating_curve'],dtype=float)

                #Add other information such as site, site type, source, units, and timestamp.
                curve_df['location_id'] = curve['metadata']['location_id']
                curve_df['location_type'] = curve['metadata']['id_type']
                curve_df['source'] = curve['metadata']['source']
                curve_df['flow_units'] = curve['metadata']['flow_unit']
                curve_df['stage_units'] = curve['metadata']['stage_unit']
                curve_df['wrds_timestamp'] = response.headers['Date']

                #Append rating curve to DataFrame containing all curves
                all_curves = all_curves.append(curve_df)
            else:
                continue

    return all_curves
#######################################################################
#Following Functions used for preprocesing of AHPS sites (NWS and USGS)
########################################################################
    
#######################################################################
#Function to return a correct maps.
########################################################################
def select_grids(dataframe, stages, datum88, buffer):
    '''
    Given a DataFrame (in a specific format), and a dictionary of stages, and the datum (in navd88). 
    loop through the available inundation datasets and find the datasets that are equal to or immediately above the thresholds and only return 1 dataset per threshold (if any).

    Parameters
    ----------
    dataframe : DataFrame
        DataFrame that has to be in a specific format and contains the stages and paths to the inundation datasets.
    stages : DICT
        Dictionary of thresholds (key) and stages (values)
    datum88: FLOAT
        The datum associated with the LID that is pre-converted to NAVD88 (if needed)
    buffer: Float
        Interval which the uppder bound can be assigned. For example, Threshold + buffer = upper bound. Recommended to make buffer 0.1 greater than desired interval as code selects maps < and not <=

    Returns
    -------
    maps : DICT
        Dictionary of threshold (key) and inundation dataset path (value)
    map_flows: DICT
        Dictionary of threshold (key) and flows in CFS rounded to the nearest whole number associated with the selected maps (value)

    '''
    #Define threshold categories
    thresholds = ['action', 'minor', 'moderate', 'major']
    maps = {}
    map_flows={}
    #For each threshold, pick the appropriate map for analysis.
    for i,threshold in enumerate(thresholds):
        #Check if stage is None
        if not stages[threshold] is None:
            #Define the threshold floor elevation (navd88).
            lower_bound = round((stages[threshold] + datum88),1)
            #Define the threshold ceiling (navd88)
            upper_bound = round((stages[threshold] + datum88 + buffer),1)
            #For thresholds that are action, minor, moderate
            if threshold in ['action', 'minor', 'moderate']:
                #Make sure the next threshold has a valid stage
                if stages[thresholds[i+1]] is None:
                    next_threshold = upper_bound
                else:
                    #Determine what the next threshold elevation is.
                    next_threshold = round((stages[thresholds[i+1]] + datum88),1)
               #Make sure the upper_bound is not greater than the next threshold, if it is then reassign upper_bound.
                if upper_bound > next_threshold:
                    upper_bound = next_threshold                
                #Get the single map which meets the criteria.
                value = dataframe.query(f'({lower_bound}<=elevation) & (elevation<{upper_bound})')['elevation'].min()
            #For major threshold
            else:
                #Get the single map which meets criteria.
                value = dataframe.query(f'({lower_bound}<=elevation) & (elevation<{upper_bound})')['elevation'].min()
    
            #If the selected value is a number
            if np.isfinite(value):
                #Get the map path and the flow associated with the map (rounded to nearest whole number)
                map_path = dataframe.query(f'elevation == {value}')['path'].item()
                map_flow = round(dataframe.query(f'elevation == {value}')['flow'].item(),0)
                #Check to see if map_flow is valid (if beyond rating_curve it is nan)
                if not np.isfinite(map_flow):
                    map_path = 'No Flow'
                    map_flow = 'No Flow'
                                    
            #If the selected value is not a number (or interpolated flow is nan caused by elevation of map which is beyond rating curve range), then map_path and map_flows are both set to 'No Map'. 
            else: 
                map_path = 'No Map' 
                map_flow = 'No Map' 
        else:
            map_path = 'No Threshold'
            map_flow = 'No Threshold'
        
        #Write map paths and flows to dictionary        
        maps[threshold] = map_path
        map_flows[threshold] = map_flow
    
    #Get the maximum inundation map (using elevation) and this will be the domain extent
    max_value = dataframe['elevation'].max()
    map_path = dataframe.query(f'elevation == {max_value}')['path'].item()
    map_flow = 'Not Used'
    maps['extent'] = map_path
    map_flows['extent'] = map_flow
    
    return maps,map_flows

#######################################################################
#Process AHPS Extent Grid (Fill Holes)
#######################################################################
def process_extent(extent, profile, output_raster = False):
    '''
    Convert raster to feature (using raster_to_feature), the footprint is used so all raster values are set to 1 where there is data.
    fill all donut holes in resulting feature. 
    Filled geometry is then converted back to raster using same raster properties as input profile. 
    Output raster will have be encoded as follows:
        filled footprint (wet) = 1
        remaining area in raster domain (dry) = 0
        NoData = 3

    Parameters
    ----------
    extent : Rasterio Dataset Reader
        Path to extent raster
    extent_profile: Rasterio Profile
        profile related to the extent argument
    output_raster: STR
        Path to output raster. If no path supplied, then no raster is written to disk. default = False

    Returns (If no output raster specified)
    -------
    extent_filled_raster : rasterio dataset
        Extent raster with filled donut holes
    profile : rasterio profile
        Profile associated with extent_filled_raster

    '''
        
    #Convert extent to feature and explode geometry
    poly_extent = raster_to_feature(extent, profile, footprint_only = True)
    poly_extent = poly_extent.explode()
    
    #Fill holes in extent
    poly_extent_fill_holes=MultiPolygon(Polygon(p.exterior) for p in poly_extent['geometry'])
    # loop through the filled polygons and insert the new geometry
    for i in range(len(poly_extent_fill_holes)):
        poly_extent.loc[i,'geometry'] = poly_extent_fill_holes[i]
    
    #Dissolve filled holes with main map and explode
    poly_extent['dissolve_field'] = 1
    poly_extent = poly_extent.dissolve(by = 'dissolve_field')
    poly_extent = poly_extent.explode()
    poly_extent = poly_extent.reset_index()
    
    #Convert filled polygon back to raster
    extent_filled_raster = features.rasterize(((geometry, 1) for geometry in poly_extent['geometry']), fill = 0, dtype = 'int32',transform = profile['transform'], out_shape = (profile['height'], profile['width']))
    
    #Update profile properties (dtype and no data)
    profile.update(dtype = rasterio.int32)
    profile.update(nodata=0)
    
    #Check if output raster is specified. If so, the write extent filled raster to disk. 
    if output_raster:
        #Create directory
        Path(output_raster).parent.mkdir(parents = True, exist_ok = True)
        with rasterio.Env():
            with rasterio.open(output_raster, 'w', **profile) as dst:
                dst.write(extent_filled_raster, 1)
    #If no output raster is supplied the return the rasterio array and profile.
    else:
        return extent_filled_raster, profile
########################################################################
#Convert raster to polygon
########################################################################
def raster_to_feature(grid, profile_override = False, footprint_only = False):
    '''
    Given a grid path, convert to vector, dissolved by grid value, in GeoDataFrame format.

    Parameters
    ----------
    grid_path : pathlib path OR rasterio Dataset Reader
        Path to grid or a rasterio Dataset Reader
    profile_override: rasterio Profile
        Default is False, If a rasterio Profile is supplied, it will dictate the transform and crs.
    footprint_only: BOOL
        If true, dataset will be divided by itself to remove all unique values. If False, all values in grid will be carried through on raster to feature conversion. default = False

    Returns
    -------
    dissolve_geodatabase : GeoDataFrame
        Dissolved (by gridvalue) vector data in GeoDataFrame.

    '''
    #Determine what format input grid is:
    #If a pathlib path, open with rasterio
    if isinstance(grid, pathlib.PurePath):
        dataset = rasterio.open(grid)
    #If a rasterio dataset object, assign to dataset
    elif isinstance(grid, rasterio.DatasetReader):
        dataset = grid

    #Get data/mask and profile properties from dataset
    data = dataset.read(1)
    msk = dataset.read_masks(1)   
    data_transform = dataset.transform
    coord_sys = dataset.crs

    #If a profile override was supplied, use it to get the transform and coordinate system.
    if profile_override:
        data_transform = profile_override['transform']
        coord_sys = profile_override['crs']
    
    #If a footprint of the raster is desired, convert all data values to 1
    if footprint_only:
        data[msk == 255] = 1   
    
    #Convert grid to feature
    spatial = []
    values = []
    for geom, val in rasterio.features.shapes(data, mask = msk, transform = data_transform):
        spatial.append(shape(geom))
        values.append(val)        
    spatial_geodataframe = gpd.GeoDataFrame({'values': values,'geometry':spatial }, crs = coord_sys)
    dissolve_geodataframe = spatial_geodataframe.dissolve(by = 'values')
    return dissolve_geodataframe
########################################################################
#Create AHPS Benchmark Grid
########################################################################
def process_grid(benchmark, benchmark_profile, domain, domain_profile, reference_raster):
    '''
    Given a benchmark grid and profile, a domain rasterio dataset and profile, and a reference raster, 
    Match the benchmark dataset to the domain extent and create a classified grid convert to:
        0 (no data footprint of domain)
        1 (data footprint of domain)
        2 (data footprint of benchmark)
    Then reproject classified benchmark grid to match reference grid resolution and crs. 
    Output is an array of values and a profile.

    Parameters
    ----------
    benchmark : rasterio dataset
        Rasterio dataset of the benchmark dataset for a given threshold
    benchmark_profile : rasterio profile
        A potentially modified profile to the benchmark dataset.
    domain: rasterio dataset
        Rasterio dataset of the domain grid (the maximum available grid for a given site)
    domain_profile: rasterio profile
        A potentially modified profile of the domain dataset.
    reference_raster : pathlib Path
        Path to reference dataset.

    Returns
    -------
    boolean_benchmark : numpy Array
        Array of values for the benchmark_boolean grid.
    profile : rasterio profile
        Updated, final profile of the boolean_benchmark grid. 

    '''
    
    #Make benchmark have same dimensions as domain (Assume domain has same CRS as benchmark)
    #Get source CRS (benchmark and domain assumed to be same CRS)
    source_crs = benchmark_profile['crs'].to_wkt()
    #Get domain data
    domain_arr = domain.read(1)
    #Get benchmark data
    benchmark_arr = benchmark.read(1)
    #Create empty array with same dimensions as domain
    benchmark_fit_to_domain = np.empty(domain_arr.shape)
    #Make benchmark have same footprint as domain (Assume domain has same CRS as benchmark)
    reproject(benchmark_arr, 
              destination = benchmark_fit_to_domain,
              src_transform = benchmark.transform, 
              src_crs = source_crs,
              src_nodata = benchmark.nodata,
              dst_transform = domain.transform, 
              dst_crs = source_crs,
              dst_nodata = benchmark.nodata,
              dst_resolution = source_crs,
              resampling = Resampling.bilinear)    
    #Convert fitted benchmark dataset to boolean. 0 = NODATA Regions and 1 = Data Regions
    benchmark_fit_to_domain_bool = np.where(benchmark_fit_to_domain == benchmark.nodata,0,1)
    #Merge domain datamask and benchmark data mask. New_nodata_value (2) = Domain NO DATA footprint, 0 = NO DATA for benchmark (within data region of domain), 1 = DATA region of benchmark.
    new_nodata_value = 2
    classified_benchmark = np.where(domain_arr == domain.nodata, new_nodata_value, benchmark_fit_to_domain_bool)
    
    ##Reproject classified benchmark to reference raster crs and resolution.
    #Read in reference raster
    reference = rasterio.open(reference_raster)
    #Determine the new transform and dimensions of reprojected/resampled classified benchmark dataset whos width, height, and bounds are same as domain dataset.
    new_benchmark_transform, new_benchmark_width, new_benchmark_height = calculate_default_transform(source_crs, reference.crs, domain.width, domain.height, *domain.bounds, resolution = reference.res)   
    #Define an empty array that is same dimensions as output by the "calculate_default_transform" command. 
    classified_benchmark_projected = np.empty((new_benchmark_height,new_benchmark_width), dtype=np.uint8) 
    #Reproject and resample the classified benchmark dataset. Nearest Neighbor resampling due to integer values of classified benchmark.
    reproject(classified_benchmark, 
              destination = classified_benchmark_projected,
              src_transform = domain.transform, 
              src_crs = source_crs,
              src_nodata = new_nodata_value,
              dst_transform = new_benchmark_transform, 
              dst_crs = reference.crs,
              dst_nodata = new_nodata_value,
              dst_resolution = reference.res,
              resampling = Resampling.nearest)
    
    #Update profile using reference profile as base (data type, NODATA, transform, width/height).
    profile = reference.profile
    profile.update(transform = new_benchmark_transform)
    profile.update(dtype = rasterio.uint8)
    profile.update(nodata = new_nodata_value) 
    profile.update (width = new_benchmark_width)
    profile.update(height = new_benchmark_height)   
    
    return classified_benchmark_projected, profile


def calculate_metrics_from_agreement_raster(agreement_raster):

    ''' Calculates metrics from an agreement raster '''

    agreement_encoding_digits_to_names = { 0: "TN",
                                           1: "FN",
                                           2: "FP",
                                           3: "TP"
                                          }


    if isinstance(agreement_raster,rasterio.DatasetReader):
        pass
    elif isinstance(agreement_raster,str):
        agreement_raster = rasterio.open(agreement_raster)
    else:
        raise TypeError(f"{agreement_raster} is not a Rasterio Dataset Reader or a filepath to a raster")

    # cycle through blocks 
    totals = dict.from_keys(list(range(4)),0)
    for idx,wind in agreement_raster.block_windows(1):
        window_data = agreement_raster.read(1,window=wind)
        values, counts = np.unique(window_data,return_counts=True)
        for val,cts in values_counts:
            totals[val] += cts

    results = dict()
    for digit,count in totals.items():
        results[agreement_encoding_digits_to_names[digit]] = count
   
    return(results)


# evaluation metric fucntions

def csi(TP,FP,FN,TN=None):
    ''' Critical Success Index '''
    return TP / (FP + FN + TP)

def tpr(TP,FP,FN,TN=None):
    ''' True Positive Rate '''
    return TP / (TP + FN)

def far(TP,FP,FN,TN=None):
    ''' False Alarm Rate '''
    return FP / (TP + FP)

def mcc(TP,FP,FN,TN=None):

    ''' Matthew's Correlation Coefficient '''
    return (TP*TN - FP*FN) / np.sqrt((TP+FP)*(TP+FN)*(TN+FP)*(TN+FN))

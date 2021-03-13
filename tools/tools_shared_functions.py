#!/usr/bin/env python3

import os
import json
import csv
import rasterio
import pandas as pd
import geopandas as gpd
import requests

from tools_shared_variables import (TEST_CASES_DIR, PRINTWORTHY_STATS, GO_UP_STATS, GO_DOWN_STATS,
                                    ENDC, TGREEN_BOLD, TGREEN, TRED_BOLD, TWHITE, WHITE_BOLD, CYAN_BOLD)

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


#def compare_to_previous(benchmark_category, test_id, stats_modes_list, magnitude, version, test_version_dictionary, version_test_case_dir):
#    text_block = []
#    # Compare to previous stats files that are available.
#    archive_to_check = os.path.join(TEST_CASES_DIR, benchmark_category + 'test_cases', test_id, 'official_versions')
#    for stats_mode in stats_modes_list:
#        archive_dictionary = profile_test_case_archive(archive_to_check, magnitude, stats_mode)
#
#        if archive_dictionary == {}:
#            break
#
#        # Create header for section.
#        header = [stats_mode]
#        for previous_version, paths in archive_dictionary.items():
#            header.append(previous_version)
#        header.append(version)
#        text_block.append(header)
#
#        # Loop through stats in PRINTWORTHY_STATS for left.
#        for stat in PRINTWORTHY_STATS:
#            stat_line = [stat]
#            for previous_version, paths in archive_dictionary.items():
#                # Load stats for previous version.
#                previous_version_stats_json_path = paths['stats_json']
#                if os.path.exists(previous_version_stats_json_path):
#                    previous_version_stats_dict = json.load(open(previous_version_stats_json_path))
#
#                    # Append stat for the version to state_line.
#                    stat_line.append(previous_version_stats_dict[stat])
#
#
#            # Append stat for the current version to stat_line.
#            stat_line.append(test_version_dictionary[stats_mode][stat])
#
#            text_block.append(stat_line)
#
#        text_block.append([" "])
#
#    regression_report_csv = os.path.join(version_test_case_dir, 'stats_summary.csv')
#    with open(regression_report_csv, 'w', newline='') as csvfile:
#        csv_writer = csv.writer(csvfile)
#        csv_writer.writerows(text_block)
#
#    print()
#    print("--------------------------------------------------------------------------------------------------")
#
#    stats_mode = stats_modes_list[0]
#    try:
#        last_version_index = text_block[0].index('dev_latest')
#    except ValueError:
#        try:
#            last_version_index = text_block[0].index('fim_2_3_3')
#        except ValueError:
#            try:
#                last_version_index = text_block[0].index('fim_1_0_0')
#            except ValueError:
#                print(TRED_BOLD + "Warning: " + ENDC + "Cannot compare " + version + " to a previous version because no authoritative versions were found in previous_versions directory. Future version of run_test_case may allow for comparisons between dev versions.")
#                print()
#                continue
#
#
#
#    for line in text_block:
#        first_item = line[0]
#        if first_item in stats_modes_list:
#            current_version_index = line.index(version)
#            if first_item != stats_mode:  # Update the stats_mode and print a separator.
#                print()
#                print()
#                print("--------------------------------------------------------------------------------------------------")
#            print()
#            stats_mode = first_item
#            print(CYAN_BOLD + current_huc + ": " + magnitude.upper(), ENDC)
#            print(CYAN_BOLD + stats_mode.upper().replace('_', ' ') + " METRICS" + ENDC)
#            print()
#
#            color = WHITE_BOLD
#            metric_name = '      '.center(len(max(PRINTWORTHY_STATS, key=len)))
#            percent_change_header = '% CHG'
#            difference_header = 'DIFF'
#            current_version_header = line[current_version_index].upper()
#            last_version_header = line[last_version_index].upper()
#            # Print Header.
#            print(color + metric_name + "      " + percent_change_header.center((7)) + "       " + difference_header.center((15))  + "    " + current_version_header.center(18) + " " + last_version_header.center(18), ENDC)
#        # Format and print stat row.
#        elif first_item in PRINTWORTHY_STATS:
#            stat_name = first_item.upper().center(len(max(PRINTWORTHY_STATS, key=len))).replace('_', ' ')
#            current_version = round((line[current_version_index]), 3)
#            last_version = round((line[last_version_index]) + 0.000, 3)
#            difference = round(current_version - last_version, 3)
#            if difference > 0:
#                symbol = '+'
#                if first_item in GO_UP_STATS:
#                    color = TGREEN_BOLD
#                elif first_item in GO_DOWN_STATS:
#                    color = TRED_BOLD
#                else:
#                    color = TWHITE
#            if difference < 0:
#                symbol = '-'
#                if first_item in GO_UP_STATS:
#                    color = TRED_BOLD
#                elif first_item in GO_DOWN_STATS:
#                    color = TGREEN_BOLD
#                else:
#                    color = TWHITE
#
#            if difference == 0 :
#                symbol, color = '+', TGREEN
#            percent_change = round((difference / last_version)*100,2)
#
#            print(WHITE_BOLD + stat_name + ENDC + "     " + color + (symbol + " {:5.2f}".format(abs(percent_change)) + " %").rjust(len(percent_change_header)), ENDC + "    " + color + ("{:12.3f}".format((difference))).rjust(len(difference_header)), ENDC + "    " + "{:15.3f}".format(current_version).rjust(len(current_version_header)) + "   " + "{:15.3f}".format(last_version).rjust(len(last_version_header)) + "  ")
#
#    print()
#    print()
#    print()
#    print("--------------------------------------------------------------------------------------------------")
#    print()
#


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

    print("-----> Evaluating performance across the total area...")
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

                print("-----> Masking at " + poly_layer + "...")
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

                print("-----> Evaluating performance at " + poly_layer + "...")
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
        #get crosswalk info (always last dictionary in list)
        *metadata_list, crosswalk_info = metadata_list
        #Update each dictionary with timestamp and crosswalk info
        for metadata in metadata_list:
            metadata.update({"wrds_timestamp": timestamp})
            metadata.update(crosswalk_info)
        #If count is 1
        if location_count == 1:
            metadata_list = metadata_json['locations'][0]
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
    Converts input DataFrame to a GeoDataFrame using the lat/lon attributes
    with sites containing null lat/lon removed. Reprojects GeoDataFrame
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
    #Define EPSG codes for possible usgs latlon datum names (NAD83WGS84 assigned NAD83)
    crs_lookup ={'NAD27':'EPSG:4267', 'NAD83':'EPSG:4269', 'NAD83WGS84': 'EPSG:4269'}
    #Create empty geodataframe and define CRS for potential horizontal datums
    metadata_gdf = gpd.GeoDataFrame()
    #Iterate through each site
    for metadata in metadata_list:
        #Convert metadata to json
        df = pd.json_normalize(metadata)
        #Columns have periods due to nested dictionaries
        df.columns = df.columns.str.replace('.', '_')
        #Drop any metadata sites that don't have lat/lon populated
        df.dropna(subset = ['identifiers_nws_lid','usgs_data_latitude','usgs_data_longitude'], inplace = True)
        #If dataframe still has data
        if not df.empty:
            #Get horizontal datum (use usgs) and assign appropriate EPSG code
            h_datum = df.usgs_data_latlon_datum_name.item()
            src_crs = crs_lookup[h_datum]
            #Convert dataframe to geodataframe using lat/lon (USGS)
            site_gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.usgs_data_longitude, df.usgs_data_latitude), crs =  src_crs)
            #Reproject to huc 8 crs
            site_gdf = site_gdf.to_crs(huc8.crs)
            #Append site geodataframe to metadata geodataframe
            metadata_gdf = metadata_gdf.append(site_gdf, ignore_index = True)

    #Trim metadata to only have certain fields.
    if not retain_attributes:
        metadata_gdf = metadata_gdf[['identifiers_nwm_feature_id', 'identifiers_nws_lid', 'geometry']]
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
def get_thresholds(threshold_url, location_ids, physical_element = 'all', threshold = 'all', bypass_source_flag = False):
    '''
    Get nws_lid threshold stages and flows (i.e. bankfull, action, minor,
    moderate, major). Returns a dictionary for stages and one for flows.

    Parameters
    ----------
    threshold_url : STR
        WRDS threshold API.
    location_ids : STR
        nws_lid code (only a single code).
    physical_element : STR, optional
        Physical element option. The default is 'all'.
    threshold : STR, optional
        Threshold option. The default is 'all'.
    bypass_source_flag : BOOL, optional
        Special case if calculated values are not available (e.g. no rating
        curve is available) then this allows for just a stage to be returned.
        Used in case a flow is already known from another source, such as
        a model. The default is False.

    Returns
    -------
    stages : DICT
        Dictionary of stages at each threshold.
    flows : DICT
        Dictionary of flows at each threshold.

    '''

    url = f'{threshold_url}/{physical_element}/{threshold}/{location_ids}'
    response = requests.get(url)
    if response.ok:
        thresholds_json = response.json()
        #Get metadata
        thresholds_info = thresholds_json['nws_stream_thresholds']
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
            #If neither USGS or NRLDB is available
            else:
                #A flag option for cases where only a stage is needed for USGS scenario where a rating curve source is not available yet stages are available for the site. If flag is enabled, then stages are retrieved from the first record in thresholds_info. Typically the flows will not be populated as no rating curve is available. Flag should only be enabled when flows are already supplied by source (e.g. USGS) and threshold stages are needed.
                if bypass_source_flag:
                    threshold_data = thresholds_info[0]
                else:
                    threshold_data = []
            #Get stages and flows for each threshold
            if threshold_data:
                stages = threshold_data['stage_values']
                flows = threshold_data['calc_flow_values']
                #Add source information to stages and flows. Flows source inside a nested dictionary. Remove key once source assigned to flows.
                stages['source'] = threshold_data['metadata']['threshold_source']
                flows['source'] = flows['rating_curve']['source']
                flows.pop('rating_curve', None)
                #Add timestamp WRDS data was retrieved.
                stages['wrds_timestamp'] = response.headers['Date']
                flows['wrds_timestamp'] = response.headers['Date']
                #Add Site information
                stages['nws_lid'] = threshold_data['metadata']['nws_lid']
                flows['nws_lid'] = threshold_data['metadata']['nws_lid']
                stages['usgs_site_code'] = threshold_data['metadata']['usgs_site_code']
                flows['usgs_site_code'] = threshold_data['metadata']['usgs_site_code']
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

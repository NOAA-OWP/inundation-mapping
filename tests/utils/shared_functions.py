#!/usr/bin/env python3

import os
import json
import csv
import rasterio
import pandas as pd
from utils.shared_variables import (TEST_CASES_DIR, PRINTWORTHY_STATS, GO_UP_STATS, GO_DOWN_STATS,
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
    

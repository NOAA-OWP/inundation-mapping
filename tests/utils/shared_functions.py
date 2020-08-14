#!/usr/bin/env python3


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
    FAR             = false_positives / (true_positives + false_positives)
    CSI             = true_positives / (true_positives + false_positives + false_negatives)
    BIAS            = (true_positives + false_positives) / (true_positives + false_negatives)
    
    # Compute equitable threat score (ETS) / Gilbert Score. 
    a_ref = ((true_positives + false_positives)*(true_positives + false_negatives)) / total_population
    EQUITABLE_THREAT_SCORE = (true_positives - a_ref) / (true_positives - a_ref + false_positives + false_negatives)

    total_population = true_positives + false_positives + true_negatives + false_negatives
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
    MCC = (TP*TN - FP*FN)/ np.sqrt((TP+FP)*(TP+FN)*(TN+FP)*(TN+FN))
    
    if masked_count != None:
        total_pop_and_mask_pop = total_population + masked_count
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
        
    total_population = true_positives + false_positives + true_negatives + false_negatives

    predPositive_perc = (predPositive / total_population) * 100
    predNegative_perc = (predNegative / total_population) * 100
    obsPositive_perc = (obsPositive / total_population) * 100
    obsNegative_perc = (obsNegative / total_population) * 100
    
    positiveDiff_perc = predPositive_perc - obsPositive_perc
    
    prevalence = (true_positives + false_negatives) / total_population
    PPV = true_positives / predPositive
    NPV = true_negatives / predNegative
    TPR = true_positives / obsPositive
    TNR = true_negatives / obsNegative
    ACC = (true_positives + true_negatives) / total_population
    Bal_ACC = np.mean([TPR,TNR])
    F1_score = (2*true_positives) / (2*true_positives + false_positives + false_negatives)

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


def get_contingency_table_from_binary_rasters(benchmark_raster_path, predicted_raster_path, agreement_raster=None, mask_values=None, additional_layers_dict={}, exclusion_mask=""):
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

    
#    # Mask agreement array according to mask catchments.
#    for value in mask_values:
#        agreement_array = np.where(np.absolute(predicted_array_raw) == int(value), 4, agreement_array)
        

    agreement_array = np.add(benchmark_array, 2*predicted_array)
    agreement_array = np.where(agreement_array>4, 10, agreement_array)
    
    del benchmark_src, benchmark_array, predicted_array, predicted_array_raw

    # Mask agreement_array with waterbody raster 100m buffer.
    if exclusion_mask != "":
        exclusion_src = rasterio.open(exclusion_mask)
    
        exclusion_src = rasterio.open(exclusion_mask)
        
        exclusion_array_original = exclusion_src.read(1)
        exclusion_array = np.empty(agreement_array.shape, dtype=np.int8)
                
        print("-----> Masking waterbodies...")
        reproject(exclusion_array_original, 
                  destination = exclusion_array,
                  src_transform = exclusion_src.transform, 
                  src_crs = exclusion_src.crs,
                  src_nodata = exclusion_src.nodata,
                  dst_transform = predicted_src.transform, 
                  dst_crs = predicted_src.crs,
                  dst_nodata = exclusion_src.nodata,
                  dst_resolution = predicted_src.res,
                  resampling = Resampling.nearest)
                    
        # Perform mask.
        agreement_array = np.where(exclusion_array == 1, 4, agreement_array)

    contingency_table_dictionary = {}
    
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
            f.write("%s\n" % '4: Waterbody area (excluded from contingency table analysis). Waterbody mask: {exclusion_mask}'.format(exclusion_mask=exclusion_mask))
            f.write("%s\n" % 'Results produced at: {current_time}'.format(current_time=current_time))
                          
    # Store summed pixel counts in dictionary.
    contingency_table_dictionary.update({'total_area':{'true_negatives': int((agreement_array == 0).sum()),
                                                      'false_negatives': int((agreement_array == 1).sum()),
                                                      'false_positives': int((agreement_array == 2).sum()),
                                                      'true_positives': int((agreement_array == 3).sum()),
                                                      'masked_count': int((agreement_array == 4).sum())
                                                      }})                               
    
        
        
    # Parse through dictionary of other layers and create contingency table metrics for the desired area. Layer must be raster with same shape as agreement_raster.
    if additional_layers_dict != {}:
        for layer_name in additional_layers_dict:
            print("-----> Evaluating performance at " + layer_name + "...")
            layer_path = additional_layers_dict[layer_name]
            layer_src = rasterio.open(layer_path)
            
            layer_array_original = layer_src.read(1)
            layer_array = np.empty(agreement_array.shape, dtype=np.int8)
                    
            reproject(layer_array_original, 
                  destination = layer_array,
                  src_transform = layer_src.transform, 
                  src_crs = layer_src.crs,
                  src_nodata = layer_src.nodata,
                  dst_transform = predicted_src.transform, 
                  dst_crs = predicted_src.crs,
                  dst_nodata = layer_src.nodata,
                  dst_resolution = predicted_src.res,
                  resampling = Resampling.nearest)
                    
            # Omit all areas that spatially disagree with the layer_array.
            layer_agreement_array = np.where(layer_array>0, agreement_array, 10)
            
            # Write the layer_agreement_raster.
            layer_agreement_raster = os.path.join(os.path.split(agreement_raster)[0], layer_name + '_agreement.tif')
            with rasterio.Env():
                profile = predicted_src.profile
                profile.update(nodata=10)
                with rasterio.open(layer_agreement_raster, 'w', **profile) as dst:
                    dst.write(layer_agreement_array, 1)
            
            # Store summed pixel counts in dictionary.
            contingency_table_dictionary.update({layer_name:{'true_negatives': int((layer_agreement_array == 0).sum()),
                                                             'false_negatives': int((layer_agreement_array == 1).sum()),
                                                             'false_positives': int((layer_agreement_array == 2).sum()),
                                                             'true_positives': int((layer_agreement_array == 3).sum()),
                                                             'masked_count': int((layer_agreement_array == 4).sum())
                                                              }})
            del layer_agreement_array

    return contingency_table_dictionary
    

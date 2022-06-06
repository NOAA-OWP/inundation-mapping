import numpy as np
def get_nlcd_counts_inside_flood(feat,masked):
    # Gets NLCD Counds inside flood extent
    feature_stats = {
        'f_HydroID': feat.GetField('HydroID'),
        'f_TotalPixels': int(masked.count()),
        'f_lulc_11': np.count_nonzero((masked == [11])),
        'f_lulc_12': np.count_nonzero((masked == [12])),
        'f_lulc_21': np.count_nonzero((masked == [21])),
        'f_lulc_22': np.count_nonzero((masked == [22])),
        'f_lulc_23': np.count_nonzero((masked == [23])),
        'f_lulc_24': np.count_nonzero((masked == [24])),
        'f_lulc_31': np.count_nonzero((masked == [31])),
        'f_lulc_41': np.count_nonzero((masked == [41])),
        'f_lulc_42': np.count_nonzero((masked == [42])),
        'f_lulc_43': np.count_nonzero((masked == [43])),
        'f_lulc_51': np.count_nonzero((masked == [51])),
        'f_lulc_52': np.count_nonzero((masked == [52])),
        'f_lulc_71': np.count_nonzero((masked == [71])),
        'f_lulc_72': np.count_nonzero((masked == [72])),
        'f_lulc_73': np.count_nonzero((masked == [73])),
        'f_lulc_74': np.count_nonzero((masked == [74])),
        'f_lulc_81': np.count_nonzero((masked == [81])),
        'f_lulc_82': np.count_nonzero((masked == [82])),
        'f_lulc_90': np.count_nonzero((masked == [90])),
        'f_lulc_95': np.count_nonzero((masked == [95])),
        'f_lulc_1': np.count_nonzero((masked == [11])) + np.count_nonzero((masked == [12])),
        'f_lulc_2': np.count_nonzero((masked == [21])) + np.count_nonzero((masked == [22]))
                  + np.count_nonzero((masked == [23])) + np.count_nonzero((masked == [24])),
        'f_lulc_3': np.count_nonzero((masked == [31])),
        'f_lulc_4': np.count_nonzero((masked == [41])) + np.count_nonzero((masked == [42]))
                  + np.count_nonzero((masked == [43])),
        'f_lulc_5': np.count_nonzero((masked == [51])) + np.count_nonzero((masked == [52])),
        'f_lulc_7': np.count_nonzero((masked == [71])) + np.count_nonzero((masked == [72]))
                  + np.count_nonzero((masked == [73])) + np.count_nonzero((masked == [74])),
        'f_lulc_8': np.count_nonzero((masked == [81])) + np.count_nonzero((masked == [82])),
        'f_lulc_9': np.count_nonzero((masked == [90])) + np.count_nonzero((masked == [95]))

    }
    return feature_stats

def get_nlcd_counts(feat, masked):
    # Acquires information for table on each raster attribute per poly feature
    feature_stats = {
        'FID': int(feat.GetFID()),
        'HydroID': feat.GetField('HydroID'),
        'TotalPixels': int(masked.count()),
        'lulc_11': np.count_nonzero((masked == [11])),
        'lulc_12': np.count_nonzero((masked == [12])),
        'lulc_21': np.count_nonzero((masked == [21])),
        'lulc_22': np.count_nonzero((masked == [22])),
        'lulc_23': np.count_nonzero((masked == [23])),
        'lulc_24': np.count_nonzero((masked == [24])),
        'lulc_31': np.count_nonzero((masked == [31])),
        'lulc_41': np.count_nonzero((masked == [41])),
        'lulc_42': np.count_nonzero((masked == [42])),
        'lulc_43': np.count_nonzero((masked == [43])),
        'lulc_51': np.count_nonzero((masked == [51])),
        'lulc_52': np.count_nonzero((masked == [52])),
        'lulc_71': np.count_nonzero((masked == [71])),
        'lulc_72': np.count_nonzero((masked == [72])),
        'lulc_73': np.count_nonzero((masked == [73])),
        'lulc_74': np.count_nonzero((masked == [74])),
        'lulc_81': np.count_nonzero((masked == [81])),
        'lulc_82': np.count_nonzero((masked == [82])),
        'lulc_90': np.count_nonzero((masked == [90])),
        'lulc_95': np.count_nonzero((masked == [95])),
        'lulc_1': np.count_nonzero((masked == [11])) + np.count_nonzero((masked == [12])),
        'lulc_2': np.count_nonzero((masked == [21])) + np.count_nonzero((masked == [22]))
                  + np.count_nonzero((masked == [23])) + np.count_nonzero((masked == [24])),
        'lulc_3': np.count_nonzero((masked == [31])),
        'lulc_4': np.count_nonzero((masked == [41])) + np.count_nonzero((masked == [42]))
                  + np.count_nonzero((masked == [43])),
        'lulc_5': np.count_nonzero((masked == [51])) + np.count_nonzero((masked == [52])),
        'lulc_7': np.count_nonzero((masked == [71])) + np.count_nonzero((masked == [72]))
                  + np.count_nonzero((masked == [73])) + np.count_nonzero((masked == [74])),
        'lulc_8': np.count_nonzero((masked == [81])) + np.count_nonzero((masked == [82])),
        'lulc_9': np.count_nonzero((masked == [90])) + np.count_nonzero((masked == [95]))

    }
    return feature_stats


def get_levee_counts(feat, masked):

    # Acquires information for table on each levee attribute per catchment
    feature_stats = {
        'HydroID': feat.GetField('HydroID'),
        'TotalLeveePixels': int(masked.count()),
    }
    return feature_stats

def get_bridge_counts(feat_masked):
    pass


def get_mask_value_counts(feat, masked):
    # Acquires information for table on each raster attribute per poly feature
    feature_stats = {
        'FID': int(feat.GetFID()),
        'HydroID': feat.GetField('HydroID'),
        'TotalPixels': int(masked.count()),
        'tn': np.count_nonzero((masked == [0])),
        'fn': np.count_nonzero((masked == [1])),
        'fp': np.count_nonzero((masked == [2])),
        'tp': np.count_nonzero((masked == [3])),
        'mp': np.count_nonzero((masked == [4]))
        }
    
    return feature_stats
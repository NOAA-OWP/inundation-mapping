#!/usr/bin/env python3

import os
import pandas as pd
# %matplotlib qt
dir='data/test_cases'
ble_sites = os.listdir(dir)
ble_sites.remove('validation_data_ble')
ble_sites.remove('validation_data_legacy')
branch_name = 'mannings_optimal'

# obj_function_results = pd.DataFrame({"FIM 1.0.0": pd.Series([], dtype='float'),
#                                      "FIM 2.3.3": pd.Series([], dtype='float'),
#                                      str("FIM " + branch_name): pd.Series([], dtype='float'),
#                                      "BLE site": pd.Series([], dtype='str')})
obj_function_results = pd.DataFrame({"FIM": pd.Series([], dtype='float'),
                                     "Objective Function": pd.Series([], dtype='float'),
                                     "BLE site": pd.Series([], dtype='str')})
for site in ble_sites:

    eval_100 = pd.read_csv(os.path.join(dir,site, 'performance_archive', 'development_versions', branch_name,'100yr','stats_summary.csv'),index_col=0)
    fim_3_newbranch_100=eval_100.loc[['FP_area_km2','FN_area_km2'],branch_name].sum()
    fim_1_0_0_100=eval_100.loc[['FP_area_km2','FN_area_km2'],'fim_1_0_0'].sum()
    fim_2_3_3_100=eval_100.loc[['FP_area_km2','FN_area_km2'],'fim_2_3_3'].sum()

    eval_500 = pd.read_csv(os.path.join(dir,site, 'performance_archive', 'development_versions', branch_name,'500yr','stats_summary.csv'),index_col=0)
    fim_3_newbranch_500=eval_500.loc[['FP_area_km2','FN_area_km2'],branch_name].sum()
    fim_1_0_0_500=eval_500.loc[['FP_area_km2','FN_area_km2'],'fim_1_0_0'].sum()
    fim_2_3_3_500=eval_500.loc[['FP_area_km2','FN_area_km2'],'fim_2_3_3'].sum()


    fim_3_newbranch_objfunc = fim_3_newbranch_100 + fim_3_newbranch_500
    fim_1_0_0_objfunc = fim_1_0_0_100 + fim_1_0_0_500
    fim_2_3_3_objfunc = fim_2_3_3_100 + fim_2_3_3_500


    # {"FIM": [str(branch_name), "1.0.0", "2.3.3"],
    #  "Objective Function":[fim_3_newbranch_objfunc, fim_1_0_0_objfunc, fim_2_3_3_objfunc],
    #  "BLE site": site*3}

    # obj_function_results = obj_function_results.append({"FIM 1.0.0": fim_1_0_0_objfunc,
    #                                                     "FIM 2.3.3": fim_2_3_3_objfunc,
    #                                                     str("FIM " + branch_name): fim_3_newbranch_objfunc,
    #                                                     "BLE site": site},
    #                                                    ignore_index=True)
    obj_function_results = obj_function_results.append({"FIM": str(branch_name),"Objective Function":fim_3_newbranch_objfunc,"BLE site": site},
                                                       ignore_index=True)
    obj_function_results = obj_function_results.append({"FIM": "1.0.0","Objective Function":fim_1_0_0_objfunc,"BLE site": site},
                                                       ignore_index=True)
    obj_function_results = obj_function_results.append({"FIM": "2.3.3","Objective Function":fim_2_3_3_objfunc,  "BLE site": site},
                                                       ignore_index=True)


obj_function_results.to_csv('data/temp/ofs.csv')


# import numpy as np
# import seaborn as sns
# import matplotlib.pyplot as plt
#
# sns.set_context('paper')
# # create plot
# sns.barplot(x = "BLE site", y = "Objective Function", hue = "FIM", data = obj_function_results,
#             palette = 'hls',
#             capsize = 0.05,
#             saturation = 8,
#             errcolor = 'gray', errwidth = 2,
#             ci = 'sd')

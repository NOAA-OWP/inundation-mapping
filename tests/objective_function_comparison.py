#!/usr/bin/env python3

import os
import pandas as pd
# %matplotlib qt
dir='data/test_cases'
ble_sites = os.listdir(dir)
ble_sites.remove('validation_data_ble')
ble_sites.remove('validation_data_legacy')
branch_name_huc8 = 'ble_test_sites_huc8'
branch_name_huc6 = 'ble_test_sites_huc6'
stat_list = ['fim_1_0_0', 'fim_2_3_3', 'dev_b0bb2c6',branch_name_huc8,branch_name_huc6]
# obj_function_results = pd.DataFrame({"FIM": pd.Series([], dtype='float'),
#                                      "Objective Function": pd.Series([], dtype='float'),
#                                      "BLE site": pd.Series([], dtype='str')})
eval_all = pd.DataFrame([])
for site in ble_sites:
    print(site)
    eval_100_huc8 = pd.read_csv(os.path.join(dir,site, 'performance_archive', 'development_versions', branch_name_huc8,'100yr','stats_summary.csv'),index_col=0)
    eval_100_huc6 = pd.read_csv(os.path.join(dir,site, 'performance_archive', 'development_versions', branch_name_huc6,'100yr','stats_summary.csv'),index_col=0)
    eval_100 = pd.concat([eval_100_huc8, eval_100_huc6[branch_name_huc6]], axis=1)
    eval_100['eval'] = '100yr'
    # fim_3_newbranch_100=eval_100.loc[['FP_area_km2','FN_area_km2'],branch_name].sum()
    # fim_1_0_0_100=eval_100.loc[['FP_area_km2','FN_area_km2'],'fim_1_0_0'].sum()
    # fim_2_3_3_100=eval_100.loc[['FP_area_km2','FN_area_km2'],'fim_2_3_3'].sum()

    eval_500_huc8 = pd.read_csv(os.path.join(dir,site, 'performance_archive', 'development_versions', branch_name_huc8,'500yr','stats_summary.csv'),index_col=0)
    eval_500_huc6 = pd.read_csv(os.path.join(dir,site, 'performance_archive', 'development_versions', branch_name_huc6,'500yr','stats_summary.csv'),index_col=0)
    eval_500 = pd.concat([eval_500_huc8, eval_500_huc6[branch_name_huc6]], axis=1)
    eval_500['eval'] = '500yr'
    # fim_3_newbranch_500=eval_500.loc[['FP_area_km2','FN_area_km2'],branch_name].sum()
    # fim_1_0_0_500=eval_500.loc[['FP_area_km2','FN_area_km2'],'fim_1_0_0'].sum()
    # fim_2_3_3_500=eval_500.loc[['FP_area_km2','FN_area_km2'],'fim_2_3_3'].sum()
    eval = eval_100.append(eval_500)
    eval = eval.filter(items=stat_list)
    eval = eval.reindex(columns=stat_list)
    eval['site'] = site
    eval_all = eval_all.append(eval)

eval_all.to_csv('data/temp/ble_results.csv')
    # fim_3_newbranch_objfunc = fim_3_newbranch_100 + fim_3_newbranch_500
    # fim_1_0_0_objfunc = fim_1_0_0_100 + fim_1_0_0_500
    # fim_2_3_3_objfunc = fim_2_3_3_100 + fim_2_3_3_500
    #
    #
    # obj_function_results = obj_function_results.append({"FIM": str(branch_name),"Objective Function":fim_3_newbranch_objfunc,"BLE site": site},
    #                                                    ignore_index=True)
    # obj_function_results = obj_function_results.append({"FIM": "1.0.0","Objective Function":fim_1_0_0_objfunc,"BLE site": site},
    #                                                    ignore_index=True)
    # obj_function_results = obj_function_results.append({"FIM": "2.3.3","Objective Function":fim_2_3_3_objfunc,  "BLE site": site},
    #                                                    ignore_index=True)


# obj_function_results.to_csv('data/temp/ofs.csv')


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

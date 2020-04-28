import matplotlib.pyplot as plt
import numpy as np
import json
import geopandas as gpd
import pandas as pd
from raster import Raster
import os
from shapely.geometry import Point

projectDirectory = os.path.join(os.path.expanduser('~'),'projects','foss_fim')
dataDirectory = os.path.join(projectDirectory,'data')

# nwm_catchments_fileName = os.path.join(dataDirectory,'nwm','NWMCatchment.shp')
# nwm_flows_fileName = os.path.join(dataDirectory,'test2','inputs','nwm_flows_proj_120903_v2.gpkg')
#
# esri_catchments_fileName = os.path.join(projectDirectory,'tests','CatchmentH.shp')
esri_flows_fileName = os.path.join(projectDirectory,'tests','eval_1','final_esri_hand_outputs','FPRiver.gpkg')

# foss_catchments_fileName = os.path.join(dataDirectory,'test2','outputs','gw_catchments_reaches_clipped_addedAttributes_crosswalked.gpkg')
# foss_raster_catchments_fileName = os.path.join(dataDirectory,'test2','outputs','gw_catchments_reaches_clipped_addedAttributes.tif')
foss_flows_fileName = os.path.join(dataDirectory,'test2','outputs','demDerived_reaches_split_clipped_addedAttributes_crosswalked.gpkg')
foss_flows_fileName = os.path.join(dataDirectory,'test2','outputs','NHDPlusBurnLineEvent_subset_split_clipped_addedAttributes_crosswalked.gpkg')
# foss_flows_fileName = os.path.join(dataDirectory,'test2','outputs_v32','demDerived_reaches_split_clipped_addedAttributes_crosswalked.gpkg')

esri_src_fileName = os.path.join(projectDirectory,'tests','eval_1','final_esri_hand_outputs','120903_channel_properties.json')
foss_src_fileName = os.path.join(dataDirectory,'test2','outputs','src.json')
# foss_src_fileName = os.path.join(dataDirectory,'test2','outputs_v32','src.json')

esri_src_table_fileName = os.path.join(projectDirectory,'tests','eval_1','final_esri_hand_outputs','pf_ModelStream.csv')
foss_src_table_fileName = os.path.join(dataDirectory,'test2','outputs','src_full_crosswalked.csv')
# foss_src_table_fileName = os.path.join(dataDirectory,'test2','outputs_v32','src_full_crosswalked.csv')

esri_cw_fileName = os.path.join(projectDirectory,'tests','eval_1','final_esri_hand_outputs','cross_walk_table_esri_120903.csv')
foss_cw_fileName = os.path.join(dataDirectory,'test2','outputs','crosswalk_table.csv')
# foss_cw_fileName = os.path.join(dataDirectory,'test2','outputs_v32','crosswalk_table.csv')

esri_rem_fileName = os.path.join(projectDirectory,'tests','eval_1','final_esri_hand_outputs','hand_120903.tif')
foss_rem_fileName = os.path.join(dataDirectory,'test2','outputs','rem_clipped_zeroed_masked.tif')

forecast_100_fileName = os.path.join(projectDirectory,'tests','eval_1','validation_data','forecast_120903_100yr.csv')

# catchments
# esri_catchments = gpd.read_file(esri_catchments_fileName)
# foss_catchments = gpd.read_file(foss_catchments_fileName)
# # foss_raster_catchments = Raster(foss_raster_catchments_fileName)
# nwm_catchments = gpd.read_file(nwm_catchments_fileName)
#
# # flows
esri_flows = gpd.read_file(esri_flows_fileName)
foss_flows = gpd.read_file(foss_flows_fileName)
# nwm_flows = gpd.read_file(nwm_flows_fileName)

# slopes
# esri_slope=Raster('eval_1/final_esri_hand_outputs/unitrun/hdr.adf')
# foss_slope=Raster('../data/test2/outputs/slopes_d8_thalwegCond_filled_clipped_masked.tif')
#
# foss_cell_area = abs(foss_rem.gt[1]*foss_rem.gt[5])
# esri_cell_area = abs(esri_rem.gt[1] * esri_rem.gt[5])
#
# foss_dv_bool = foss_rem.array!=foss_rem.ndv
# esri_dv_bool = esri_rem.array!=esri_rem.ndv
#
# esri_slope.array = esri_slope.array[esri_dv_bool] - 1
#
# foss_slopes_trans = np.sqrt(1+(foss_slope.array[foss_dv_bool])**2)
# esri_slopes_trans = np.sqrt(1+(esri_slope.array[esri_dv_bool])**2)
# for d in np.array(range(0,30))*.3048:
#     foss_area = np.sum(np.logical_and(foss_dv_bool,foss_rem.array<=d) * foss_cell_area * foss_slopes_trans)
#     esri_area = np.sum(np.logical_and(esri_dv_bool,esri_rem.array<=d) * esri_cell_area * esri_slopes_trans)





# sinuosity

def sinuosity(flows_geometry):

    numberOfGeoms = len(flows_geometry)
    arc_lengths = [-1] * numberOfGeoms ; straight_lengths = [-1] * numberOfGeoms

    for i,geom in enumerate(flows_geometry):
        arc_lengths[i] = geom.length

        point_1 = Point(*geom.bounds[0:2])
        point_2 = Point(*geom.bounds[2:4])

        straight_lengths[i] = point_1.distance(point_2)

    sinuosity_table = pd.DataFrame({'arc_lengths' : arc_lengths , 'straight_lengths' : straight_lengths})

    return(sinuosity_table)

esri_sinuosity = sinuosity(esri_flows.geometry)
foss_sinuosity = sinuosity(foss_flows.geometry)

avg_esri_si = (esri_sinuosity['arc_lengths']/esri_sinuosity['straight_lengths']).mean()
avg_foss_si = (foss_sinuosity['arc_lengths']/foss_sinuosity['straight_lengths']).mean()

print(avg_esri_si,avg_foss_si,avg_esri_si/avg_foss_si)


# SRCS's
with open(esri_src_fileName,'r') as f:
    esri_src = json.load(f)

with open(foss_src_fileName,'r') as f:
    foss_src = json.load(f)

esri_cw = pd.read_csv(esri_cw_fileName,dtype=int)
foss_cw = pd.read_csv(foss_cw_fileName,dtype=int)

esri_rem = Raster(esri_rem_fileName)
foss_rem = Raster(foss_rem_fileName)

esri_src_table = pd.read_csv(esri_src_table_fileName,dtype={'A':float, 'B':float, 'H':float, 'Length_m':float, 'P':float, 'R':float, 'HydroID':int, 'Q':float})
foss_src_table = pd.read_csv(foss_src_table_fileName,dtype={'HydroID':int, 'Stage':float, 'Number of Cells':int, 'SurfaceArea (m2)':float,
       'BedArea (m2)':float, 'Volume (m3)':float, 'SLOPE':float, 'LENGTHKM':float, 'AREASQKM':float,
       'Roughness':float, 'TopWidth (m)':float, 'WettedPerimeter (m)':float, 'WetArea (m2)':float,
       'HydraulicRadius (m)':float, 'Discharge (m3s-1)':float, 'feature_id':int})

forecast_100 = pd.read_csv(forecast_100_fileName,dtype={'feature_id' : int , 'discharge' : float})

intersection_of_feature_id = list(set(esri_cw['feature_id'].unique()) & set(foss_cw['feature_id'].unique()) & set(forecast_100['feature_id'].unique())  )


max_q = np.max(forecast_100['discharge'])
# print(max_q)

esri_src_table['BA'] = esri_src_table['P'] * esri_src_table['Length_m']
esri_src_table['V'] = esri_src_table['A'] * esri_src_table['Length_m']

esri_src_table = esri_src_table[:][esri_src_table['H']<=10]
foss_src_table = foss_src_table[:][foss_src_table['Stage']<=10]
# print(esri_src_table.sort_values(by=['HydroID','H']))

esri_cw = esri_cw[:][esri_cw['feature_id'].isin(intersection_of_feature_id)]
foss_cw = foss_cw[:][foss_cw['feature_id'].isin(intersection_of_feature_id)]

esri_src_table = esri_src_table.merge(esri_cw,on='HydroID',how='inner')
foss_src_table = foss_src_table.merge(foss_cw,on='HydroID',how='inner')

foss_src_table.drop(columns='feature_id_y',inplace=True)
foss_src_table.rename(columns={'feature_id_x':'feature_id'},inplace=True)
# esri_hids = esri_cw['HydroID'][esri_cw['feature_id'].isin(intersection_of_feature_id)]
# foss_hids = foss_cw['HydroID'][foss_cw['feature_id'].isin(intersection_of_feature_id)]

# esri_src_table = esri_src_table[:][esri_src_table['HydroID'].isin(esri_hids)]
# foss_src_table = foss_src_table[:][foss_src_table['HydroID'].isin(foss_hids)]

# esri_src_table = esri_src_table[:][esri_src_table['HydroID'].isin(esri_hids)]
# foss_src_table = foss_src_table[:][foss_src_table['HydroID'].isin(foss_hids)]

foss_src_table['Length_m'] = foss_src_table['LENGTHKM'] *1000
esri_src_table = esri_src_table.merge(esri_flows[['HydroID','S0']],on='HydroID',how='left')

foss_src_table.rename(columns={'Stage' : 'H' , 'BedArea (m2)' : 'BA','Volume (m3)' : 'V' ,
                                'SLOPE' : 'S0' , 'WettedPerimeter (m)': 'P', 'WetArea (m2)' : 'A',
                                'HydraulicRadius (m)':'R', 'Discharge (m3s-1)': 'Q'},inplace=True)

foss_src_table = foss_src_table[['H' , 'BA','V' ,'S0' ,'P','A','R','Q','feature_id','HydroID','Length_m']]

foss_src_table['n'] = 0.06
esri_src_table['n'] = 0.06

# esri_src_table.sort_values(by=['HydroID','H'],inplace=True)
# foss_src_table.sort_values(by=['HydroID','H'],inplace=True)

esri_src_table.drop(columns='HydroID',inplace=True)
foss_src_table.drop(columns='HydroID',inplace=True)

esri_src_table = esri_src_table.astype({'H' : str})
foss_src_table = foss_src_table.astype({'H' : str})
# esri_src_table = esri_src_table.groupby(['feature_id','H']).mean()
# foss_src_table = foss_src_table.groupby(['feature_id','H']).mean()
# esri_src_table = esri_src_table.astype({'H' :float})
# foss_src_table = foss_src_table.astype({'H' :float})

# esri_src_table.reset_index(drop=True)
# foss_src_table.reset_index(drop=True)

src_table = foss_src_table.merge(esri_src_table,suffixes=('_foss','_esri'),on=['feature_id','H'])
# esri_src_table.sort_values(by=['HydroID','H'],inplace=True)

# src_table.sort_values(by=['feature_id','H'],inplace=True)
# src_table.reset_index(drop=False,inplace=True)

src_table = src_table.groupby('H').mean()
src_table.reset_index(drop=False,inplace=True)
src_table = src_table.astype({'H' :float})
src_table.sort_values(by=['H'],inplace=True)
# print(src_table.index)

pd.set_option('display.max_rows', 2000)
# print(src_table[['feature_id','H','V_esri','V_foss']].iloc[0:200,:])
# print(src_table)
percent_error_V = 100 * (src_table['V_foss'].iloc[1:]-src_table['V_esri'].iloc[1:])/src_table['V_esri'].iloc[1:]
percent_error_BA = 100 * (src_table['BA_foss'].iloc[1:]-src_table['BA_esri'].iloc[1:])/src_table['BA_esri'].iloc[1:]
percent_error_L = 100 * (src_table['Length_m_foss']-src_table['Length_m_esri'])/src_table['Length_m_esri']
percent_error_S = 100 * (src_table['S0_foss']-src_table['S0_esri'])/src_table['S0_esri']
percent_error_Q = 100 * (src_table['Q_foss'].iloc[1:]-src_table['Q_esri'].iloc[1:])/src_table['Q_esri'].iloc[1:]

multiplied_error_V = (src_table['V_foss'].iloc[1:]/src_table['V_esri'].iloc[1:])**(5/3)
multiplied_error_BA = (src_table['BA_foss'].iloc[1:]/src_table['BA_esri'].iloc[1:])**(2/3)
multiplied_error_L = (src_table['Length_m_foss']/src_table['Length_m_esri'])
multiplied_error_S = (src_table['S0_foss']/src_table['S0_esri'])**(1/2)
multiplied_error_Q = (src_table['Q_foss'].iloc[1:]/src_table['Q_esri'].iloc[1:])

print(percent_error_V.mean(),percent_error_BA.mean(),percent_error_L.mean(),percent_error_S.mean(),percent_error_Q.mean())
print(multiplied_error_V.mean(),multiplied_error_BA.mean(),multiplied_error_L.mean(),multiplied_error_S.mean(),multiplied_error_Q.mean())
print((multiplied_error_V.mean()*multiplied_error_S.mean())/(multiplied_error_BA.mean()*multiplied_error_L.mean()))
# print(percent_error_V,percent_error_BA,percent_error_L,percent_error_S,percent_error_Q)
# exit()
#
# tot_V_esri = [] ; tot_V_foss = []
# foss_dv_bool = foss_rem.array!=foss_rem.ndv
# esri_dv_bool = esri_rem.array!=esri_rem.ndv
# for d in np.array(range(0,30))*.3048:
#     foss_cell_area = abs(foss_rem.gt[1]*foss_rem.gt[5])
#     esri_cell_area = abs(esri_rem.gt[1] * esri_rem.gt[5])
#     foss_volume = np.sum(d-foss_rem.array[np.logical_and(foss_dv_bool,foss_rem.array<=d)]) * foss_cell_area
#     esri_volume = np.sum(d-esri_rem.array[np.logical_and(esri_dv_bool,esri_rem.array<=d)]) * esri_cell_area
#     tot_V_esri = tot_V_esri + [esri_volume] ; tot_V_foss = tot_V_foss + [foss_volume]
#
# print(np.array(tot_V_foss).mean()/np.array(tot_V_esri).mean())
# print((foss_dv_bool.sum() * foss_cell_area) / (esri_dv_bool.sum() * esri_cell_area))





# print(esri_src_table[['feature_id','H','V']].iloc[0:20,:])
# print(foss_src_table)
# print(esri_src_table)




# foss_src_table['HydroID']

stage_list = foss_src[str(500)]['stage_list']
maxLength = len(stage_list)

overall_esri = None
for fid in intersection_of_feature_id:
    esri_hid = esri_cw['HydroID'][esri_cw['feature_id'] == fid].to_numpy()
    foss_hid = foss_cw['HydroID'][foss_cw['feature_id'] == fid].to_numpy()

    # all_esri_q = np.zeros(len(esri_src[str(esri_hid[0])]['stage_list']),dtype=np.float32)
    all_esri_q = None
    for hid in esri_hid:
        current_esri_q = np.array(esri_src[str(hid)]['q_list'])

        if len(current_esri_q) < maxLength:
            nan_array = np.repeat(np.nan, maxLength - len(current_esri_q))
            # print(nan_array)
            current_esri_q = np.hstack((current_esri_q,nan_array))

        if len(current_esri_q) > maxLength:
            current_esri_q = current_esri_q[0:maxLength]

        if all_esri_q is None:
            all_esri_q = current_esri_q
        else:
            all_esri_q = np.vstack((all_esri_q,current_esri_q))

    all_foss_q = None
    for hid in foss_hid:

        current_foss_q = np.array(foss_src[str(hid)]['q_list'])

        if all_foss_q is None:
            all_foss_q = current_foss_q
        else:
            all_foss_q = np.vstack((all_foss_q,current_foss_q))

    # print(all_esri_q.shape,all_foss_q.shape)
    # print(all_esri_q)

    if len(all_esri_q.shape) == 2:
        mean_esri_q = np.nanmean(all_esri_q,axis=0)

    if len(all_foss_q.shape) == 2:
        mean_foss_q = np.nanmean(all_foss_q,axis=0)

    # mean_error = mean_foss_q-mean_esri_q

    # print(mean_esri_q.shape,mean_foss_q.shape,mean_error.shape)

    # mean_abs_error = np.absolute(mean_error)

    if overall_esri is None:
        # overall_error = mean_error
        overall_esri = mean_esri_q
        overall_foss = mean_foss_q
        # overall_abs_error = mean_abs_error
    else:
        # print(mean_error,overall_error.shape)
        # overall_error = np.vstack((overall_error,mean_error))
        overall_esri = np.vstack((overall_esri,mean_esri_q))
        overall_foss = np.vstack((overall_foss,mean_foss_q))
        # overall_abs_error = np.vstack((overall_abs_error,mean_abs_error))

# print(overall_error)
# print(list(overall_error))
# overall_error_q_list = list(np.nanmean(overall_error,axis=0))
overall_esri_q_list = list(np.nanmean(overall_esri,axis=0))
overall_foss_q_list = list(np.nanmean(overall_foss,axis=0))

plt.plot(overall_esri_q_list,stage_list,'r')
plt.plot(overall_foss_q_list,stage_list,'b')
# plt.axis([0,max_q*1.1,0,10])
plt.show()

exit()








# print(np.mean(overall_abs_error,axis=0))

# foss_src = pd.read_csv(foss_src_fileName,skip_blank_lines=True,dtype=object)

# print('\nFeature IDs')
# print("ESRI # of unique catchments: {}".format(len(np.unique(esri_catchments['feature_id']))))
# print("FOSS # of unique catchments: {}".format(len(np.unique(foss_catchments['feature_id']))))
# print("NWM # of unique catchments: {}".format(len(np.unique(nwm_catchments['feature_id']))))
# print("ESRI # of unique flows: {}".format(len(np.unique(esri_flows['feature_id']))))
# print("FOSS # of unique flows: {}".format(len(np.unique(foss_flows['feature_id']))))
# print("NWM # of unique flows: {}".format(len(np.unique(nwm_flows['ID']))))
# print("FOSS # of unique SRC Feature ID: {}".format(len(np.unique(foss_src['feature_id']))))
#
# print('\nHydroID')
# print("ESRI # of unique catchments: {}".format(len(np.unique(esri_catchments['HydroID']))))
# print("FOSS # of unique catchments: {}".format(len(np.unique(foss_catchments['HydroID']))))
# # print("FOSS # of unique catchments in raster: {}".format(len(np.unique(foss_raster_catchments.array[foss_raster_catchments.array!=foss_raster_catchments.ndv]))))
# print("ESRI # of unique flows: {}".format(len(np.unique(esri_flows['HydroID']))))
# print("FOSS # of unique flows: {}".format(len(np.unique(foss_flows['HydroID']))))
# print("ESRI # of unique SRC HydroID: {}".format(len(np.unique(list(esri_src.keys())))))
# print("FOSS # of unique HydroID's: {}".format(len(np.unique(foss_src['HydroID']))))
#
# print(foss_flows['LengthKm'].max())
# print(foss_flows['LengthKm'].mean())

# print(list(esri_src.keys()))

# print(len(foss_src))
# plots src's
# unique_feature_ids_in_foss_src = np.unique(foss_src['feature_id'])

# featID = 5791828

# indices_of_feature = np.where(foss_src['feature_id'] == featID)

# unique_hydro_ids = np.unique(foss_src['HydroID'][indices_of_feature])

# hydroID = '822'
# esri_hydroID = '9975'

# hydroID = '1279'
# esri_hydroID = '10349'

hydroID = '1268'
esri_hydroID = '10743'

hydroID = '1269'
esri_hydroID = '10742'

# indices_of_hydroid = np.where(foss_src['HydroID'] == hydroID)[0]

foss_stages = foss_src[hydroID]['stage_list']
foss_discharge = foss_src[hydroID]['q_list']

# feature_id = foss_src['feature_id'][indices_of_hydroid[0]]
esri_stages = esri_src[esri_hydroID]['stage_list']
esri_flows = esri_src[esri_hydroID]['q_list']


plt.plot(foss_discharge,foss_stages,'b')
plt.plot(esri_flows,esri_stages,'r')
plt.show()

# for hid in unique_hydro_ids:



# for featID in unique_feature_ids_in_foss_src:

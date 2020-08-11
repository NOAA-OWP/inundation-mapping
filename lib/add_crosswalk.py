#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
from numpy import unique
import json
import argparse
import sys

input_catchments_fileName = sys.argv[1]
input_flows_fileName = sys.argv[2]
input_srcbase_fileName = sys.argv[3]
input_majorities_fileName = sys.argv[4]
output_catchments_fileName = sys.argv[5]
output_flows_fileName = sys.argv[6]
output_src_fileName = sys.argv[7]
output_src_json_fileName = sys.argv[8]
output_crosswalk_fileName = sys.argv[9]
output_hydro_table_fileName = sys.argv[10]
input_huc_fileName = sys.argv[11]
input_nwmflows_fileName = sys.argv[12]
mannings_json = sys.argv[13]

input_catchments = gpd.read_file(input_catchments_fileName)
input_flows = gpd.read_file(input_flows_fileName)
input_huc = gpd.read_file(input_huc_fileName)
input_majorities = gpd.read_file(input_majorities_fileName)
input_nwmflows = gpd.read_file(input_nwmflows_fileName)

input_majorities = input_majorities.rename(columns={'_majority' : 'feature_id'})
input_majorities = input_majorities[:][input_majorities['feature_id'].notna()]
input_majorities['feature_id'] = input_majorities['feature_id'].astype(int)

relevant_input_nwmflows = input_nwmflows[input_nwmflows['feature_id'].isin(input_majorities['feature_id'])]
relevant_input_nwmflows = relevant_input_nwmflows.filter(items=['feature_id','order_'])

# output_catchments = input_catchments.merge(input_flows.drop(['geometry'],axis=1),on='HydroID')
output_catchments = input_catchments.merge(input_majorities[['HydroID','feature_id']],on='HydroID')
output_catchments = output_catchments.merge(relevant_input_nwmflows[['order_','feature_id']],on='feature_id')

output_flows = input_flows.merge(input_majorities[['HydroID','feature_id']],on='HydroID')
output_flows = output_flows.merge(relevant_input_nwmflows[['order_','feature_id']],on='feature_id')

# read in manning's n values
with open(mannings_json, "r") as read_file:
    mannings_dict = json.load(read_file)
output_flows['ManningN'] = output_flows['order_'].astype(str).map(mannings_dict)

# calculate src_full
input_src_base = pd.read_csv(input_srcbase_fileName, dtype= object) #
input_src_base['CatchId'] = input_src_base['CatchId'].astype(int)

input_src_base = input_src_base.merge(output_flows[['ManningN','HydroID']],left_on='CatchId',right_on='HydroID')

input_src_base = input_src_base.rename(columns=lambda x: x.strip(" "))
input_src_base = input_src_base.apply(pd.to_numeric,**{'errors' : 'coerce'})
input_src_base['TopWidth (m)'] = input_src_base['SurfaceArea (m2)']/input_src_base['LENGTHKM']/1000
input_src_base['WettedPerimeter (m)'] = input_src_base['BedArea (m2)']/input_src_base['LENGTHKM']/1000
input_src_base['WetArea (m2)'] = input_src_base['Volume (m3)']/input_src_base['LENGTHKM']/1000
input_src_base['HydraulicRadius (m)'] = input_src_base['WetArea (m2)']/input_src_base['WettedPerimeter (m)']
input_src_base['HydraulicRadius (m)'].fillna(0, inplace=True)
input_src_base['Discharge (m3s-1)'] = input_src_base['WetArea (m2)']* \
pow(input_src_base['HydraulicRadius (m)'],2.0/3)* \
pow(input_src_base['SLOPE'],0.5)/input_src_base['ManningN']

# set nans to 0
input_src_base.loc[input_src_base['Stage']==0,['Discharge (m3s-1)']] = 0


# output_src = input_src.rename(columns={'CatchId':'HydroID'})
output_src = input_src_base.drop(columns=['CatchId'])
output_src['HydroID'] = output_src['HydroID'].astype(int)
output_src = output_src.merge(input_majorities[['HydroID','feature_id']],on='HydroID')

output_crosswalk = output_src[['HydroID','feature_id']]
output_crosswalk = output_crosswalk.drop_duplicates(ignore_index=True)

# make hydroTable
output_hydro_table = output_src.loc[:,['HydroID','feature_id','Stage','Discharge (m3s-1)']]
output_hydro_table.rename(columns={'Stage' : 'stage','Discharge (m3s-1)':'discharge_cms'},inplace=True)
output_hydro_table['fossid'] = output_hydro_table.loc[:,'HydroID'].apply(lambda x : str(x)[0:4])
input_huc['fossid'] = input_huc['fossid'].astype(str)
output_hydro_table = output_hydro_table.merge(input_huc.loc[:,['fossid','HUC8']],how='left',on='fossid')
output_hydro_table = output_hydro_table.merge(input_flows.loc[:,['HydroID','LakeID']],how='left',on='HydroID')
output_hydro_table['LakeID'] = output_hydro_table['LakeID'].astype(int)
output_hydro_table = output_hydro_table.rename(columns={'HUC8':'HUC'})
output_hydro_table.drop(columns='fossid',inplace=True)
#output_hydro_table['discharge_cms'] = output_hydro_table['discharge_cms'].round(4)

# make src json
output_src_json = dict()
hydroID_list = unique(output_src['HydroID'])

for hid in hydroID_list:
    indices_of_hid = output_src['HydroID'] == hid
    stage_list = output_src['Stage'][indices_of_hid].astype(float)
    q_list = output_src['Discharge (m3s-1)'][indices_of_hid].astype(float)

    stage_list = stage_list.tolist()
    q_list = q_list.tolist()

    output_src_json[str(hid)] = { 'q_list' : q_list , 'stage_list' : stage_list }

# write out
output_catchments.to_file(output_catchments_fileName, driver="GPKG",index=False)
output_flows.to_file(output_flows_fileName, driver="GPKG", index=False)
output_src.to_csv(output_src_fileName,index=False)
output_crosswalk.to_csv(output_crosswalk_fileName,index=False)
output_hydro_table.to_csv(output_hydro_table_fileName,index=False)

with open(output_src_json_fileName,'w') as f:
    json.dump(output_src_json,f,sort_keys=True,indent=2)

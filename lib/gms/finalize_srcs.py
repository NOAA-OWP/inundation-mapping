#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
from numpy import unique
import json
import argparse
import sys
from utils.shared_functions import getDriver

def finalize_srcs(input_srcbase_fileName,output_src_fileName,output_hydro_table_fileName,mannings_n,calibration_mode=False):

    # read in manning's n values
    with open(mannings_n, "r") as read_file:

        if calibration_mode == False:
            mannings_dict = json.load(read_file)
        else:
            mannings_dict = {}
            for cnt,value in enumerate(mannings_n.split(",")[2:]):
                streamorder = cnt+1
                mannings_dict[str(streamorder)] = value

    # calculate src_full
    input_src_base = pd.read_csv(input_srcbase_fileName, dtype= object)
    if input_src_base.CatchId.dtype != 'int': input_src_base.CatchId = input_src_base.CatchId.astype(int)

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

    output_src = input_src_base.drop(columns=['CatchId'])
    if output_src.HydroID.dtype != 'int': output_src.HydroID = output_src.HydroID.astype(int)

    output_src = output_src.merge(crosswalk[['HydroID','feature_id']],on='HydroID')

    # make hydroTable
    output_hydro_table = output_src.loc[:,['HydroID','feature_id','Stage','Discharge (m3s-1)']]
    output_hydro_table.rename(columns={'Stage' : 'stage','Discharge (m3s-1)':'discharge_cms'},inplace=True)
    if output_hydro_table.HydroID.dtype != 'str': output_hydro_table.HydroID = output_hydro_table.HydroID.astype(str)
    output_hydro_table['HydroID'] = output_hydro_table.HydroID.str.zfill(8)
    output_hydro_table['fossid'] = output_hydro_table.loc[:,'HydroID'].apply(lambda x : str(x)[0:4])
    if input_huc.fossid.dtype != 'str': input_huc.fossid = input_huc.fossid.astype(str)

    output_hydro_table = output_hydro_table.merge(input_huc.loc[:,['fossid','HUC8']],how='left',on='fossid')
    if output_flows.HydroID.dtype != 'str': output_flows.HydroID = output_flows.HydroID.astype(str)
    output_flows['HydroID'] = output_flows.HydroID.str.zfill(8)
    output_hydro_table = output_hydro_table.merge(output_flows.loc[:,['HydroID','LakeID']],how='left',on='HydroID')
    output_hydro_table['LakeID'] = output_hydro_table['LakeID'].astype(int)

    output_hydro_table = output_hydro_table.rename(columns={'HUC8':'HUC'})
    if output_hydro_table.HUC.dtype != 'str': output_hydro_table.HUC = output_hydro_table.HUC.astype(str)
    output_hydro_table.HUC = output_hydro_table.HUC.str.zfill(8)

    output_hydro_table.drop(columns='fossid',inplace=True)
    if output_hydro_table.feature_id.dtype != 'int': output_hydro_table.feature_id = output_hydro_table.feature_id.astype(int)
    if output_hydro_table.feature_id.dtype != 'str': output_hydro_table.feature_id = output_hydro_table.feature_id.astype(str)


        output_src.to_csv(output_src_fileName,index=False)
        output_hydro_table.to_csv(output_hydro_table_fileName,index=False)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-s','--input-srcbase-fileName', help='Base synthetic rating curve table', required=True)
    parser.add_argument('-r','--output-src-fileName', help='Output crosswalked synthetic rating curve table', required=True)
    parser.add_argument('-t','--output-hydro-table-fileName',help='Hydrotable',required=True)
    parser.add_argument('-m','--mannings-n',help='Mannings n. Accepts single parameter set or list of parameter set in calibration mode. Currently input as csv.',required=True)
    parser.add_argument('-c','--calibration-mode',help='Mannings calibration flag',required=False,action='store_true')

    args = vars(parser.parse_args())

    finalize_srcs(**args)

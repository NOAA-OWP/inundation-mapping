#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
from numpy import unique
import json
import argparse
import sys
sys.path.insert(1,"../utils")
from shared_functions import getDriver

def finalize_srcs(input_srcbase,input_srcfull,input_hydrotable,output_srcfull,output_hydrotable):


    # calculate src_full
    input_srcbase = pd.read_csv(input_srcbase, dtype= {'CatchId': int})
    #input_hydrotable = pd.read_csv(input_hydrotable)
    input_srcfull = pd.read_csv(input_srcfull)

    input_srcbase = input_srcbase.merge(input_srcfull[['ManningN','HydroID']],left_on='CatchId',right_on='HydroID')

    input_srcbase = input_srcbase.rename(columns=lambda x: x.strip(" "))
    input_srcbase = input_srcbase.apply(pd.to_numeric,**{'errors' : 'coerce'})
    input_srcbase['TopWidth (m)'] = input_srcbase['SurfaceArea (m2)']/input_srcbase['LENGTHKM']/1000
    input_srcbase['WettedPerimeter (m)'] = input_srcbase['BedArea (m2)']/input_srcbase['LENGTHKM']/1000
    input_srcbase['WetArea (m2)'] = input_srcbase['Volume (m3)']/input_srcbase['LENGTHKM']/1000
    input_srcbase['HydraulicRadius (m)'] = input_srcbase['WetArea (m2)']/input_srcbase['WettedPerimeter (m)']
    input_srcbase['HydraulicRadius (m)'].fillna(0, inplace=True)
    input_srcbase['Discharge (m3s-1)'] = input_srcbase['WetArea (m2)']* \
                                          pow(input_srcbase['HydraulicRadius (m)'],2.0/3)* \
                                          pow(input_srcbase['SLOPE'],0.5)/input_srcbase['ManningN']

    # set nans to 0
    input_srcbase.loc[input_srcbase['Stage']==0,['Discharge (m3s-1)']] = 0

    input_srcbase.rename(columns={"HydroID":'CatchId'},inplace=True)
    print(input_srcbase,input_srcbase.dtypes);exit()

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
    if output_hydro_table.HUC.dtype != 'str':
        output_hydro_table.HUC = output_hydro_table.HUC.astype(str)
    output_hydro_table.HUC = output_hydro_table.HUC.str.zfill(8)

    output_hydro_table.drop(columns='fossid',inplace=True)

    if output_hydro_table.feature_id.dtype != 'int':
        output_hydro_table.feature_id = output_hydro_table.feature_id.astype(int)
    if output_hydro_table.feature_id.dtype != 'str':
        output_hydro_table.feature_id = output_hydro_table.feature_id.astype(str)


        output_src.to_csv(output_src_fileName,index=False)
        output_hydro_table.to_csv(output_hydro_table_fileName,index=False)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-s','--input-srcbase', help='Base synthetic rating curve table', required=True)
    parser.add_argument('-w','--input-hydrotable',help='Input Hydro-Table',required=False)
    parser.add_argument('-f','--input-srcfull',help='Input Hydro-Table',required=False)
    parser.add_argument('-r','--output-srcfull', help='Output crosswalked synthetic rating curve table', required=False)
    parser.add_argument('-t','--output-hydrotable',help='Hydrotable',required=False)

    args = vars(parser.parse_args())

    finalize_srcs(**args)

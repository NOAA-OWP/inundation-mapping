#!/usr/bin/env python3

import os
import geopandas as gpd
import pandas as pd
import numpy as np

def bathy_rc_lookup(input_src_base,input_bathy_fileName,output_bathy_fileName,output_bathy_streamorder_fileName):
    ## Convert input_src_base featureid to integer
    if input_src_base.feature_id.dtype != 'int': input_src_base.feature_id = input_src_base.feature_id.astype(int)

    ## Calculate XS Area field (Channel Volume / Stream Length)
    input_src_base['XS Area (m2)'] = input_src_base['Volume (m3)'] / (input_src_base['LENGTHKM'] * 1000)

    ## Read in the bankfull channel geometry text file
    input_bathy = pd.read_csv(input_bathy_fileName, dtype= {'COMID': int})
    print(input_bathy.head)
    print(input_src_base.head)

    ## Perform merge using feature_id/COMID attributes
    input_bathy = input_bathy.rename(columns={'COMID':'feature_id','BANKFULL_WIDTH':'BANKFULL_WIDTH (m)','BANKFULL_XSEC_AREA':'BANKFULL_XSEC_AREA (m2)'})
    modified_src_base = input_src_base.merge(input_bathy.loc[:,['feature_id','BANKFULL_WIDTH (m)','BANKFULL_XSEC_AREA (m2)']],how='left',on='feature_id')
    print(modified_src_base.head)

    ## Calculate bankfull vs top width difference for each feature_id
    modified_src_base['Top Width Diff (m)'] = (modified_src_base['TopWidth (m)'] - modified_src_base['BANKFULL_WIDTH (m)'])

    ## Groupby HydroID and find min of Top Width Diff (m)
    output_bathy = modified_src_base[['feature_id','HydroID','order_','Stage','BANKFULL_WIDTH (m)','TopWidth (m)','XS Area (m2)','BANKFULL_XSEC_AREA (m2)','Top Width Diff (m)']]
    ## mask out stage = 0 rows in SRC
    output_bathy['Top Width Diff (m)'].mask(output_bathy['Stage']== 0,inplace=True)
    ## mask out negative top width differences (avoid thalweg burn notch)
    output_bathy['Top Width Diff (m)'].mask(output_bathy['Top Width Diff (m)'] < 0,inplace=True)
    ## find index of minimum top width difference --> this will be used as the SRC "bankfull" row for future calcs
    output_bathy = output_bathy.loc[output_bathy.groupby('HydroID')['Top Width Diff (m)'].idxmin()].reset_index(drop=True)
    print('Average: bankfull width crosswalk difference (m): ' + str(output_bathy['Top Width Diff (m)'].mean()))
    print('Minimum: bankfull width crosswalk difference (m): ' + str(output_bathy['Top Width Diff (m)'].min()))
    print('Maximum: bankfull width crosswalk difference (m): ' + str(output_bathy['Top Width Diff (m)'].max()))
    print('STD: bankfull width crosswalk difference (m): ' + str(output_bathy['Top Width Diff (m)'].std()) +'\n')

    ## Calculate XS Area difference between SRC and Bankfull database
    output_bathy['XS Area Diff (m2)'] = (output_bathy['BANKFULL_XSEC_AREA (m2)'] - output_bathy['XS Area (m2)'])
    output_bathy['XS Bankfull Area Ratio'] = (output_bathy['BANKFULL_XSEC_AREA (m2)'] / output_bathy['XS Area (m2)']).round(2)
    ## masking negative XS Area Diff and XS Area = 0
    output_bathy['XS Bankfull Area Ratio'].mask((output_bathy['XS Area Diff (m2)']<0) | (output_bathy['XS Area (m2)'] == 0),inplace=True)
    ## masking negative XS Area Diff and XS Area = 0
    output_bathy['XS Area Diff (m2)'].mask((output_bathy['XS Area Diff (m2)']<0) | (output_bathy['XS Area (m2)'] == 0),inplace=True)
    ## remove bogus values (crosswalk issues or bad bankfull data points)
    output_bathy['XS Area Diff (m2)'].mask(output_bathy['XS Bankfull Area Ratio']>10,inplace=True)
    ## remove bogus values (crosswalk issues or bad bankfull data points)
    output_bathy['XS Bankfull Area Ratio'].mask(output_bathy['XS Bankfull Area Ratio']>10,inplace=True)
    ## Print XS Area Diff statistics
    print('Average: bankfull XS Area crosswalk difference (m2): ' + str(output_bathy['XS Area Diff (m2)'].mean()))
    print('Minimum: bankfull XS Area crosswalk difference (m2): ' + str(output_bathy['XS Area Diff (m2)'].min()))
    print('Maximum: bankfull XS Area crosswalk difference (m2): ' + str(output_bathy['XS Area Diff (m2)'].max()))
    print('STD: bankfull XS Area crosswalk difference (m2): ' + str(output_bathy['XS Area Diff (m2)'].std()))

    ## Bin XS Bankfull Area Ratio by stream order
    stream_order_bathy_ratio = output_bathy[['order_','Stage','XS Bankfull Area Ratio']]
    ## mask stage values when XS Bankfull Area Ratio is null
    stream_order_bathy_ratio['Stage'].mask(stream_order_bathy_ratio['XS Bankfull Area Ratio'].isnull(),inplace=True)
    stream_order_bathy_ratio = stream_order_bathy_ratio.groupby('order_').agg(count=('XS Bankfull Area Ratio','count'),mean_xs_area_ratio=('XS Bankfull Area Ratio','mean'),median_stage_bankfull=('Stage','median'))
    ## fill XS Bankfull Area Ratio and Stage values if no values were found in the grouby calcs
    stream_order_bathy_ratio = (stream_order_bathy_ratio.ffill()+stream_order_bathy_ratio.bfill())/2
    ## fill first and last stream order values if needed
    stream_order_bathy_ratio = stream_order_bathy_ratio.bfill().ffill()
    print(stream_order_bathy_ratio.head)

    ## Combine SRC df and df of XS Area for each hydroid and matching stage and order from bins above
    output_bathy = output_bathy.merge(stream_order_bathy_ratio,how='left',on='order_')
    modified_src_base = modified_src_base.merge(stream_order_bathy_ratio,how='left',on='order_')

    ## Calculate stage vs median_stage_bankfull difference for bankfull lookup
    modified_src_base['lookup_stage_diff'] = (modified_src_base['median_stage_bankfull'] - modified_src_base['Stage']).abs()

    ## Groupby HydroID again and find min of lookup_stage_diff
    xs_area_hydroid_lookup = modified_src_base[['HydroID','XS Area (m2)','lookup_stage_diff','mean_xs_area_ratio']]
    xs_area_hydroid_lookup = xs_area_hydroid_lookup.loc[xs_area_hydroid_lookup.groupby('HydroID')['lookup_stage_diff'].idxmin()].reset_index(drop=True)

    ## Calculate bathy adjusted XS Area ('XS Area (m2)' mutliplied by mean_xs_area_ratio)
    xs_area_hydroid_lookup['bathy_calc_xs_area'] = (xs_area_hydroid_lookup['XS Area (m2)'] * xs_area_hydroid_lookup['mean_xs_area_ratio']) - xs_area_hydroid_lookup['XS Area (m2)']
    print(xs_area_hydroid_lookup.head)

    ## Merge bathy_calc_xs_area to the modified_src_base
    modified_src_base = modified_src_base.merge(xs_area_hydroid_lookup.loc[:,['HydroID','bathy_calc_xs_area']],how='left',on='HydroID')

    ## Calculate new bathy adjusted channel geometry variables
    modified_src_base = modified_src_base.rename(columns={'Discharge (m3s-1)':'Discharge (m3s-1)_nobathy'})
    modified_src_base['XS Area (m2)_bathy_adj'] = modified_src_base['XS Area (m2)'] + modified_src_base['bathy_calc_xs_area']
    modified_src_base['Volume (m3)_bathy_adj'] = modified_src_base['XS Area (m2)_bathy_adj'] * modified_src_base['LENGTHKM'] * 1000
    modified_src_base['WetArea (m2)_bathy_adj'] = modified_src_base['Volume (m3)_bathy_adj']/modified_src_base['LENGTHKM']/1000
    modified_src_base['HydraulicRadius (m)_bathy_adj'] = modified_src_base['WetArea (m2)_bathy_adj']/modified_src_base['WettedPerimeter (m)']
    modified_src_base['HydraulicRadius (m)_bathy_adj'].fillna(0, inplace=True)
    ## mask out negative top width differences (avoid thalweg burn notch)
    modified_src_base['HydraulicRadius (m)_bathy_adj'].mask((modified_src_base['HydraulicRadius (m)_bathy_adj']>10) & (modified_src_base['Stage']<5),inplace=True)
    ## backfill NA values created is previous step
    modified_src_base['HydraulicRadius (m)_bathy_adj'] = modified_src_base['HydraulicRadius (m)_bathy_adj'].bfill()
    ## Calculate Q using Manning's equation
    modified_src_base['Discharge (m3s-1)'] = modified_src_base['WetArea (m2)_bathy_adj']* \
    pow(modified_src_base['HydraulicRadius (m)_bathy_adj'],2.0/3)* \
    pow(modified_src_base['SLOPE'],0.5)/modified_src_base['ManningN']
    ## mask discharge values for stage = 0 rows in SRC (replace with 0) --> do we need SRC to start at 0??
    modified_src_base['Discharge (m3s-1)'].mask(modified_src_base['Stage']== 0,0,inplace=True)

    ## Organize bathy calc output variables for csv
    output_bathy = output_bathy[['HydroID','order_','Stage','TopWidth (m)','BANKFULL_WIDTH (m)','Top Width Diff (m)','XS Area (m2)','BANKFULL_XSEC_AREA (m2)','XS Area Diff (m2)','XS Bankfull Area Ratio','count','median_stage_bankfull','mean_xs_area_ratio']]

    ## Export bathy/bankful crosswalk table for easy viewing
    output_bathy.to_csv(output_bathy_fileName,index=False)
    stream_order_bathy_ratio.to_csv(output_bathy_streamorder_fileName,index=True)

    print('Completed Bathy Calculations...')
    return(modified_src_base)

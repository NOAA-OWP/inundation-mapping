#!/usr/bin/env python3

import os
import geopandas as gpd
import pandas as pd

def bathy_rc_lookup(input_src_base,input_bathy_fileName,output_bathy_fileName):
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
    match_comid = input_src_base.merge(input_bathy.loc[:,['feature_id','BANKFULL_WIDTH (m)','BANKFULL_XSEC_AREA (m2)']],how='left',on='feature_id')
    print(match_comid.head)

    ## Calculate bankfull vs top width difference for each feature_id
    match_comid['Top Width Diff (m)'] = (match_comid['TopWidth (m)'] - match_comid['BANKFULL_WIDTH (m)']).abs()

    ## Groupby HydroID and find min of Top Width Diff (m)
    find_closest_match = match_comid[['HydroID','Stage','BANKFULL_WIDTH (m)','TopWidth (m)','XS Area (m2)','BANKFULL_XSEC_AREA (m2)','Top Width Diff (m)']]
    find_closest_match = find_closest_match.loc[find_closest_match.groupby('HydroID')['Top Width Diff (m)'].idxmin()].reset_index(drop=True)
    print('Average: bankfull width crosswalk difference (m): ' + str(find_closest_match['Top Width Diff (m)'].mean()))
    print('Minimum: bankfull width crosswalk difference (m): ' + str(find_closest_match['Top Width Diff (m)'].min()))
    print('Maximum: bankfull width crosswalk difference (m): ' + str(find_closest_match['Top Width Diff (m)'].max()))
    print('STD: bankfull width crosswalk difference (m): ' + str(find_closest_match['Top Width Diff (m)'].std()))

    ## Calculate XS Area difference between SRC and Bankfull database match_comid
    find_closest_match['XS Area Diff (m2)'] = (find_closest_match['BANKFULL_XSEC_AREA (m2)'] - find_closest_match['XS Area (m2)'])
    find_closest_match = find_closest_match[['HydroID','Stage','TopWidth (m)','BANKFULL_WIDTH (m)','Top Width Diff (m)','XS Area (m2)','BANKFULL_XSEC_AREA (m2)','XS Area Diff (m2)']]

    ## Export bathy/bankful crosswalk table for easy viewing
    print(find_closest_match.head)
    find_closest_match.to_csv(output_bathy_fileName,index=False)

    print('Completed...')
    return(match_comid)

#!/usr/bin/env python3

from os import environ
import pandas as pd

sa_ratio_flag = float(environ['surf_area_thalweg_ratio_flag']) #10x
thal_stg_limit = float(environ['thalweg_stg_search_max_limit']) #3m
bankful_xs_ratio_flag = float(environ['bankful_xs_area_ratio_flag']) #10x
bathy_xsarea_flag = float(environ['bathy_xs_area_chg_flag']) #1x
thal_hyd_radius_flag = float(environ['thalweg_hyd_radius_flag']) #10x


def bathy_rc_lookup(input_src_base,input_bathy_fileName,output_bathy_fileName,output_bathy_streamorder_fileName,output_bathy_thalweg_fileName,output_bathy_xs_lookup_fileName,):
    ## Convert input_src_base featureid to integer
    if input_src_base.feature_id.dtype != 'int': input_src_base.feature_id = input_src_base.feature_id.astype(int)

    ## Read in the bankfull channel geometry text file
    input_bathy = pd.read_csv(input_bathy_fileName, dtype= {'COMID': int})

    ## Merge input_bathy and modified_src_base df using feature_id/COMID attributes
    input_bathy = input_bathy.rename(columns={'COMID':'feature_id','BANKFULL_WIDTH':'BANKFULL_WIDTH (m)','BANKFULL_XSEC_AREA':'BANKFULL_XSEC_AREA (m2)'})
    modified_src_base = input_src_base.merge(input_bathy.loc[:,['feature_id','BANKFULL_WIDTH (m)','BANKFULL_XSEC_AREA (m2)']],how='left',on='feature_id')

    ## Check that the merge process returned matching feature_id entries
    if modified_src_base['BANKFULL_WIDTH (m)'].count() == 0:
        print('No matching feature_id found between input bathy data and src_base --> No bathy calculations added to SRC!')
        return(input_src_base)
    else:
        ## Use SurfaceArea variable to identify thalweg-restricted stage values for each hydroid
        ## Calculate the interrow SurfaceArea ratio n/(n-1)
        modified_src_base['SA_div'] = modified_src_base['SurfaceArea (m2)'].div(modified_src_base['SurfaceArea (m2)'].shift(1))
        ## Mask SA_div when Stage = 0 or when the SA_div value (n / n-1) is > threshold value (i.e. 10x)
        modified_src_base['SA_div'].mask((modified_src_base['Stage']==0) | (modified_src_base['SA_div']<sa_ratio_flag) | (modified_src_base['SurfaceArea (m2)']==0),inplace=True)
        ## Create new df to filter and groupby HydroID
        find_thalweg_notch = modified_src_base[['HydroID','Stage','SurfaceArea (m2)','SA_div']]
        find_thalweg_notch = find_thalweg_notch[find_thalweg_notch['Stage']<thal_stg_limit] # assuming thalweg burn-in is less than 3 meters
        find_thalweg_notch = find_thalweg_notch[find_thalweg_notch['SA_div'].notnull()]
        find_thalweg_notch = find_thalweg_notch.loc[find_thalweg_notch.groupby('HydroID')['Stage'].idxmax()].reset_index(drop=True)
        ## Assign thalweg_burn_elev variable to the stage value found in previous step
        find_thalweg_notch['Thalweg_burn_elev'] = find_thalweg_notch['Stage']
        ## Merge the Thalweg_burn_elev value back into the modified SRC --> this is used to mask the discharge after Manning's equation
        modified_src_base = modified_src_base.merge(find_thalweg_notch.loc[:,['HydroID','Thalweg_burn_elev']],how='left',on='HydroID')

        ## Calculate bankfull vs top width difference for each feature_id
        modified_src_base['Top Width Diff (m)'] = (modified_src_base['TopWidth (m)'] - modified_src_base['BANKFULL_WIDTH (m)']).abs()
        ## Calculate XS Area field (Channel Volume / Stream Length)
        modified_src_base['XS Area (m2)'] = modified_src_base['Volume (m3)'] / (modified_src_base['LENGTHKM'] * 1000)

        ## Groupby HydroID and find min of Top Width Diff (m)
        output_bathy = modified_src_base[['feature_id','HydroID','order_','Stage','SurfaceArea (m2)','Thalweg_burn_elev','BANKFULL_WIDTH (m)','TopWidth (m)','XS Area (m2)','BANKFULL_XSEC_AREA (m2)','Top Width Diff (m)']]
        ## filter out stage = 0 rows in SRC (assuming geom at stage 0 is not a valid channel geom)
        output_bathy = output_bathy[output_bathy['Stage'] > 0]
        ## filter SRC rows identified as Thalweg burned
        output_bathy['Top Width Diff (m)'].mask(output_bathy['Stage'] <= output_bathy['Thalweg_burn_elev'],inplace=True)
        ## ignore hydroid/featureid that did not have a valid Bankfull lookup (areas outside CONUS - i.e. Canada)
        output_bathy = output_bathy[output_bathy['BANKFULL_XSEC_AREA (m2)'].notnull()]
        ## ignore SRC entries with 0 surface area --> handles input SRC artifacts/errors in Great Lakes region
        output_bathy = output_bathy[output_bathy['SurfaceArea (m2)'] > 0]
        ## find index of minimum top width difference --> this will be used as the SRC "bankfull" row for future calcs
        output_bathy = output_bathy.loc[output_bathy.groupby('HydroID')['Top Width Diff (m)'].idxmin()].reset_index(drop=True)
        print('Average: bankfull width crosswalk difference (m): ' + str(output_bathy['Top Width Diff (m)'].mean()))
        print('Minimum: bankfull width crosswalk difference (m): ' + str(output_bathy['Top Width Diff (m)'].min()))
        print('Maximum: bankfull width crosswalk difference (m): ' + str(output_bathy['Top Width Diff (m)'].max()))
        print('STD: bankfull width crosswalk difference (m): ' + str(output_bathy['Top Width Diff (m)'].std()) +'\n' + '#################')

        ## Calculate XS Area difference between SRC and Bankfull database
        output_bathy['XS Area Diff (m2)'] = (output_bathy['BANKFULL_XSEC_AREA (m2)'] - output_bathy['XS Area (m2)'])
        output_bathy['XS Bankfull Area Ratio'] = (output_bathy['BANKFULL_XSEC_AREA (m2)'] / output_bathy['XS Area (m2)']).round(2)
        ## masking negative XS Area Diff and XS Area = 0
        output_bathy['XS Bankfull Area Ratio'].mask((output_bathy['XS Area Diff (m2)']<0) | (output_bathy['XS Area (m2)'] == 0),inplace=True)
        ## masking negative XS Area Diff and XS Area = 0
        output_bathy['XS Area Diff (m2)'].mask((output_bathy['XS Area Diff (m2)']<0) | (output_bathy['XS Area (m2)'] == 0),inplace=True)
        ## remove bogus values where bankfull area ratio > threshold --> 10x (topwidth crosswalk issues or bad bankfull regression data points??)
        output_bathy['XS Area Diff (m2)'].mask(output_bathy['XS Bankfull Area Ratio']>bankful_xs_ratio_flag,inplace=True)
        ## remove bogus values where bankfull area ratio > threshold --> 10x (topwidth crosswalk issues or bad bankfull regression data points??)
        output_bathy['XS Bankfull Area Ratio'].mask(output_bathy['XS Bankfull Area Ratio']>bankful_xs_ratio_flag,inplace=True)
        ## Print XS Area Diff statistics
        print('Average: bankfull XS Area crosswalk difference (m2): ' + str(output_bathy['XS Area Diff (m2)'].mean()))
        print('Minimum: bankfull XS Area crosswalk difference (m2): ' + str(output_bathy['XS Area Diff (m2)'].min()))
        print('Maximum: bankfull XS Area crosswalk difference (m2): ' + str(output_bathy['XS Area Diff (m2)'].max()))
        print('STD: bankfull XS Area crosswalk difference (m2): ' + str(output_bathy['XS Area Diff (m2)'].std()))

        ## Bin XS Bankfull Area Ratio by stream order
        stream_order_bathy_ratio = output_bathy[['order_','Stage','XS Bankfull Area Ratio']].copy()
        ## mask stage values when XS Bankfull Area Ratio is null (need to filter to calculate the median for valid values below)
        stream_order_bathy_ratio['Stage'].mask(stream_order_bathy_ratio['XS Bankfull Area Ratio'].isnull(),inplace=True)
        stream_order_bathy_ratio = stream_order_bathy_ratio.groupby('order_').agg(count=('XS Bankfull Area Ratio','count'),mean_xs_area_ratio=('XS Bankfull Area Ratio','mean'),median_stage_bankfull=('Stage','median'))
        ## fill XS Bankfull Area Ratio and Stage values if no values were found in the grouby calcs
        stream_order_bathy_ratio = (stream_order_bathy_ratio.ffill()+stream_order_bathy_ratio.bfill())/2
        ## fill first and last stream order values if needed
        stream_order_bathy_ratio = stream_order_bathy_ratio.bfill().ffill()
        ## Get count_total tally of the total number of stream order hydroids in the HUC (not filtering anything out)
        stream_order_bathy_ratio_count = output_bathy.groupby('order_').agg(count_total=('Stage','count'))
        stream_order_bathy_ratio = stream_order_bathy_ratio.merge(stream_order_bathy_ratio_count,how='left',on='order_')
        ## Fill any remaining null values: mean_xs_area_ratio --> 1 median_stage_bankfull --> 0
        stream_order_bathy_ratio['mean_xs_area_ratio'].mask(stream_order_bathy_ratio['mean_xs_area_ratio'].isnull(),1,inplace=True)
        stream_order_bathy_ratio['median_stage_bankfull'].mask(stream_order_bathy_ratio['median_stage_bankfull'].isnull(),0,inplace=True)

        ## Combine SRC df and df of XS Area for each hydroid and matching stage and order from bins above
        output_bathy = output_bathy.merge(stream_order_bathy_ratio,how='left',on='order_')
        modified_src_base = modified_src_base.merge(stream_order_bathy_ratio,how='left',on='order_')

        ## Calculate stage vs median_stage_bankfull difference for bankfull lookup
        modified_src_base['lookup_stage_diff'] = (modified_src_base[['median_stage_bankfull','Thalweg_burn_elev']].max(axis=1) - modified_src_base['Stage']).abs()

        ## If median_stage_bankfull is null then set lookup_stage_diff to 999 at stage 0 (handles errors for channels outside CONUS)
        modified_src_base['lookup_stage_diff'].mask((modified_src_base['Stage'] == 0) & (modified_src_base['median_stage_bankfull'].isnull()),999,inplace=True)

        ## Groupby HydroID again and find min of lookup_stage_diff
        xs_area_hydroid_lookup = modified_src_base[['HydroID','BANKFULL_XSEC_AREA (m2)','XS Area (m2)','Stage','Thalweg_burn_elev','median_stage_bankfull','lookup_stage_diff','mean_xs_area_ratio']]
        xs_area_hydroid_lookup = xs_area_hydroid_lookup.loc[xs_area_hydroid_lookup.groupby('HydroID')['lookup_stage_diff'].idxmin()].reset_index(drop=True)

        ## Calculate bathy adjusted XS Area ('XS Area (m2)' mutliplied by mean_xs_area_ratio)
        xs_area_hydroid_lookup['bathy_calc_xs_area'] = (xs_area_hydroid_lookup['XS Area (m2)'] * xs_area_hydroid_lookup['mean_xs_area_ratio']) - xs_area_hydroid_lookup['XS Area (m2)']

        ## Calculate the ratio btw the lookup SRC XS_Area and the Bankfull_XSEC_AREA --> use this as a flag for potentially bad XS data
        xs_area_hydroid_lookup['bankfull_XS_ratio_flag'] = (xs_area_hydroid_lookup['bathy_calc_xs_area'] / xs_area_hydroid_lookup['BANKFULL_XSEC_AREA (m2)'])
        ## Set bath_cal_xs_area to 0 if the bankfull_XS_ratio_flag is > threshold --> 5x (assuming too large of difference to be a reliable bankfull calculation)
        xs_area_hydroid_lookup['bathy_calc_xs_area'].mask(xs_area_hydroid_lookup['bankfull_XS_ratio_flag']>bathy_xsarea_flag,xs_area_hydroid_lookup['BANKFULL_XSEC_AREA (m2)'],inplace=True)
        xs_area_hydroid_lookup['bathy_calc_xs_area'].mask(xs_area_hydroid_lookup['bankfull_XS_ratio_flag'].isnull(),0,inplace=True)

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
        modified_src_base['HydraulicRadius (m)_bathy_adj'].mask((modified_src_base['HydraulicRadius (m)_bathy_adj']>thal_hyd_radius_flag) & (modified_src_base['Stage']<thal_stg_limit),0,inplace=True)

        ## Calculate Q using Manning's equation
        modified_src_base['Discharge (m3s-1)'] = modified_src_base['WetArea (m2)_bathy_adj']* \
        pow(modified_src_base['HydraulicRadius (m)_bathy_adj'],2.0/3)* \
        pow(modified_src_base['SLOPE'],0.5)/modified_src_base['ManningN']
        ## mask discharge values for stage = 0 rows in SRC (replace with 0) --> do we need SRC to start at 0??
        modified_src_base['Discharge (m3s-1)'].mask(modified_src_base['Stage'] == 0,0,inplace=True)
        modified_src_base['Discharge (m3s-1)'].mask(modified_src_base['Stage'] == modified_src_base['Thalweg_burn_elev'],0,inplace=True)
        modified_src_base['Discharge (m3s-1)'].mask(modified_src_base['Stage'] < modified_src_base['Thalweg_burn_elev'],-999,inplace=True)

        ## Organize bathy calc output variables for csv
        output_bathy = output_bathy[['HydroID','order_','Stage','SurfaceArea (m2)','TopWidth (m)','BANKFULL_WIDTH (m)','Top Width Diff (m)','XS Area (m2)','BANKFULL_XSEC_AREA (m2)','XS Area Diff (m2)','XS Bankfull Area Ratio','count','median_stage_bankfull','mean_xs_area_ratio']]

        ## Export bathy/bankful calculation tables for easy viewing
        output_bathy.to_csv(output_bathy_fileName,index=False)
        stream_order_bathy_ratio.to_csv(output_bathy_streamorder_fileName,index=True)
        find_thalweg_notch.to_csv(output_bathy_thalweg_fileName,index=True)
        xs_area_hydroid_lookup.to_csv(output_bathy_xs_lookup_fileName,index=True)

        print('Completed Bathy Calculations...')
        return(modified_src_base)

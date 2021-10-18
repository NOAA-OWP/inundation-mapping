#!/usr/bin/env python3

import sys
import os
import pandas as pd
import argparse
from multiprocessing import Pool
from os.path import isfile, join, dirname, isdir
import matplotlib.pyplot as plt
import seaborn as sns

sa_ratio_flag = 10 #float(environ['surf_area_thalweg_ratio_flag']) #10x --> Flag: Surface area ratio value to identify possible thalweg notch "jump" (SA x+1 / SA x)
thal_stg_limit = 3 #float(environ['thalweg_stg_search_max_limit']) #3m --> Threshold: Stage value limit below which to look for the surface area ratio flag (only flag thalweg notch below this threshold)
bankful_xs_ratio_flag = 10 #float(environ['bankful_xs_area_ratio_flag']) #10x --> Flag: Identify bogus BARC adjusted values where the regression bankfull XS Area/SRC bankfull area is > threshold (topwidth crosswalk issues or bad bankfull regression data points??)
bathy_xsarea_flag = 1 #float(environ['bathy_xs_area_chg_flag']) #1x --> Flag: Cross section area limit to cap the amount of bathy XS area added to the SRC. Limits the bathy_calc_xs_area/ BANKFULL_XSEC_AREA to the specified threshold
thal_hyd_radius_flag = 10 #float(environ['thalweg_hyd_radius_flag']) #10x --> Flag: Idenitify possible erroneous BARC-adjusted hydraulic radius values. BARC discharge values greater than the specified threshold and within the thal_stg_limit are set to 0
ignore_streamorder = 10 #int(environ['ignore_streamorders']) #10 --> Do not perform BARC for streamorders >= provided value


def bathy_rc_lookup(args):
    input_src_fileName                  = args[0]
    df_bfull_geom                       = args[1]
    df_nwm1_5                           = args[2]
    output_bathy_fileName               = args[3]
    output_bathy_streamorder_fileName   = args[4]
    output_bathy_thalweg_fileName       = args[5]
    output_bathy_xs_lookup_fileName     = args[6]
    input_htable_fileName               = args[7]
    output_htable_fileName              = args[8]
    out_src_bankfull_filename           = args[9]
    src_plot_option                     = args[10]
    huc_output_dir                      = args[11]

    print(input_src_fileName)
    print(output_bathy_fileName)
    print(output_bathy_streamorder_fileName)
    print(output_bathy_thalweg_fileName)
    print(output_bathy_xs_lookup_fileName)
    print(input_htable_fileName)
    print(output_htable_fileName)

    ## Read in the default src_full_crosswalked.csv
    input_src_base = pd.read_csv(input_src_fileName, dtype= {'feature_id': int})

    ## NWM recurr rename discharge var
    df_nwm1_5 = df_nwm1_5.rename(columns={'discharge':'discharge_1_5'})

    ## Convert input_src_base featureid to integer
    #if input_src_base.feature_id.dtype != 'int': input_src_base.feature_id = input_src_base.feature_id.astype(int)

    ## Read in the bankfull channel geometry text file
    #input_bathy = pd.read_csv(input_bathy_fileName, dtype= {'COMID': int})

    ## Merge input_bathy and modified_src_base df using feature_id/COMID attributes
    df_bfull_geom = df_bfull_geom.rename(columns={'BANKFULL_XSEC_AREA_q':'BANKFULL_XSEC_AREA (m2)'})
    print(df_bfull_geom)
    modified_src_base = input_src_base.merge(df_bfull_geom,how='left',on='feature_id')
    modified_src_base = modified_src_base.merge(df_nwm1_5,how='left',on='feature_id')

    ## Check that the merge process returned matching feature_id entries
    if modified_src_base['BANKFULL_XSEC_AREA (m2)'].count() == 0:
        print('No matching feature_id found between input bathy data and src_base --> No bathy calculations added to SRC!')
    else:
        ## Use SurfaceArea variable to identify thalweg-restricted stage values for each hydroid
        ## Calculate the interrow SurfaceArea ratio n/(n-1)
        modified_src_base['SA_div_flag'] = modified_src_base['SurfaceArea (m2)'].div(modified_src_base['SurfaceArea (m2)'].shift(1))
        ## Mask SA_div_flag when Stage = 0 or when the SA_div_flag value (n / n-1) is > threshold value (i.e. 10x)
        modified_src_base['SA_div_flag'].mask((modified_src_base['Stage']==0) | (modified_src_base['SA_div_flag']<sa_ratio_flag) | (modified_src_base['SurfaceArea (m2)']==0),inplace=True)
        ## Create new df to filter and groupby HydroID
        find_thalweg_notch = modified_src_base[['HydroID','Stage','SurfaceArea (m2)','SA_div_flag']]
        find_thalweg_notch = find_thalweg_notch[find_thalweg_notch['Stage']<thal_stg_limit] # assuming thalweg burn-in is less than 3 meters
        find_thalweg_notch = find_thalweg_notch[find_thalweg_notch['SA_div_flag'].notnull()]
        find_thalweg_notch = find_thalweg_notch.loc[find_thalweg_notch.groupby('HydroID')['Stage'].idxmax()].reset_index(drop=True)
        ## Assign thalweg_burn_elev variable to the stage value found in previous step
        find_thalweg_notch['Thalweg_burn_elev'] = find_thalweg_notch['Stage']
        ## Merge the Thalweg_burn_elev value back into the modified SRC --> this is used to mask the discharge after Manning's equation
        modified_src_base = modified_src_base.merge(find_thalweg_notch.loc[:,['HydroID','Thalweg_burn_elev']],how='left',on='HydroID')

        ## Groupby HydroID and find min of Top Width Diff (m)
        #output_bathy = modified_src_base[['feature_id','HydroID','order_','Stage','SurfaceArea (m2)','Thalweg_burn_elev','XS Area (m2)','BANKFULL_XSEC_AREA (m2)','Top Width Diff (m)']]
        modified_src_base['HRadius_bankfull'] = (modified_src_base['discharge_1_5_cms']*modified_src_base['ManningN']) / (pow(modified_src_base['SLOPE'],0.5)*modified_src_base['BANKFULL_XSEC_AREA (m2)'])
        modified_src_base['HRadius_bankfull'] = pow(modified_src_base['HRadius_bankfull'],3.0/2)
        modified_src_base['WettedPerimeter_bankfull'] = modified_src_base['BANKFULL_XSEC_AREA (m2)']/modified_src_base['HRadius_bankfull']

        print('Completed Bathy Calculations...')
        modified_src_base.to_csv(out_src_bankfull_filename,index=False)
        # make hydroTable
        # output_hydro_table = modified_src_base.loc[:,['HydroID','Stage','Discharge (m3s-1)']]
        # output_hydro_table.rename(columns={'Stage' : 'stage','Discharge (m3s-1)':'discharge_cms'},inplace=True)
        # df_htable = pd.read_csv(input_htable_fileName,dtype={'HUC': str})
        # df_htable.drop(['discharge_cms'], axis=1, inplace=True) # drop the original discharge column to be replaced with updated version
        # df_htable = df_htable.merge(output_hydro_table, how='left', left_on=['HydroID','stage'], right_on=['HydroID','stage'])
        # df_htable.to_csv(output_htable_fileName,index=False)
        print('Output new hydroTable and src_full_crosswalked!')

        ## plot rating curves
        if src_plot_option == 'True':
            if isdir(huc_output_dir) == False:
                os.mkdir(huc_output_dir)
            generate_src_plot(modified_src_base, huc_output_dir)


def generate_src_plot(df_src, plt_out_dir):

    ## create list of unique hydroids
    hydroids = df_src.HydroID.unique().tolist()

    ## plot each hydroid SRC in the huc
    for hydroid in hydroids:
        print("Creating SRC plot: " + str(hydroid))
        plot_df = df_src.loc[df_src['HydroID'] == hydroid]

        f, ax = plt.subplots(figsize=(6.5, 6.5))
        ax.set_title(str(hydroid))
        sns.despine(f, left=True, bottom=True)
        sns.scatterplot(x='orig_Discharge (m3s-1)', y='Stage', data=plot_df, label="Orig SRC", ax=ax, color='blue')
        sns.scatterplot(x='Discharge (m3s-1)', y='Stage', data=plot_df, label="SRC w/ BARC", ax=ax, color='orange')
        #sns.lineplot(x='discharge_1_5', y='Stage_1_5', data=plot_df, color='green', ax=ax)
        #plt.fill_between(plot_df['discharge_1_5'], plot_df['Stage_1_5'],alpha=0.5)
        #plt.text(plot_df['discharge_1_5'].median(), plot_df['Stage_1_5'].median(), "NWM 1.5yr: " + str(plot_df['Stage_1_5'].median()))
        ax.legend()
        plt.savefig(plt_out_dir + os.sep + str(hydroid) + '_BARC.png',dpi=175, bbox_inches='tight')
        plt.close()

if __name__ == '__main__':
    #output_src,input_bathy_fileName,output_bathy_fileName,output_bathy_streamorder_fileName,output_bathy_thalweg_fileName,output_bathy_xs_lookup_fileName
    parser = argparse.ArgumentParser(description="Estimate the unaccounted for channel bathymetry using a regression-based estimate of channel XSec Area")
    parser.add_argument('-fim_dir','--fim-dir', help='FIM output dir', required=True,type=str)
    parser.add_argument('-bflows','--bankfull-flow-input',help='NWM recurrence flows dir',required=True,type=str)
    parser.add_argument('-bfull','--bankfull-xsec-input',help='Regression dataset w/ bankfull geometry by featureid',required=True,type=str)
    parser.add_argument('-suff','--hydrotable-suffix',help="Suffix to append to the new hydroTable csv file (e.g. '_BARC')",required=True,type=str)
    parser.add_argument('-j','--number-of-jobs',help='number of workers',required=False,default=1,type=int)
    parser.add_argument('-plots','--src-plot-option',help='Optional (True or False): use this flag to create src plots for all hydroids. WARNING - long runtime',required=False,default='False',type=str)

    args = vars(parser.parse_args())

    fim_dir = args['fim_dir']
    bankfull_flow_filepath = args['bankfull_flow_input']
    bankfull_regres_filepath = args['bankfull_xsec_input']
    hydrotable_suffix = args['hydrotable_suffix']
    number_of_jobs = args['number_of_jobs']
    src_plot_option = args['src_plot_option']
    procs_list = []

    if not isfile(bankfull_flow_filepath):
        print('!!! Can not find the input recurr flow file: ' + str(bankfull_flow_filepath))
    else:
        df_nwm1_5 = pd.read_csv(bankfull_flow_filepath,dtype={'feature_id': int})
        ## Check that the bankfull flow filepath exists and read to dataframe
        if not isfile(bankfull_regres_filepath):
            print('!!! Can not find the input bankfull geometry regression file: ' + str(bankfull_flow_filepath))
        else:
            ## Read the Manning's n csv (ensure that it contains feature_id, channel mannings, floodplain mannings)
            print('Importing the bankfull regression data file: ' + bankfull_flow_filepath)
            df_bfull_geom = pd.read_csv(bankfull_regres_filepath,dtype= {'COMID': int})
            if 'COMID' not in df_bfull_geom.columns and 'feature_id' not in df_bfull_geom.columns:
                print('Missing required data column ("feature_id" or "COMID")!!! --> ' + df_bfull_geom)
            else:
                print('Running the rating curve bathy estimation function...')

                ## Loop through hucs in the fim_dir and create list of variables to feed to multiprocessing
                huc_list  = os.listdir(fim_dir)
                for huc in huc_list:
                    if huc != 'logs' and huc[-3:] != 'log' and huc[-4:] != '.csv':
                        #output_src,input_bathy_fileName,output_bathy_fileName,output_bathy_streamorder_fileName,output_bathy_thalweg_fileName,output_bathy_xs_lookup_fileName
                        in_src_bankfull_filename = join(fim_dir,huc,'src_full_crosswalked.csv')
                        out_src_bankfull_filename = join(fim_dir,huc,'src_full_crosswalked_BARC.csv')
                        htable_filename = join(fim_dir,huc,'hydroTable.csv')
                        new_htable_filename = join(fim_dir,huc,'hydroTable' + hydrotable_suffix + '.csv')
                        output_bath_filename = join(fim_dir,huc,'bathy_crosswalk_calcs.csv')
                        output_bathy_thalweg_fileName = join(fim_dir,huc,'bathy_thalweg_flag.csv')
                        output_bathy_streamorder_fileName = join(fim_dir,huc,'bathy_stream_order_calcs.csv')
                        output_bathy_thalweg_fileName = join(fim_dir,huc,'bathy_thalweg_flag.csv')
                        output_bathy_xs_lookup_fileName = join(fim_dir,huc,'bathy_xs_area_hydroid_lookup.csv')
                        huc_plot_output_dir = join(fim_dir,huc,'src_plots')

                        if isfile(in_src_bankfull_filename):
                            print(str(huc))
                            procs_list.append([in_src_bankfull_filename,df_bfull_geom,df_nwm1_5,output_bath_filename,output_bathy_streamorder_fileName,output_bathy_thalweg_fileName,output_bathy_xs_lookup_fileName,htable_filename,new_htable_filename,out_src_bankfull_filename,src_plot_option,huc_plot_output_dir])
                        else:
                            print(str(huc) + ' --> can not find the src_full_crosswalked_bankfull.csv in the fim output dir: ' + str(join(fim_dir,huc)))

                ## Initiate multiprocessing
                print(f"Applying bathy adjustment calcs for {len(procs_list)} hucs using {number_of_jobs} jobs")
                with Pool(processes=number_of_jobs) as pool:
                    pool.map(bathy_rc_lookup, procs_list)

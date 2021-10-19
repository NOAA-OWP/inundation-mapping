#!/usr/bin/env python3

import pandas as pd
import numpy as np
from glob import iglob, glob
from os.path import join, exists
import argparse
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec


def join_srcs(fr_data_dir,ms_data_dir,gms_data_dir,outputs_dir=None):

    gms_srcs_file_list_generator = iglob( join(gms_data_dir,"*","branches","*","src_full_*.csv"), recursive=True)
    
    """
    gms_srcs = None
    for i in gms_srcs_file_list_generator:
        current_gms_src = pd.read_csv(i)

        if gms_srcs is None:
            gms_srcs = current_gms_src
        else:
            gms_srcs = pd.concat((gms_srcs,current_gms_src),ignore_index=True)
    """

    gms_processed_data_file_name = join(outputs_dir,'gms_mean_srcs_ble.h5')
    ms_processed_data_file_name = join(outputs_dir,'ms_mean_srcs_ble.h5')
    fr_processed_data_file_name = join(outputs_dir,'fr_mean_srcs_ble.h5')

    if exists(gms_processed_data_file_name):
        gms_srcs = pd.read_hdf(gms_processed_data_file_name)
    else:
        # GMS srcs
        print("Loading GMS SRCs")
        gms_srcs = pd.concat( [ pd.read_csv(i) for i in gms_srcs_file_list_generator] , ignore_index=True)
        gms_srcs.loc[:,'Number of Cells'] = gms_srcs.loc[:,'Number of Cells'].astype(int)
        gms_srcs = gms_srcs.loc[:,['Stage','Discharge (m3s-1)','Volume (m3)','BedArea (m2)','Number of Cells']].groupby('Stage').agg('mean').reset_index(drop=False)

   
    if exists(fr_processed_data_file_name):
        fr_srcs = pd.read_hdf(fr_processed_data_file_name)
    else:
        # fr srcsi
        fr_srcs_file_name = glob( join(fr_data_dir,'*',"src_full_crosswalked.csv"))[0]
        fr_srcs = pd.read_csv( fr_srcs_file_name )

        fr_srcs_file_names = glob( join(fr_data_dir,'*',"src_full_crosswalked.csv"))
        print("Loading FR SRCs")
        fr_srcs = pd.concat( [ pd.read_csv(i) for i in fr_srcs_file_names ] , ignore_index = True )
        # average out values by stage
        fr_srcs = fr_srcs.loc[:,['Stage','Discharge (m3s-1)','Volume (m3)','BedArea (m2)','Number of Cells']].groupby('Stage').agg('mean').reset_index(drop=False)

    # gms
    if exists(ms_processed_data_file_name):
        ms_srcs = pd.read_hdf(ms_processed_data_file_name)
    else:
        ms_src_file_names = glob( join(ms_data_dir,'*',"src_full_crosswalked.csv"))
        print("Loading MS SRCs")
        ms_srcs = pd.concat( [ pd.read_csv(i) for i in ms_src_file_names ] , ignore_index = True )

        ms_srcs = pd.concat( (ms_srcs,fr_srcs), ignore_index=True )
    
        ms_srcs = ms_srcs.loc[:,['Stage','Discharge (m3s-1)','Volume (m3)','BedArea (m2)','Number of Cells']].groupby('Stage').agg('mean').reset_index(drop=False)
    
    # save files
    if not exists(gms_processed_data_file_name):
        gms_srcs.to_hdf(gms_processed_data_file_name,key='data')

    if not exists(ms_processed_data_file_name):
        ms_srcs.to_hdf(ms_processed_data_file_name,key='data')

    if not exists(fr_processed_data_file_name):
        fr_srcs.to_hdf(fr_processed_data_file_name,key='data')

    # figure
    fig = plt.figure(constrained_layout=True,figsize=(6,7))
    gs = GridSpec(3,2,figure=fig)

    # SRC plots
    ax1 = fig.add_subplot(gs[0,:])
    ax1.plot(gms_srcs.loc[:,'Discharge (m3s-1)'],gms_srcs.loc[:,'Stage'],label='GMS Mean')
    ax1.plot(ms_srcs.loc[:,'Discharge (m3s-1)'],ms_srcs.loc[:,'Stage'],label='MS Mean')
    ax1.plot(fr_srcs.loc[:,'Discharge (m3s-1)'],fr_srcs.loc[:,'Stage'],label="FR Mean")
    ax1.set_xlabel(r'Discharge ($m^{3}/s$)'+'\n'+'(a)')
    ax1.set_ylabel('Stage, m')
    ax1.ticklabel_format(axis='x',style='sci',useMathText=True,scilimits=(0,4))
    ax1.legend(loc='center',bbox_to_anchor=(0.5,1.25,0,0),ncol=3)

    #ax1.legend()
    #if outputs_dir is not None:
    #    ax1.savefig( join(outputs_dir,"rating_curve.jpg") )
    
    # Volume plots
    ax2 = fig.add_subplot(gs[1,0])
    ax2.plot(gms_srcs.loc[:,'Stage'],gms_srcs.loc[:,'Volume (m3)'],label='GMS Mean')
    ax2.plot(ms_srcs.loc[:,'Stage'],ms_srcs.loc[:,'Volume (m3)'],label='MS Mean')
    ax2.plot(fr_srcs.loc[:,'Stage'],fr_srcs.loc[:,'Volume (m3)'],label="FR Mean")
    ax2.set_xlabel('Stage, m'+'\n'+'(b)')
    ax2.set_ylabel(r'Volume, $m^3$')
    #ax2.legend()
    #if outputs_dir is not None:
    #    plt.savefig( join(outputs_dir,"volume.jpg") )
    
    # Bed Area plots
    ax3 = fig.add_subplot(gs[1,1])
    ax3.plot(gms_srcs.loc[:,'Stage'],gms_srcs.loc[:,'BedArea (m2)'],label='GMS Mean')
    ax3.plot(ms_srcs.loc[:,'Stage'],ms_srcs.loc[:,'BedArea (m2)'],label='MS Mean')
    ax3.plot(fr_srcs.loc[:,'Stage'],fr_srcs.loc[:,'BedArea (m2)'],label="FR Mean")
    ax3.set_xlabel('Stage, m'+'\n'+'(c)')
    ax3.set_ylabel(r'Bed Area, $m^2$')
    #ax3.legend()
    #if outputs_dir is not None:
    #    plt.savefig( join(outputs_dir,"bedarea.jpg") )
    
    # Volume and Bed Area plots
    ax4 = fig.add_subplot(gs[2,0])
    ax4.plot(gms_srcs.loc[:,'Stage'],
             np.power(gms_srcs.loc[:,'Volume (m3)'],5/3) * np.power(gms_srcs.loc[:,'BedArea (m2)'],-2/3),
             label='GMS Mean')
    ax4.plot(ms_srcs.loc[:,'Stage'],
             np.power(ms_srcs.loc[:,'Volume (m3)'],5/3) * np.power(ms_srcs.loc[:,'BedArea (m2)'],-2/3),
             label='MS Mean')
    ax4.plot(fr_srcs.loc[:,'Stage'],
             np.power(fr_srcs.loc[:,'Volume (m3)'],5/3) * np.power(fr_srcs.loc[:,'BedArea (m2)'],-2/3),
             label="FR Mean")
    ax4.set_xlabel('Stage, m'+'\n'+'(d)')
    ax4.set_ylabel(r'$\dfrac{(Volume,\ m^3)^{5/3} }{(Bed\ Area,\ m^2) ^{2/3} }$')
    #ax4.legend()
    #if outputs_dir is not None:
    #    plt.savefig( join(outputs_dir,"volume_and_bedarea.jpg") )

    # Number of Cells plots
    ax5 = fig.add_subplot(gs[2,1])
    ax5.plot(gms_srcs.loc[:,'Stage'],gms_srcs.loc[:,'Number of Cells'],label='GMS Mean')
    ax5.plot(ms_srcs.loc[:,'Stage'],ms_srcs.loc[:,'Number of Cells'],label='MS Mean')
    ax5.plot(fr_srcs.loc[:,'Stage'],fr_srcs.loc[:,'Number of Cells'],label="FR Mean")
    ax5.set_xlabel('Stage, m'+'\n'+'(e)')
    ax5.set_ylabel('Pixels')
    ax5.ticklabel_format(axis='y',style='sci',useMathText=True,scilimits=(0,3))
    
    if outputs_dir is not None:
        fig.savefig( join(outputs_dir,"rating_curve_comparison.jpg"), dpi=300)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Comparing SRCs')
    parser.add_argument('-f','--fr-data-dir', help='Directory with FR data', required=True)
    parser.add_argument('-g','--gms-data-dir', help='Directory with GMS data', required=True)
    parser.add_argument('-m','--ms-data-dir', help='Directory with MS data', required=True)
    parser.add_argument('-o','--outputs-dir', help='Directory for outputs data', required=False,default=None)

    args = vars(parser.parse_args())

    join_srcs(**args)

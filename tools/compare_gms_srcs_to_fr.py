#!/usr/bin/env python3

import pandas as pd
import numpy as np
from glob import iglob, glob
from os.path import join
import argparse
import matplotlib.pyplot as plt

def join_srcs(fr_data_dir,outputs_dir=None):

    gms_srcs = None
    gms_srcs_file_list_generator = iglob( join(fr_data_dir,"*","gms","*","src_full_*.csv"), recursive=True)
    
    for i in gms_srcs_file_list_generator:
        current_gms_src = pd.read_csv(i)

        if gms_srcs is None:
            gms_srcs = current_gms_src
        else:
            gms_srcs = pd.concat((gms_srcs,current_gms_src),ignore_index=True)

    # fr srcsi
    fr_src_file_name = glob( join(fr_data_dir,'*',"src_full_crosswalked.csv"))[0]
    fr_src = pd.read_csv( fr_src_file_name )

    # average out values by stage
    gms_srcs = gms_srcs.loc[:,['Stage','Discharge (m3s-1)','Volume (m3)','BedArea (m2)','Number of Cells']].groupby('Stage').agg('mean').reset_index(drop=False)
    fr_src = fr_src.loc[:,['Stage','Discharge (m3s-1)','Volume (m3)','BedArea (m2)','Number of Cells']].groupby('Stage').agg('mean').reset_index(drop=False)
    
    # SRC plots
    plt.figure()
    plt.plot(gms_srcs.loc[:,'Discharge (m3s-1)'],gms_srcs.loc[:,'Stage'],label='GMS Mean')
    plt.plot(fr_src.loc[:,'Discharge (m3s-1)'],fr_src.loc[:,'Stage'],label="FIM3 FR Mean")
    plt.xlabel('Discharge (m3s-1)')
    plt.ylabel('Stage (m)')
    plt.legend()
    if outputs_dir is not None:
        plt.savefig( join(outputs_dir,"rating_curve.jpg") )
    #combined = pd.concat((gms_srcs,fr_src), ignore_index=True, axis=1)
    
    # Volume plots
    plt.figure()
    plt.plot(gms_srcs.loc[:,'Stage'],gms_srcs.loc[:,'Volume (m3)'],label='GMS Mean')
    plt.plot(fr_src.loc[:,'Stage'],fr_src.loc[:,'Volume (m3)'],label="FIM3 FR Mean")
    plt.xlabel('Stage (m)')
    plt.ylabel('Volume (m3)')
    plt.legend()
    if outputs_dir is not None:
        plt.savefig( join(outputs_dir,"volume.jpg") )
    
    # Bed Area plots
    plt.figure()
    plt.plot(gms_srcs.loc[:,'Stage'],gms_srcs.loc[:,'BedArea (m2)'],label='GMS Mean')
    plt.plot(fr_src.loc[:,'Stage'],fr_src.loc[:,'BedArea (m2)'],label="FIM3 FR Mean")
    plt.xlabel('Stage (m)')
    plt.ylabel('Bed Area (m2)')
    plt.legend()
    if outputs_dir is not None:
        plt.savefig( join(outputs_dir,"bedarea.jpg") )
    
    # Volume and Bed Area plots
    plt.figure()
    plt.plot(gms_srcs.loc[:,'Stage'],
             np.power(gms_srcs.loc[:,'Volume (m3)'],5/3) * np.power(gms_srcs.loc[:,'BedArea (m2)'],-2/3),
             label='GMS Mean')
    plt.plot(fr_src.loc[:,'Stage'],
             np.power(fr_src.loc[:,'Volume (m3)'],5/3) * np.power(fr_src.loc[:,'BedArea (m2)'],-2/3),
             label="FIM3 FR Mean")
    plt.xlabel('Stage (m)')
    plt.ylabel('Volume (m3s-1) ^ 5/3 divided by Bed Area (m2) ^ -2/3')
    plt.legend()
    if outputs_dir is not None:
        plt.savefig( join(outputs_dir,"volume_and_bedarea.jpg") )
    

    # Number of Cells plots
    plt.figure()
    plt.plot(gms_srcs.loc[:,'Stage'],gms_srcs.loc[:,'Number of Cells'],label='GMS Mean')
    plt.plot(fr_src.loc[:,'Stage'],fr_src.loc[:,'Number of Cells'],label="FIM3 FR Mean")
    plt.xlabel('Stage (m)')
    plt.ylabel('Number of Cells')
    plt.legend()
    if outputs_dir is not None:
        plt.savefig( join(outputs_dir,"number_of_cells.jpg") )




if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Comparing SRCs')
    parser.add_argument('-f','--fr-data-dir', help='Directory with FR data', required=True)
    parser.add_argument('-o','--outputs-dir', help='Directory for outputs data', required=False,default=None)

    args = vars(parser.parse_args())

    join_srcs(**args)

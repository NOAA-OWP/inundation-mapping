#!/usr/bin/env python3

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import argparse
import matplotlib.transforms as mtransforms

""" Generates graph of progression of CSI results for BLE Domain """

def generate_csi_dataframe():

    data = { 'version' : [ '1 - FR\nNFIE', '2 - FR\nESRI', '3 - FR\nb0bb2c6', '3 - FR\nhydcon',
                           '3 - FR\nn=0.12', '3 - MS\nn=0.12', '4 - GMS\nn=0.12', 
                           '4\nGMS+\n3DEP\nn=0.12'],
             'Date Results Available' : [ 'Late\n2019','Late\n2019','Mid\n2020','Late\n2020',
                                          'Mid\n2021','Mid\n2021', 'Late\n2021',
                                          'Early\n2022' ],
             '100 yr' : [53.43,56.13,56.51,55.76,59.15,60.69,61.82,64.75],
             '500 yr' : [56.04,59.01,59.31,58.39,61.49,63.09,64.35,66.62]
           }

    df = pd.DataFrame(data)

    return(df)


def make_progression_plot(output_filename):
    
    df = generate_csi_dataframe()

    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    ax2 = ax1.twiny()

    ax1.plot('version','100 yr',marker='.',data=df,label='100 yr')
    ax1.plot('version','500 yr',marker='.',data=df,label='500 yr')

    trans_offset = mtransforms.offset_copy(ax1.transData, fig=fig,
                                       x=0.00, y=0.025, units='inches')

    for i,row in df.iterrows():
        ax1.text(row['version'],row['100 yr'],row['100 yr'],
                 transform=trans_offset,
                 rotation=25,
                 horizontalalignment='center')
        ax1.text(row['version'],row['500 yr'],row['500 yr'],
                 transform=trans_offset,
                 rotation=25,
                 horizontalalignment='center')

    ax1.set_ylim((50,70))

    ax1.set_xlabel('Version')
    ax1.set_ylabel('Critical Success Index (CSI, %)')
    ax1.set_title('OWP FIM Skill Progression',fontsize=20)
    
    for tick in ax1.get_xticklabels():
        tick.set_rotation(0)
        tick.set_horizontalalignment('center')

    ax2.set_xlim(ax1.get_xlim())
    ax2.set_xticks(ax1.get_xticks())
    ax2.set_xticklabels(df['Date Results Available'])
    ax2.set_xlabel('Evaluation Date on BLE Domain')

    for tick in ax2.get_xticklabels():
        tick.set_rotation(35)
        tick.set_horizontalalignment('center')

    ax1.annotate('Overall Improvement\n10-11 CSI Points (~20%)',xy=(4.5,55))

    ax1.legend()

    plt.tight_layout()
    fig.savefig(output_filename,dpi=300)

if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Makes FIM Skill Progression Plot')
    parser.add_argument('-o','--output-filename',help='Output Filename',required=True)


    args = vars(parser.parse_args())

    make_progression_plot(**args)

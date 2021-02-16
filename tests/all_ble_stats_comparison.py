#!/usr/bin/env python3

import os
import pandas as pd
import argparse


def subset_vector_layers(huclist, branch, current_dev, outfolder):

    test_cases= r'data/test_cases_new'
    ble_sitelist = [str(line.rstrip('\n')) for line in open(huclist)]
    stat_list = ['fim_1_0_0', 'fim_2_3_3',str(current_dev), 'new_feature','eval']
    eval_all = pd.DataFrame([])
        
    # stat_list = stat_list + [branch]
    for site in ble_sitelist:
        eval_100_path=os.path.join(test_cases,str(site) + '_ble', 'performance_archive', 'development_versions', branch, '100yr','stats_summary.csv')
        eval_500_path=os.path.join(test_cases,str(site) + '_ble', 'performance_archive', 'development_versions', branch, '500yr','stats_summary.csv')
        
        if os.path.exists(eval_100_path) and os.path.exists(eval_500_path):
            eval_100 = pd.read_csv(eval_100_path,index_col=0)
            eval_100['eval'] = '100yr'

            eval_500 = pd.read_csv(eval_500_path,index_col=0)
            eval_500['eval'] = '500yr'

            eval_combined = eval_100.append(eval_500)
            eval_combined.columns = ['new_feature' if x==str(branch) else x for x in eval_combined.columns]
            eval_combined = eval_combined.filter(items=stat_list)
            eval_combined = eval_combined.reindex(columns=stat_list)
            eval_combined['site'] = str(site)
            eval_combined['branch'] = str(branch)
            eval_all = eval_all.append(eval_combined)

    if not os.path.exists(outfolder):
        os.makedirs(outfolder)
    eval_all.to_csv(os.path.join(outfolder,'ble_stats_comparison.csv'))

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Collect eval stats for BLE sites')
    parser.add_argument('-b','--huclist', help='list of ble sites to test', required=True,type=str)
    parser.add_argument('-e','--branch', help='list of outfolder(s)', required=False,type=str)
    parser.add_argument('-d','--current-dev',help='name of current dev stat column',required=True,type=str)
    parser.add_argument('-f','--outfolder',help='output folder',required=True,type=str)

    args = vars(parser.parse_args())

    huclist = args['huclist']
    branch = args['branch']
    current_dev = args['current_dev']
    outfolder = args['outfolder']

    subset_vector_layers(huclist,branch,current_dev,outfolder)

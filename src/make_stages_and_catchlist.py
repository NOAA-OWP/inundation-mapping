#!/usr/bin/env python3

import geopandas as gpd
import numpy as np
import sys


@profile
def make_stages_and_catchlist(flows_filename, catchments_filename, stages_filename, catchlist_filename, stages_min, stages_interval, stages_max):
    flows = gpd.read_file(flows_filename)
    catchments = gpd.read_file(catchments_filename)


    hydroIDs = flows['HydroID'].tolist()
    len_of_hydroIDs = len(hydroIDs)
    slopes = flows['S0'].tolist()
    lengthkm = flows['LengthKm'].tolist()
    areasqkm = catchments['areasqkm'].tolist()


    stages_max = stages_max + stages_interval
    stages = np.round(np.arange(stages_min,stages_max,stages_interval),4)

    with open(stages_filename,'w') as f:
        f.write("Stage\n")
        for stage in stages:
            f.write("{}\n".format(stage))

    with open(catchlist_filename,'w') as f:
        f.write("{}\n".format(len_of_hydroIDs))
        for h,s,l,a in zip(hydroIDs,slopes,lengthkm,areasqkm):
            f.write("{} {} {} {}\n".format(h,s,l,a))

    #TODO we need a main block
if __name__ == '__main__':
    # Parse arguments.
    parser = argparse.ArgumentParser(description='make_stages_and_catchlist.py')
    parser.add_argument('-f', '--flows-filename', help='flows-filename',required=True)
    parser.add_argument('-c', '--catchments-filename', help='catchments-filename',required=True)
    parser.add_argument('-s', '--stages-filename', help='stages-filename',required=True)
    parser.add_argument('-a', '--catchlist-filename', help='catchlist-filename',required=True)
    parser.add_argument('-m', '--stages-min', help='stages-min',required=True)
    parser.add_argument('-i', '--stages-interval', help='stages-interval',required=True)
    parser.add_argument('-t', '--stages-max', help='stages-max',required=True)

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    make_stages_and_catchlist(**args)

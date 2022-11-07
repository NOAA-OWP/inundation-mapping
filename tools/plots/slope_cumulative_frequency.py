#!/usr/bin/env python3

import pygeohydro
import py3dep
import argparse
import matplotlib.pyplot as plt


def make_histogram(huc4codes,resolution):

    wbd = pygeohydro.pygeohydro.WBD('huc4')
    huc4_gdf = wbd.byids('huc4',huc4codes)

    def _get_slopes(huc4_row,resolution,geo_crs):
        return py3dep.get_map('Slope Degrees',huc4_row.geometry, resolution=resolution, geo_crs=geo_crs)
        
    slopes = huc4_gdf.apply(_get_slopes,axis=1,resolution=resolution, geo_crs=huc4_gdf.crs).tolist()

    slopes_m2m = [py3dep.deg2mpm(s) for s in slopes]

    #fig = plt.figure(figsize=(9,9),dpi=300)
    fig = plt.figure(dpi=300)
    ax = fig.add_subplot(1,1,1)

    """
    plt.rc('title', size=28)
    plt.rc('legend', size=26)
    plt.rc('axes', size=24)
    plt.rc('xtick', size=22)
    plt.rc('ytick', size=22)
    """

    # slopes 
    for s in slopes_m2m:
        s.plot.hist(ax=ax, cumulative=True,density=True, bins=10000, histtype='step', range=(0,2))
    
    
    ax.set_title('Terrain Slope')
    ax.set_ylabel('Cumulative Frequency')
    ax.set_xlabel('Slope (Vertical/Horizontal)')
    ax.set_xlim(0,0.8)

    ax.legend(huc4codes,loc='lower right', title='HUC4')
    #ax.legend(slope_handles,labels=huc4codes)

    plt.tight_layout()
    plt.savefig('/data/misc/lidar_manuscript_data/TEST_hist.png')

make_histogram(['1202','1401'],30)

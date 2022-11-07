#!/usr/bin/env python3

import os

import geopandas as gpd
import rioxarray as rxr
import xarray-spatial
import geocube

# data dir
data_dir = os.path.join('data','misc','lidar_manuscript_data')

# variable declarations
nwm_catchments = os.path.join(data_dir,'nwm_catchments_1202.gpkg')

# load agreement rasters
agreement_rasters_string_template = os.path.join(data_dir,'3dep_test_1202_{}m_GMS_n_12_{}yr_agreement.vrt')

# agreement factors
resolutions = [3,5,10,15,20]
years = [100,500]

# load files

# define metrics


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

#make_histogram(['1202','1401'],30)

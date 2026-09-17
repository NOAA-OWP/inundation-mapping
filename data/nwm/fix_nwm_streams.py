import os

import geopandas as gpd

from src.utils.io import write_geodataframe


### This is a simple script is used to fix a few of the NWM streams that have coordinates
### in the wrong order. It is intended as a one-time fix for the input NWM streams data.

ids = [20906989, 20906985, 20906869]
dirname = '/data/inputs/nwm_hydrofabric/'
filename_in = os.path.join(dirname, '/nwm_flows.gpkg')
filename_out = os.path.join(dirname, 'nwm_flows_20250328.gpkg')

# dirname = '/data/inputs/pre_clip_huc8/20250218/11070203'  # previously 20241002
# filename_in = os.path.join(dirname, 'nwm_subset_streams.gpkg')
# filename_out = os.path.join(dirname, 'nwm_subset_streams_20250328.gpkg')

data = gpd.read_file(filename_in)
for id in ids:
    data.loc[data['ID'] == id, 'geometry'] = data[data['ID'] == id].geometry.reverse()
write_geodataframe(data, filename_out)

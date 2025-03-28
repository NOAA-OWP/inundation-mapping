import os
import geopandas as gpd

ids = [20906989, 20906985, 20906869]
# dirname = '/data/inputs/nwm_hydrofabric/'
# filename_in = os.path.join(dirname, '/nwm_flows.gpkg')
# filename_out = os.path.join(dirname, 'nwm_flows_20250328.gpkg')

dirname = '/data/inputs/pre_clip_huc8/20250218/11070203'  # previously 20241002
filename_in = os.path.join(dirname, 'nwm_subset_streams.gpkg')
filename_out = os.path.join(dirname, 'nwm_subset_streams_20250328.gpkg')
data = gpd.read_file(filename_in)
for id in ids:
    data.loc[data['ID'] == id, 'geometry'] = data[data['ID'] == id].geometry.reverse()
data.to_file(filename_out, driver='GPKG')

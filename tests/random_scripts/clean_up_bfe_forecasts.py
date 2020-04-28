#!/usr/bin/env python3

import pandas as pd
import geopandas as gpd
import sys

orig_table = pd.read_csv(sys.argv[1])
# esri_cross_walk_table = pd.read_csv(sys.argv[2])
# cross_walk_table_fileName = sys.argv[3]

orig_flows = ['E_Q_10PCT','E_Q_01PCT','E_Q_0_2PCT']
flows = ['10yr','100yr','500yr']
huc6code = 120903
dischargeMultiplier = 0.3048 ** 3

for i,og in enumerate(orig_flows):

    forecast = orig_table[['feature_id',og]]

    forecast = forecast.rename(columns={og : 'discharge'})
    forecast = forecast.astype({'feature_id' : int , 'discharge' : float})

    forecast = forecast.groupby('feature_id').median()
    forecast = forecast.reset_index(level=0)

    forecast['discharge'] = forecast['discharge'] * dischargeMultiplier

    forecast.to_csv("forecast_{}_{}.csv".format(huc6code,flows[i]),index=False)

# cross_walk_table = esri_flows[['HydroID','feature_id']]
# cross_walk_table = cross_walk_table.dropna()
# cross_walk_table = cross_walk_table.astype({'feature_id': int,'HydroID' : int})
# cross_walk_table.to_csv(cross_walk_table_fileName,index=False)
exit()
cross_walk_table = esri_cross_walk_table[['KeyTo','KeyFrom']]
cross_walk_table = cross_walk_table.rename(columns={'KeyTo' : 'HydroID', 'KeyFrom' : 'feature_id'})
cross_walk_table = cross_walk_table.dropna()
cross_walk_table = cross_walk_table.astype({'feature_id': int,'HydroID' : int})
cross_walk_table.to_csv(cross_walk_table_fileName,index=False)

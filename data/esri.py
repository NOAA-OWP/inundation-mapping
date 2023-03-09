#!/usr/bin/env python3
import sys
import requests
import pandas as pd
import geopandas as gpd

sys.path.append('/foss_fim/src')
from utils.shared_variables import DEFAULT_FIM_PROJECTION_CRS

class ESRI_REST(object):
    """
    This class was built for querying ESRI REST endpoints for the purpose of downloading datasets.
    See /data/nld/levee_download.py for an example useage.
    """
    
    def __init__(self, query_url, params):
        self.query_url = query_url
        self.params = params
        self.exceededTransferLimit = True
    
    @classmethod
    def query(cls, query_url:str, save_file:str=None, params:dict=dict(f="json", where="1=1", returnGeometry="True", outFields="*", outSR=DEFAULT_FIM_PROJECTION_CRS)):
        rest_call = cls(query_url, params)
        gdf_complete = rest_call._query_rest(save_file)
        # Save geodataframe as geopackage
        if save_file:
            gdf_complete.to_file(save_file, driver="GPKG", index=False)
        else:
            return gdf_complete

    def _query_rest(self, save_file):
        gdf_list = []
        record_count = 0
        while self.exceededTransferLimit:
            # Set the resultOffset to the number of records that's already been downloaded
            self.params['resultOffset'] = record_count
            sub_gdf = self._sub_query(self.params)
            gdf_list.append(sub_gdf)
            record_count += len(sub_gdf)
            print(record_count, end='\r')
        gdf_complete = pd.concat(gdf_list)
        return gdf_complete

    def _sub_query(self, params):
        self.response = requests.get(self.query_url, params=params)
        if not self.response.ok:
            raise Exception(f"The following URL recieved a bad response.\n{self.response.url}")
        # Set exceededTransferLimit if there need to be another sub_request
        r_dict = self.response.json()
        if 'exceededTransferLimit' in r_dict.keys():
            self.exceededTransferLimit = r_dict['exceededTransferLimit']
        else:
            self.exceededTransferLimit = False
        sub_gdf = gpd.read_file(self.response.text)
        return sub_gdf
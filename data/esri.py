#!/usr/bin/env python3
import requests
import pandas as pd
import geopandas as gpd


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
    def query(cls, query_url:str, save_file:str=None, **kwargs):
        '''
        Classmethod for easily queries on ESRI feature services. See /data/nld/levee_download.py for example usage.

        Parameters
        ----------
        query_url: str
            URL to query. This should have the layer # and 'query' at the end, e.g.
            https://ags03.sec.usace.army.mil/server/rest/services/NLD2_PUBLIC/FeatureServer/15/query
        save_file: str
            Optional. Location to save the output geopackage. This method will not return
            a geodataframe if this parameter is set.
        **kwargs
            All kwargs get passed to the service query. Here's an example of some standard ones:
            `f="json", where="1=1", returnGeometry="True", outFields="*", outSR="5070"`

        Returns
        -------
        gdf_complete: geopandas.GeoDataFrame
            GeoDataFrame containing all of the features returned by the query. `None` is returned if the 
            save_file parameter is set.
        '''
        # Query the input URL using the kwargs as URL parameters
        rest_call = cls(query_url, kwargs)
        gdf_complete = rest_call._query_rest()
        # Save geodataframe as geopackage
        if save_file:
            gdf_complete.to_file(save_file, driver="GPKG", index=False)
        else:
            return gdf_complete

    def _query_rest(self):
        '''
        Method that sets up multiple REST calls when there are more features than the transfer limit
        set by the feature service and concatenates each response into a single GeoDataFrame.

        Returns
        -------
        gdf_complete: geopandas.GeoDataFrame
            GeoDataFrame containing all of the features returned by the query. 
        '''
        gdf_list = []
        record_count = 0
        backup_counter = 0
        # Call the REST API repeatedly until all of the features have been collected, i.e. the transfer
        # limit has no longer been exceeded
        while (self.exceededTransferLimit) and (backup_counter < 9999):
            # Set the resultOffset to the number of records that's already been downloaded
            self.params['resultOffset'] = record_count
            sub_gdf = self._sub_query(self.params)
            gdf_list.append(sub_gdf)
            record_count += len(sub_gdf)
            backup_counter += 1
            print(record_count, end='\r')
        # Concatenate all responses into a single geodataframe
        gdf_complete = pd.concat(gdf_list)
        return gdf_complete

    def _sub_query(self, params):
        '''
        This method calls the REST API.

        Returns
        -------
        sub_gdf: geopandas.GeoDataFrame
            GeoDataFrame containing features returned by the query. FYI This may not be the full
            dataset if the 'exceededTransferLimit' == True
        '''
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
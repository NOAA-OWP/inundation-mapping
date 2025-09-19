#!/usr/bin/env python3
import geopandas as gpd
import pandas as pd
import requests
from tqdm import tqdm


gpd.options.io_engine = "pyogrio"


class ESRI_REST(object):
    """
    This class was built for querying ESRI REST endpoints for the purpose of downloading datasets.
    See /data/nld/levee_download.py for an example useage.
    """

    def __init__(self, query_url, params, verbose=True):
        self.query_url = query_url
        self.params = params
        self.verbose = verbose
        self.exceededTransferLimit = True
        self.feature_count = 0

    @classmethod
    def query(cls, query_url: str, save_file: str = None, **kwargs):
        '''
        Classmethod for easily queries on ESRI feature services.
        See /data/nld/levee_download.py for example usage.

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
            `f="json", where="1=1", returnGeometry="true", outFields="*", outSR="5070"`

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
            gdf_complete.to_file(save_file, driver="GPKG", index=False, engine='fiona')
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
        if self.verbose:
            self._meta_query()
            print("-------------------------------------")
            print(f"Service name: {self.metadata['name']}")
            print(f"Features returned: {self.feature_count}")
            if 'resultRecordCount' in self.params.keys():
                print(f"Request max record count: {self.params['resultRecordCount']}")
                print(f"Total API calls: {-(self.feature_count//-self.params['resultRecordCount'])}")
            else:
                print(f"Service max record count: {self.metadata['maxRecordCount']}")
                print(f"Total API calls: {-(self.feature_count//-self.metadata['maxRecordCount'])}")
        # Call the REST API repeatedly until all of the features have been collected, i.e. the transfer
        # limit has no longer been exceeded
        with tqdm(
            total=self.feature_count, desc='Feature download progress', disable=not self.verbose
        ) as pbar:
            while (self.exceededTransferLimit) and (backup_counter < 9999):
                # Set the resultOffset to the number of records that's already been downloaded
                self.params['resultOffset'] = record_count
                sub_gdf = self._sub_query(self.params)
                gdf_list.append(sub_gdf)
                record_count += len(sub_gdf)
                backup_counter += 1
                pbar.update(len(sub_gdf))
        # Concatenate all responses into a single geodataframe
        gdf_complete = pd.concat(gdf_list)
        return gdf_complete

    def _sub_query(self, params):
        '''
        This method calls the REST API.

        Parameters
        ----------
        params: dict
            Parameters for the rest query.

        Returns
        -------
        sub_gdf: geopandas.GeoDataFrame
            GeoDataFrame containing features returned by the query. FYI This may not be the complete
            dataset if the 'exceededTransferLimit' == True
        '''
        self._api_call(self.query_url, params)
        # Set exceededTransferLimit if there need to be another sub_request
        r_dict = self.response.json()
        if 'exceededTransferLimit' in r_dict.keys():
            self.exceededTransferLimit = r_dict['exceededTransferLimit']
        # This very nondescript error was returned when querying a polygon layer.
        # Setting resultRecordCount to a lower value fixed the error.
        elif 'error' in r_dict.keys():
            print(
                "There was an error with the query. It may have been caused by requesting too many features. "
                "Try setting resultRecordCount to a lower value."
            )
            raise Exception(r_dict['error']['message'], f"code: {r_dict['error']['code']}")
        else:
            self.exceededTransferLimit = False
        # Read the response into a GeoDataFrame
        sub_gdf = gpd.read_file(self.response.text)
        return sub_gdf

    def _api_call(self, url, params=None):
        '''
        Helper method for calling the API and checking that the response is ok.


        '''
        self.response = requests.get(url, params=params)
        if not self.response.ok:
            raise Exception(f"The following URL recieved a bad response.\n{self.response.url}")

    def _meta_query(self):
        # Get the service metadata
        self._api_call(self.query_url[: self.query_url.rfind('query')], self.params)
        self.metadata = self.response.json()
        # Get the record count returned by the query
        params = self.params.copy()
        params['returnCountOnly'] = "true"
        self._api_call(self.query_url, params)
        self.feature_count = self.response.json()['count']

#!/usr/bin/env python3
import os, re
import pandas as pd
import numpy as np
import sqlite3 as sql
import argparse
import warnings

class WaterSurfaceElev():
    """
    Class for comparing water surface elevation of FIM and USGS/AHPS water surface elevation (WSE).

    Methods
    ----------
    WaterSurfaceElev.wse_compare(flows_dataframe)
        Compares water surface elevation given an input flow file.

    WaterSurfaceElev.get_feature_id_list()
        Returns a list of feature_ids that the database can has stored and can compare WSE.

    WaterSurfaceElev.build(fim_dir, usgs_gages_filename)
        Use this method initially to build the database that the other methods use to compare WSE.

    WaterSurfaceElev.print_schema()
        Prints the schema of all tables contained within the database.

    Example Usage
    ----------
    import pandas as pd
    from compare_water_surface_elev import WaterSurfaceElev

    path_to_database = '/data/fim/sierra_test_rating_curves.db'
    flows_dataframe = pd.read_csv('/data/fim/flows_cfs.csv')

    wse_compare = WaterSurfaceElev(path_to_database)
    wse_compare.build_db(fim_dir, usgs_gages_filename)
    wse_dataframe = wse_compare.convert_flows_to_wse(flows_dataframe)
    """

    def __init__(self, db_filepath):

        self.db_filepath = db_filepath
        self.conn = None
        self.schema_dataframe = None

    def convert_flows_to_wse(self, flows_dataframe):
        """
        Converts a flow for a specific HydroID into Water Surface Elevations using both USGS rating curves and FIM Synthetic Rating Curves

        Parameters
        ----------
        flows_dataframe : pandas.DataFrame
            Pandas DataFrame that must contain the following columns: {'feature_id': int, 'discharge_cfs': float}.
        db_filepath : str
            File path to the sierra_test_rating_curve.db SQLite database.

        Returns
        -------
        gage_dataframe : pandas.DataFrame
            DataFrame that contains the columns for the dynamic sierra test service. Columns:
            [location_id, nws_lid, HydroID, huc, dem_adj_elevation, feature_id, discharge_cfs, fim_wse_ft, usgs_wse_ft, wse_diff_ft
        """

        self.conn = sql.connect(self.db_filepath)

        crosswalk_table = pd.read_sql('''
            SELECT DISTINCT location_id,nws_lid,hydroid,huc,dem_adj_elevation,feature_id_wrds
            FROM usgs_elev_table
            ''', self.conn)

        # Join the flows to the database data
        crosswalk_table['feature_id'] = crosswalk_table.feature_id_wrds  # new usgs_elev_table has the WRDS feature_id (which we're defining as authoritative)
        crosswalk_table.drop('feature_id_wrds', axis='columns', inplace=True)
        gage_dataframe = crosswalk_table.merge(flows_dataframe[['feature_id','discharge_cfs']], on='feature_id', how='left')
        gage_dataframe['dem_adj_elevation_ft'] = round(gage_dataframe.dem_adj_elevation * 3.28084, 2) # meters to feet
        gage_dataframe.drop('dem_adj_elevation', axis=1, inplace=True)
            
        try: # Try-finally block ensures that the database connection is closed properly even if an exception is thrown
            gage_dataframe['fim_wse_ft'] = gage_dataframe.apply(self._get_fim_wse, axis=1)
            gage_dataframe['usgs_wse_ft'] = gage_dataframe.apply(self._get_usgs_wse, axis=1)

            gage_dataframe['wse_diff_ft'] = gage_dataframe['fim_wse_ft'] - gage_dataframe['usgs_wse_ft']
        except Exception as e:
            raise
        finally:
            self.conn.close()
            del self.conn
            
        return gage_dataframe

    def _get_usgs_wse(self, row):

        flow = row['discharge_cfs']
        location_id = row['location_id']

        usgs_wse = np.nan
        if not location_id: # return nan for AHPS-only locations
            return usgs_wse

        crsr = self.conn.cursor()
        # Get the USGS water surface elevation
        usgs_rc_table = crsr.execute(f'''
            SELECT flow,elevation_navd88 
            FROM usgs_rating_curves 
            WHERE location_id = '{location_id}' 
            ORDER BY elevation_navd88 ASC''').fetchall()
        if usgs_rc_table:
            # Interpolate USGS rating curve
            usgs_q, usgs_s = [i for i, j in usgs_rc_table],[j for i, j in usgs_rc_table]
            usgs_wse = round(np.interp(flow, usgs_q, usgs_s, left=np.nan, right=np.nan), 2)
        
        return usgs_wse

    def _get_fim_wse(self, row):

        flow = row['discharge_cfs'] / 35.3147 # conver CFS to CMS -- FIM stage is in CMS
        hydroid =  row['HydroID']

        crsr = self.conn.cursor()
        fim_wse = np.nan
        
        # Get the FIM water surface elevation
        fim_rc_table = crsr.execute(f'''
            SELECT discharge_cms,stage 
            FROM hydrotable 
            WHERE hydroid = {hydroid} 
            ORDER BY stage ASC''').fetchall()
        if fim_rc_table:
            # Interpolate SRC
            fim_q, fim_s = [i for i, j in fim_rc_table],[j for i, j in fim_rc_table]
            fim_stage = round(np.interp(flow, fim_q, fim_s, left=np.nan, right=np.nan), 2)
            fim_wse = fim_stage * 3.28084 + row.dem_adj_elevation_ft  # convert stage to elevation and to feet

        return fim_wse

    def get_feature_id_list(self):
        """
        Gets a lits of all feature_ids that are in the sierra_test_rating_curves.db

        Parameters
        ----------
        db_filepath : str
            File path to the sierra_test_rating_curve.db SQLite database.

        Returns
        -------
        feature_id_list : list
            List of all feature_ids.
        """
        self.conn = sql.connect(self.db_filepath)
        crsr = self.conn.cursor()
        feature_id_list = crsr.execute('''SELECT DISTINCT feature_id_wrds FROM usgs_elev_table''').fetchall()
        self.conn.close()
        feature_id_list = [i[0] for i in feature_id_list]

        return feature_id_list

    def build_db(self, fim_dir, usgs_gages_filename):
        """
        Builds the database (sierra_test_rating_curves.db) using the usgs_elev_table.csv & hydrotable.csv within fim_dir and USGS
        rating curves in usgs_gages_filename.

        Parameters
        ----------
        fim_dir : str
            File path to the sierra_test_rating_curve.db SQLite database.
        usgs_gages_filename : str
            File path to the usgs rating curve CSV.
        """

        print(f"Reading USGS gages csv and copying into database at {self.db_filepath}")
        usgs_rating_curves = pd.read_csv(usgs_gages_filename, dtype={'location_id': str})
        self.conn = sql.connect(self.db_filepath)
        usgs_rating_curves.to_sql('usgs_rating_curves', self.conn, if_exists='replace', index=False)

        # Add FIM hydroTable and usgs_elev_table to database
        print("Reading through FIM folders and aggregating hydroTable.csv & usgs_elev_table.csv into database")
        for huc_folder in [name for name in os.listdir(fim_dir) if os.path.isdir(fim_dir + os.sep + name) and re.search("\d{6,8}$", name)]:
            print(huc_folder, end='\r')
            if not os.path.exists(os.path.join(fim_dir, huc_folder, 'usgs_elev_table.csv')): continue
            hydrotable = pd.read_csv(os.path.join(fim_dir, huc_folder, 'hydroTable.csv'), dtype={'HUC': str,'feature_id': int})
            elev_table = pd.read_csv(os.path.join(fim_dir, huc_folder, 'usgs_elev_table.csv'), dtype={'location_id': str, 'HydroID': int, 'feature_id': int, 'feature_id_wrds': int})
            elev_table['huc'] = huc_folder
            # Calculate WSE and discharge fields for FIM hydrotable
            hydrotable = hydrotable[hydrotable.HydroID.isin(elev_table.HydroID.unique())] # only store rating curves whose hydroid is in usgs_elev_table.csv
            hydrotable = hydrotable[['HydroID', 'stage', 'discharge_cms']] # only pass select columns to database to keep size small
            # Add tables to database
            hydrotable.to_sql('hydrotable', self.conn, if_exists='append', index=False)
            elev_table.to_sql('usgs_elev_table', self.conn, if_exists='append', index=False)

        crsr = self.conn.cursor()
        crsr.execute("CREATE INDEX index_feature_id ON usgs_elev_table (feature_id_wrds);")
        crsr.execute("CREATE INDEX index_location_id ON usgs_rating_curves (location_id);")
        crsr.execute("CREATE INDEX index_hydroid ON hydrotable (hydroid);")
        self.conn.close()
        del self.conn

        print(f'Successfully created {self.db_filepath}')
    
    def print_schema(self):
        """Prints all tables and their schema in a friendly format"""
        self.conn = sql.connect(self.db_filepath)
        crsr = self.conn.cursor()

        # Get the tables and columns
        tables = crsr.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        columns = ('index', 'column_name', 'data_type', 'nullable', 'default', 'pk')

        # Put the schema of each table into a dataframe for friendly printing
        for table in tables:
            table = table[0]
            print('\n' + table)
            
            data = crsr.execute(f'PRAGMA TABLE_INFO({table})').fetchall()
            schema_df = pd.DataFrame(data=data, columns=columns)
            schema_df.drop('index', axis=1, inplace=True)
            print(schema_df)
        self.conn.close()
        
if __name__ == '__main__':
    """
    Builds the database (sierra_test_rating_curves.db) that is used to compare FIM water surface elevations to AHPS/USGS water
    surface elevations.
    """

    parser = argparse.ArgumentParser(description='generate comparison between FIM and USGS water surface elevations')
    parser.add_argument('-fim_dir','--fim-dir', help='FIM output dir', required=False,type=str)
    parser.add_argument('-gages','--usgs-gages-filename',help='USGS rating curves',required=False,type=str)
    parser.add_argument('-output_dir','--output-dir', help='sierra_test_rating_curves.db output folder', required=True,type=str)
    parser.add_argument('-o','--overwrite', help='overwrite if sierra_test_rating_curves.db already exists in output folder', required=False,default=False,action='store_true')

    args = vars(parser.parse_args())

    fim_dir = args['fim_dir']
    output_dir = args['output_dir']
    usgs_gages_filename = args['usgs_gages_filename']
    overwrite = args['overwrite']
    database = os.path.join(output_dir, 'sierra_test_rating_curves.db')

    # Check for required directories and files
    assert os.path.isdir(fim_dir)
    assert os.path.exists(usgs_gages_filename)

    # Create output directory if it does not exist
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    # Delete sierra_test_rating_curves.db if overwrite flag is passed
    if overwrite and os.path.exists(database):
        os.remove(database)
    elif os.path.exists(database):
        raise Exception(f'{database} already exists. Please specify a different output location or use the overwrite (-o) flag')

    # Make database for rating curve tables
    wse_compare = WaterSurfaceElev(database)
    wse_compare.build_db(fim_dir, usgs_gages_filename)

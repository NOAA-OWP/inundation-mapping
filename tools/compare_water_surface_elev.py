#!/usr/bin/env python3
import os, re
import pandas as pd
import numpy as np
import sqlite3 as sql
import argparse


def wse_compare(flows_dataframe, db_filepath):
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
        [location_id, HydroID, huc8, dem_adj_elevation, str_order, feature_id, discharge_cfs, fim_wse_ft, usgs_wse_ft, wse_diff_ft
    """
    conn = sql.connect(db_filepath)

    '''
        SELECT DISTINCT usgs_elev_table.location_id,usgs_elev_table.hydroid,usgs_elev_table.huc8,usgs_elev_table.dem_adj_elevation,hydrotable.feature_id
        FROM usgs_elev_table
        LEFT JOIN hydrotable on hydrotable.hydroid = usgs_elev_table.hydroid
    '''
    
    crosswalk_table = pd.read_sql('''
        SELECT DISTINCT location_id,hydroid,huc8,dem_adj_elevation,feature_id_wrds
        FROM usgs_elev_table
        ''', conn)

    # Join the flows to the database data
    crosswalk_table['feature_id'] = crosswalk_table.feature_id_wrds  # new usgs_elev_table has the WRDS feature_id (which we're defining as authoritative)
    crosswalk_table.drop('feature_id_wrds', axis='columns', inplace=True)
    gage_dataframe = crosswalk_table.merge(flows_dataframe[['feature_id','discharge_cfs']], on='feature_id', how='inner')
    gage_dataframe.dem_adj_elevation = round(gage_dataframe.dem_adj_elevation * 3.28084, 2) # meters to feet
        
    try: # Try-finally block ensures that the database connection is closed properly even if an exception is thrown
        gage_dataframe['fim_wse_ft'] = gage_dataframe.apply(lambda row: get_fim_wse(row['discharge_cfs'], row['HydroID'], conn), axis=1)
        gage_dataframe['usgs_wse_ft'] = gage_dataframe.apply(lambda row: get_usgs_wse(row['discharge_cfs'], row['location_id'], conn), axis=1)

        gage_dataframe['wse_diff_ft'] = gage_dataframe['fim_wse_ft'] - gage_dataframe['usgs_wse_ft']
    except Exception as e:
        raise
    finally:
        conn.close()
        
    return gage_dataframe

def get_usgs_wse(flow, location_id, conn):

    usgs_wse = np.nan
    crsr = conn.cursor()
    # Get the USGS water surface elevation
    usgs_rc_table = crsr.execute(f'''
        SELECT flow,elevation_navd88 
        FROM usgs_rating_curves 
        WHERE location_id = '{location_id}' 
        ORDER BY flow ASC''').fetchall()
    if usgs_rc_table:
        # Interpolate USGS rating curve
        usgs_q, usgs_s = [i for i, j in usgs_rc_table],[j for i, j in usgs_rc_table]
        usgs_wse = round(np.interp(flow, usgs_q, usgs_s, left=np.nan, right=np.nan), 2)
    
    return usgs_wse

def get_fim_wse(flow, hydroid, conn):
    
    crsr = conn.cursor()
    fim_wse = np.nan
    
    # Get the FIM water surface elevation
    fim_rc_table = crsr.execute(f'''
        SELECT discharge_cfs,elevation_ft 
        FROM hydrotable 
        WHERE hydroid = {hydroid} 
        ORDER BY discharge_cfs ASC''').fetchall()
    if fim_rc_table:
        # Interpolate SRC
        fim_q, fim_s = [i for i, j in fim_rc_table],[j for i, j in fim_rc_table]
        fim_wse = round(np.interp(flow, fim_q, fim_s, left=np.nan, right=np.nan), 2)

    return fim_wse

def get_feature_id_list(db_filepath):
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
    conn = sql.connect(db_filepath)
    crsr = conn.cursor()
    feature_id_list = crsr.execute('''SELECT DISTINCT feature_id_wrds FROM usgs_elev_table''').fetchall()
    conn.close()
    feature_id_list = [i[0] for i in feature_id_list]

    return feature_id_list

if __name__ == '__main__':

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
        raise Exception(f'{database} already exists. Please specify a different output location or use the overwrite flag')

    # Make database for rating curve tables
    print(f"Reading USGS gages csv and copying into database at {database}")
    usgs_rating_curves = pd.read_csv(usgs_gages_filename, dtype={'location_id': str})
    conn = sql.connect(database)
    usgs_rating_curves.to_sql('usgs_rating_curves', conn, if_exists='replace', index=False)


    # Add FIM hydroTable and usgs_elev_table to database
    print("Reading through FIM folders and aggregating hydroTable.csv & usgs_elev_table.csv into database")
    for huc_folder in [name for name in os.listdir(fim_dir) if os.path.isdir(fim_dir + os.sep + name) and re.search("\d{8}$", name)]:
        print(huc_folder, end='\r')
        if not os.path.exists(os.path.join(fim_dir, huc_folder, 'usgs_elev_table.csv')): continue
        hydrotable = pd.read_csv(os.path.join(fim_dir, huc_folder, 'hydroTable.csv'), dtype={'HUC': str,'feature_id': int})
        elev_table = pd.read_csv(os.path.join(fim_dir, huc_folder, 'usgs_elev_table.csv'), dtype={'location_id': str, 'HydroID': int, 'feature_id': int, 'feature_id_wrds': int})
        elev_table['huc8'] = huc_folder
        # Calculate WSE and discharge fields for FIM hydrotable
        hydrotable = hydrotable[hydrotable.HydroID.isin(elev_table.HydroID.unique())]
        hydrotable['elevation_ft'] = (hydrotable.stage + hydrotable.dem_adj_elevation) * 3.28084
        hydrotable['discharge_cfs'] = hydrotable.discharge_cms * 35.3147
        # Add tables to database
        hydrotable.to_sql('hydrotable', conn, if_exists='append', index=False)
        elev_table.to_sql('usgs_elev_table', conn, if_exists='append', index=False)

    crsr = conn.cursor()
    crsr.execute("CREATE INDEX index_feature_id ON usgs_elev_table (feature_id_wrds);")
    crsr.execute("CREATE INDEX index_location_id ON usgs_rating_curves (location_id);")
    crsr.execute("CREATE INDEX index_hydroid ON hydrotable (hydroid);")
    conn.close()


#!/usr/bin/env python3

import argparse
import os
import pickle
import pandas as pd
import geopandas as gpd
from download_process_wrds import get_metadata
from dotenv import load_dotenv
from datetime import date, datetime, timezone


def read_format_usgs_data(usgs_data_txt, DEFAULT_DATA_CRS):
    '''
    Read and format USGS station metadata from a tab-separated USGS output file into a GeoDataFrame.

    Parameters
    ----------
    usgs_data_txt : str or file-like
        Path to a USGS metadata text file with a a 43-line header which is skipped prior
        to reading the table rows.
    DEFAULT_DATA_CRS : str or pyproj.CRS
        Coordinate reference system to assign when the file does not contain a coordinate
        datum value. This may be a PROJ string, EPSG code string (e.g. "EPSG:4326"), or
        a pyproj/GeoPandas-compatible CRS object.

    Returns
    -------
    geopandas.GeoDataFrame
        A GeoDataFrame built from a subset of the USGS station columns with a Point
        geometry column created from 'dec_long_va' (x) and 'dec_lat_va' (y). The returned
        GeoDataFrame has its CRS set to the first value found in the file's
        'dec_coord_datum_cd' column; if that value is empty/falsey the provided
        DEFAULT_DATA_CRS is used and a message is printed.

    Notes
    -----
    - The function assumes all rows in the file share the same datum and uses the first
      entry of 'dec_coord_datum_cd' to set the GeoDataFrame CRS.
    - If 'dec_coord_datum_cd' is missing/empty, DEFAULT_DATA_CRS will be used and a
      message is printed to stdout indicating the assumption.

    Possible errors
    ---------------
    - KeyError if the expected columns are not present in the parsed table.
    - ValueError from geopandas if the CRS value is not a valid/recognized CRS.
    
    TODO: Update or replace this function with a call to the USGS API (instead of 
    using a predownloaded USGS data file)
    '''

    # Read in USGS data file (skipping the 43-line header)
    usgs_data_df = pd.read_csv(usgs_data_txt, sep='\t', skiprows=43)

    # Keep necessary metadata columns only
    columns_to_keep = ['agency_cd', 'site_no', 'station_nm', 'site_tp_cd', 'dec_lat_va',
                    'dec_long_va', 'coord_meth_cd', 'coord_acy_cd', 'coord_datum_cd',
                    'dec_coord_datum_cd', 'district_cd', 'state_cd', 'county_cd',
                    'country_cd', 'alt_va', 'alt_meth_cd', 'alt_acy_va', 'alt_datum_cd',
                    'huc_cd']
    usgs_data_df = usgs_data_df[columns_to_keep].drop(0)
    
    # Get the CRS (assumes that all data in the file have same CRS)
    usgs_data_crs = usgs_data_df['dec_coord_datum_cd'].tolist()[0]

    if not usgs_data_crs:
        # Use the default CRS if not available
        usgs_data_crs = DEFAULT_DATA_CRS
        print(f'Datum not available in USGS data, assuming default datum ({usgs_data_crs})') 
    
    # Create geometry column and make the df into a geodataframe
    geometry = gpd.points_from_xy(usgs_data_df['dec_long_va'], usgs_data_df['dec_lat_va'])
    usgs_data_gdf = gpd.GeoDataFrame(usgs_data_df, geometry=geometry, crs=usgs_data_crs)

    return usgs_data_gdf


def download_format_metadata(site_thresholds_csv, metadata_url, DEFAULT_DATA_CRS):
    '''
    Download and format metadata for a set of NWS sites specified in a thresholds CSV.

    Parameters
    ----------
    site_thresholds_csv : str or pathlib.Path
        Filepath to a CSV containing site thresholds. The CSV must contain a column
        named 'nws_lid' whose values will be used to filter downloaded metadata.
    metadata_url : str
        WRDS API URL with metadata endpoint.
    DEFAULT_DATA_CRS : str
        Fallback coordinate reference system / datum to assign to the output GeoDataFrame
        when the downloaded metadata does not specify a lat/lon datum.
    Returns
    -------
    meta_gdf : geopandas.GeoDataFrame
        GeoDataFrame constructed from the downloaded metadata filtered to the
        site list in site_thresholds_csv. Columns include at minimum:
        - 'nws_lid': site identifier
        - 'lat', 'lon': numeric coordinates
        - 'lat_lon_datum': datum name from metadata (may be DEFAULT_DATA_CRS)
        - 'geometry': Point geometries created from lon/lat
        The GeoDataFrame's CRS is set to the datum found in metadata (or DEFAULT_DATA_CRS
        when missing).
    thresholds_df : pandas.DataFrame
        DataFrame read from site_thresholds_csv (the original thresholds table).

    Behavior and notes
    ------------------
    - If the first metadata record's 'lat_lon_datum' is false or missing, DEFAULT_DATA_CRS
      is used and a message is printed to indicate the assumption.

    '''

    # Read in the site thresholds
    thresholds_df = pd.read_csv(site_thresholds_csv)

    # Download and format metadata
    lid_list = thresholds_df['nws_lid'].tolist()
    
    oconus_meta_list, ___ = get_metadata(
        metadata_url,
        select_by='nws_lid',
        selector=lid_list, 
        must_include=None,
        upstream_trace_distance=None,
        downstream_trace_distance=None,
    )

    print(f'Downloaded metadata for {len(oconus_meta_list)} sites from WRDS API.')
    
    meta_lids, meta_lats, meta_lons, meta_datums = [], [], [], []
    
    for meta in oconus_meta_list: 
        lid = meta.get('identifiers').get('nws_lid')
        if lid in lid_list:
            lat = meta.get('nws_preferred').get('latitude')
            lon = meta.get('nws_preferred').get('longitude')
            datum = meta.get('nws_preferred').get('latlon_datum_name')
            meta_lids.append(lid)
            meta_lats.append(lat)
            meta_lons.append(lon)
            meta_datums.append(datum)        
    
    meta_df = pd.DataFrame({'nws_lid': meta_lids, 'lat': meta_lats, 'lon': meta_lons, 'lat_lon_datum': meta_datums})
    geometry = gpd.points_from_xy(meta_df['lon'], meta_df['lat'])

    # Get datum, if available, otherwise set datum to default value
    metadata_crs = meta_df['lat_lon_datum'].tolist()[0]
    if not metadata_crs:
        metadata_crs = DEFAULT_DATA_CRS # Use a default value
        print(f'Datum not available in downloaded metadata, assuming default datum ({metadata_crs})') 
    
    meta_gdf = gpd.GeoDataFrame(meta_df, geometry=geometry, crs=metadata_crs)

    return meta_gdf, thresholds_df


def trace_network(df, start_id, trace_dist_km):
    '''
    Trace upstream and downstream channel segments from a starting feature in a network.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing network attributes for reaches. Required columns:
        - 'nwm_feature_id' : unique identifier for the reach (used as current node id)
        - 'to'             : identifier of the downstream reach (target node)
        - 'order_'         : stream order (used to constrain tracing to the same order)
        - 'LengthKM'       : reach length in kilometers (used to accumulate distance)
        - 'Lake'           : integer/flag indicating lake presence (>0 indicates a lake)
    start_id : int or str
        The nwm_feature_id value from which to start the trace.
    trace_dist_km : int, float, or str
        Maximum cumulative distance (in km) to trace in each direction. The value
        is converted to float; tracing for a direction stops when the accumulated
        length is greater than or equal to this value.

    Returns
    -------
    tuple (trace_up, trace_down)
        - trace_up : list[int]
            A list of upstream nwm_feature_id values found by following rows where
            df['to'] == current_id. The list contains upstream nodes excluding the
            start_id (i.e., nodes strictly upstream of start_id). Order is from
            nearest upstream towards farther upstream as discovered by the loop.
        - trace_down : list[int]
            A list of downstream nwm_feature_id values discovered by following the
            'to' pointer starting at start_id. The start_id itself is included as
            the first element in trace_down if it satisfies the stopping conditions.

    Behavior and stopping conditions
    --------------------------------
    - The function traces separately in two directions (downstream by following
      the 'to' pointer; upstream by finding rows whose 'to' equals the current id).
    - Tracing in each direction is constrained to stays on reaches with the same
      stream order as the start_id (order_). The order of the start node is
      determined at the first step and used for both directions.
    - Tracing for a direction stops when any of the following occurs:
      - the next row/node cannot be found in df (missing node),
      - the stream order of the next node differs from the start node's order_,
      - the cumulative LengthKM for that direction reaches or exceeds trace_dist_km,
      - the node has Lake > 0 (a lake is encountered).
    - Length accumulation resets before tracing upstream (each direction accumulates
      independently).
    - When multiple upstream rows point to the same target (branching), the upstream
      trace picks the first matching row returned by the DataFrame selection. It
      does not explore multiple branches (i.e., it follows a single upstream path).

    Notes and caveats
    -----------------
    - df selections use .values[0] in the implementation; if duplicates or multiple
      matches exist the selection is effectively arbitrary among those matches.
    - Zero or negative trace_dist_km will typically cause immediate termination
      (no nodes beyond start depending on comparison and inclusion).
    - The function does not validate column types.
    '''
    current_id = start_id
    trace_up = []
    trace_down = []
    start_order = None  # Variable to store the start_order
    accumulated_length = 0    
    
    # Trace downstream
    while True:
        current_row = df[df['nwm_feature_id'] == current_id]
    
        if current_row.empty:
            break
    
        next_id = current_row['to'].values[0] # Was NextDownId
        order = current_row['order_'].values[0]
        length = current_row['LengthKM'].values[0]
        lake = current_row['Lake'].values[0]
            
        # Assign start_order when first encountered
        if start_order is None:
            start_order = order
    
        # Exit loop if the stream order has changed
        if order != start_order:
            break
    
        # Exit loop if the length has reached the max trace distance
        accumulated_length += length
        if accumulated_length >= float(trace_dist_km):
            break
    
        # Exit loop if you reach a lake
        if lake > 0:
            break
    
        # not dropping the HydroID that has the gauge location (need later)
        trace_down.append(int(current_id))

        # Load in the next ID to process
        current_id = next_id
    
    current_id = start_id  # Reset current_id for tracing down
    accumulated_length = 0
    
    # Trace downstream
    while True:

        # Get the row that feeds into current ID (as long as it's the same stream order)
        current_row = df[(df['to'] == current_id) & (df['order_'] == start_order)]
        if current_row.empty:
            break

        next_id = current_row['nwm_feature_id'].values[0]
        order = current_row['order_'].values[0]
        length = current_row['LengthKM'].values[0]
        lake = current_row['Lake'].values[0]
        
        # Exit loop if the stream order has changed
        if order != start_order:
            break
    
        # Exit loop if the length has reached the max trace distance
        accumulated_length += length
        if accumulated_length >= float(trace_dist_km):
            break
    
        # Exit loop if you reach a lake
        if lake > 0:
            break
    
        if current_id != start_id:
            trace_up.append(current_id)

        # Load in the next ID to process
        current_id = next_id

    return trace_up, trace_down


def crosswalk_trace_flows(joined_gdf, nhd_flowlines_gpkg, search):
    '''
    Crosswalk nearest NHD flowlines to site geometries and compute upstream/downstream traces.

    Parameters
    ----------
    joined_gdf : geopandas.GeoDataFrame
        GeoDataFrame of site points (or other geometries) to be matched to NHD flowlines.
        Must have a valid CRS (coordinate reference system). Geometries are used to
        perform a nearest spatial join to the NHD flowlines.
    nhd_flowlines_gpkg : str or pathlib.Path
        File path to an NHD flowlines GeoPackage (or other file read-able by geopandas.read_file).
        The function expects the flowlines data to contain an 'ID' column (which will be copied to
        'nwm_feature_id') and other columns such as 'FROMCOMID', 'to', 'order_', 'LengthKM', and 'Lake'.
    search : float
        Search distance in miles for the upstream/downstream trace.

    Returns
    -------
    geopandas.GeoDataFrame
        A new GeoDataFrame based on joined_gdf with additional columns from the nearest NHD flowline
        and two new list-columns:
          - 'upstream_nwm_features': list of nwm_feature_id values in the upstream trace (within search miles)
          - 'downstream_nwm_features': list of nwm_feature_id values in the downstream trace (within search miles)
        Other columns from the flowlines (filtered to ['nwm_feature_id', 'FROMCOMID', 'to', 'order_', 'LengthKM', 'Lake', 'geometry'])
        are also included. A 'distance' column (distance to the matched flowline) is added by the spatial join.

    Behavior and notes
    ------------------
    - Reads the NHD flowlines file with geopandas.read_file, filters a set of expected columns and creates
      a 'nwm_feature_id' field from the flowlines 'ID' column.
    - Reprojects the flowlines to joined_gdf.crs before performing a nearest spatial join.
    - Uses geopandas.sjoin_nearest(..., max_distance=20) to find the nearest flowline for each input geometry.
      The max_distance value is interpreted in the units of the CRS of joined_gdf (e.g., meters for a CRS in meters).
    - Converts the provided search distance from miles to kilometers and uses trace_network(flowlines_gdf, start_id, trace_dist_km)
      to obtain upstream and downstream network traces for each matched flowline. The function trace_network must be available
      in the current module / namespace and is expected to return two iterables (upstream, downstream) of nwm_feature_id values.

    Requirements and warnings
    -------------------------
    - The input flowlines file must contain the columns expected by the function; otherwise KeyError will be raised.
    - The nearest-join max_distance is fixed at 20 (CRS units). If your data require a different tolerance, adapt the call.
    - The trace distance is applied in kilometers (search miles -> km conversion is performed internally).
    '''

    # Read in the NHD Flowlines (replacement for NWM_flow.gpkg)
    # TODO: Eventually could have this iterate a list of NHD flowlines files
    flowlines_gdf = gpd.read_file(nhd_flowlines_gpkg)
    flowlines_gdf['nwm_feature_id'] = flowlines_gdf['ID']
    columns_to_keep = ['nwm_feature_id', 'FROMCOMID', 'to', 'order_', 'LengthKM', 'Lake', 'geometry']
    flowlines_gdf = flowlines_gdf[columns_to_keep]
    
    print('Done reading in and filtering NHD flowlines.')
    
    # For each USGS site, get the NWM ID of the intersecting flowline
    flowlines_gdf = flowlines_gdf.to_crs(joined_gdf.crs)
    joined_gdf_with_streams = gpd.sjoin_nearest(
        joined_gdf,
        flowlines_gdf,
        how="inner",
        max_distance=20, 
        distance_col="distance"
    ).drop('index_right', axis=1)

    # Convert the trace from miles to km
    km_conversion =  1.60934
    trace_dist_km = search * km_conversion
    
    # Get nwm_feature_id for 8km upstream and 8km downstream
    upstream_trace_list, downstream_trace_list = [], []
    for index, row in joined_gdf_with_streams.iterrows():
    
        # Get upstream and downstream traces
        trace_up, trace_down = [], []
        trace_up, trace_down = trace_network(flowlines_gdf, row['nwm_feature_id'], trace_dist_km)
    
        upstream_trace_list.append(trace_up)
        downstream_trace_list.append(trace_down)
    
    # Add the upstream and downstream trace to the geodataframe
    joined_gdf_with_streams['upstream_nwm_features'] = upstream_trace_list
    joined_gdf_with_streams['downstream_nwm_features'] = downstream_trace_list
    
    print('Done getting the streamline IDs and upstream and downstream trace.')

    return joined_gdf_with_streams


def restructure_data_for_pkl(joined_gdf_with_streams):
    '''
    Restructure the data so it is the right format for saving the pkl files.
     
    Creates two structures suitable for serialization/pickling: a thresholds
    DataFrame (matching the shape expected by thresholds.pkl) and a list of
    metadata dictionaries (matching the shape expected by nwm_metafile.pkl).

    Parameters
    ----------
    joined_gdf_with_streams : pandas.DataFrame or geopandas.GeoDataFrame
        Input table where each row represents a station/stream feature enriched
        with threshold values and metadata. The function expects the following
        columns to be present (names are case-sensitive):
          - 'huc_cd'
          - 'stage_thresh_action', 'stage_thresh_minor', 'stage_thresh_moderate',
            'stage_thresh_major', 'stage_thresh_record', 'stage_thresh_units'
          - 'flow_thresh_action', 'flow_thresh_minor', 'flow_thresh_moderate',
            'flow_thresh_major', 'flow_thresh_record', 'flow_thresh_units'
          - 'source', 'wrds_timestamp', 'nws_lid'
          - 'nwm_feature_id', 'station_nm'
          - 'dec_lat_va', 'dec_long_va', 'dec_coord_datum_cd'
          - 'alt_va', 'alt_acy_va', 'alt_datum_cd', 'alt_meth_cd'
          - 'upstream_nwm_features', 'downstream_nwm_features'
          - 'order_'
        If any of these columns are missing, a KeyError will be raised.

    Returns
    -------
    tuple (pandas.DataFrame, list)
        - all_thresh_df : pandas.DataFrame
            Concatenated DataFrame of threshold rows. For each input row this
            function produces two threshold rows (one for 'stages' and one for
            'flows') with the following columns:
              - 'threshold_type' : {'stages', 'flows'}
              - 'huc'
              - 'low'
              - 'bankfull'
              - 'action'
              - 'flood'
              - 'minor'
              - 'moderate'
              - 'major'
              - 'record'
              - 'source'
              - 'wrds_timestamp'
              - 'nws_lid'
              - 'usgs_site_code'
              - 'units'
            Several fields are set to the literal string 'NA' when no value is
            provided by the input (e.g., 'low', 'bankfull', 'usgs_site_code').
        - metadata_dict_list : list of dict
            List of metadata dictionaries, one per input row. Each dictionary
            contains nested keys and values mapped from the input row, including:
              - 'identifiers' -> {'nws_lid', 'usgs_site_code', 'nwm_feature_id'}
              - 'nws_data' -> {'name', 'wfo', 'rfc', 'latitude', 'longitude',
                              'horizontal_datum_name', 'state', 'county'}
              - 'usgs_data' -> {'latitude', 'longitude', 'latlon_datum_name',
                               'state', 'altitude', 'alt_accuracy_code',
                               'alt_datum_code', 'alt_method_code', 'active'}
              - 'nws_preferred' -> {'latitude', 'longitude'}
              - 'usgs_preferred' -> {'latitude', 'longitude', 'latlon_datum_name'}
              - 'nrldb_timestamp' (string, 'NA' by default)
              - 'nwis_timestamp' (string, 'NA' by default)
              - 'upstream_nwm_features', 'downstream_nwm_features'
              - 'nwm_feature_data' -> {'stream_order'}
            Several fields are filled with the literal string 'NA' where the
            original implementation does not provide values (for example,
            'usgs_site_code', 'wfo', 'rfc', 'state', 'county', 'active',
            'nrldb_timestamp', 'nwis_timestamp').
    Notes
    -----
    - The function does not modify the input DataFrame in-place.
    - The returned DataFrame has a new, continuous integer index (ignore_index=True).
    - This function is intended to prepare data to match the shape/field names of
      two pickled artifacts: thresholds.pkl and nwm_metafile.pkl.
    - If the input contains multiple rows for the same station/feature, the output
      will contain corresponding multiple entries (no deduplication is performed).
    '''

    # Reformat the data so it matches thresholds.pkl
    all_thresh_df_list = []
    for index, row in joined_gdf_with_streams.iterrows():
        thresholds = [{'threshold_type': 'stages',
            'huc': row['huc_cd'],
            'low': 'NA',
            'bankfull': 'NA',
            'action': row['stage_thresh_action'],
            'flood': 'NA',
            'minor': row['stage_thresh_minor'],
            'moderate': row['stage_thresh_moderate'],
            'major': row['stage_thresh_major'],
            'record': row['stage_thresh_record'],
            'source': row['source'],
            'wrds_timestamp': row['wrds_timestamp'],
            'nws_lid': row['nws_lid'],
            'usgs_site_code': 'NA',
            'units': row['stage_thresh_units']},
            {'threshold_type': 'flows',
            'huc': row['huc_cd'],
            'low': 'NA',
            'bankfull': 'NA',
            'action': row['flow_thresh_action'],
            'flood': 'NA',
            'minor': row['flow_thresh_minor'],
            'moderate': row['flow_thresh_moderate'],
            'major': row['flow_thresh_major'],
            'record': row['flow_thresh_record'],
            'source': row['source'],
            'wrds_timestamp': row['wrds_timestamp'],
            'nws_lid': row['nws_lid'],
            'usgs_site_code': 'NA',
            'units': row['flow_thresh_units']}]
        
        thresholds_df = pd.DataFrame(thresholds)
        all_thresh_df_list.append(thresholds_df)

    all_thresh_df = pd.concat(all_thresh_df_list, ignore_index=True)

    # Reformat the data so it matches nwm_metafile.pkl
    metadata_dict_list = []
    for index, row in joined_gdf_with_streams.iterrows():
        metadata_dict_lid = {
            "identifiers": {
                "nws_lid": row['nws_lid'],
                "usgs_site_code": "NA",
                "nwm_feature_id": row['nwm_feature_id'],
            },
            "nws_data": { 
                "name": row['station_nm'],
                "wfo": 'NA',
                "rfc": 'NA',
                "latitude": row['dec_lat_va'],
                "longitude": row['dec_long_va'],
                "horizontal_datum_name": row['dec_coord_datum_cd'],
                "state": 'NA', #row['state_cd'],
                "county": 'NA', #row['county_cd'],
                },
            "usgs_data": {
                "latitude": row['dec_lat_va'],
                "longitude": row['dec_long_va'],
                "latlon_datum_name": row['dec_coord_datum_cd'],
                "state": 'NA', #row['state_cd'],
                "altitude": row['alt_va'],
                "alt_accuracy_code": row['alt_acy_va'],
                "alt_datum_code": row['alt_datum_cd'],
                "alt_method_code": row['alt_meth_cd'],
                "active": 'NA',
                },
            "nws_preferred": {
                "latitude": row['dec_lat_va'],
                "longitude": row['dec_long_va'],
            },
            "usgs_preferred": {
                "latitude": row['dec_lat_va'],
                "longitude": row['dec_long_va'],
                "latlon_datum_name": row['dec_coord_datum_cd'],
            },
            "nrldb_timestamp": 'NA',
            "nwis_timestamp": 'NA',
            "upstream_nwm_features": row['upstream_nwm_features'],
            "downstream_nwm_features": row['downstream_nwm_features'],
            "nwm_feature_data": {
                "stream_order": row['order_']}
        }
        metadata_dict_list.append(metadata_dict_lid)

    return all_thresh_df, metadata_dict_list


## MAIN ##
def mimic_wrds_data(site_thresholds_csv, usgs_data_txt, nhd_flowlines_gpkg,
                    env_file, workspace, label, search):

    '''
    Mimic WRDS data by assembling, joining, and saving site metadata, USGS observations,
    and threshold information so that outputs resemble those produced by WRDS.

    Parameters
    ----------
    site_thresholds_csv : str
            Path to a CSV containing site threshold information (used to determine thresholds_df).
    usgs_data_txt : str
            Path to a text file containing USGS station/observation data; consumed by read_format_usgs_data.
    nhd_flowlines_gpkg : str
            Path to a GeoPackage containing NHD flowlines; used to obtain the local projected CRS
            and for crosswalking/tracing stream networks.
    env_file : str
            Path to a .env file that must contain API_BASE_URL for metadata API calls.
    workspace : str
            Directory where output files (thresholds_{label}.pkl, metadata_{label}.pkl,
            joined_gdf_with_streams_{label}.gpkg) will be written.
    label : str
            Short label appended to output filenames to distinguish runs (e.g., 'guam_2025').
    search : int or dict or object
            Parameter forwarded to crosswalk_trace_flows that controls the search/trace behavior
            when crosswalking sites to NHD flowlines. (See crosswalk_trace_flows for expected type
            and semantics.)
    '''

    overall_start_time = datetime.now(timezone.utc)
    dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")

    print('================================')
    print('Starting processing to mimic WRDS data')
    print(f'{dt_string} (UTC)')
    print()

    # Environment
    DEFAULT_DATA_CRS = 'NAD83' # Default CRS for the USGS and WRDS data (will only be used if 
    # a CRS is not included in the available data) - Doesn't work if we use the local_proj_crs
    # because the data is stored in this more broadly applicable CRS?
    # TODO: Call this from somewhere?

    # Set up API URLs
    load_dotenv(env_file)
    API_BASE_URL = os.getenv('API_BASE_URL')
    if API_BASE_URL is None:
        raise ValueError(
            'API base url not found. Ensure inundation_mapping/tools/ has an .env file with the API_BASE_URL.'
        )

    # Read in and format the USGS data # TODO: Update with API call when I get that working
    usgs_data_gdf = read_format_usgs_data(usgs_data_txt, DEFAULT_DATA_CRS)

    # Download and format metadata for sites in the site thresholds CSV
    metadata_url = f'{API_BASE_URL}/metadata'   
    meta_gdf, thresholds_df = download_format_metadata(site_thresholds_csv, metadata_url, DEFAULT_DATA_CRS)

    # Get the local projected CRS from the NHD flowlines file
    nhd_flowlines_temp_gdf = gpd.read_file(nhd_flowlines_gpkg)
    local_proj_crs = nhd_flowlines_temp_gdf.crs.to_epsg()
    print(f'Using local projected CRS EPSG:{local_proj_crs} from NHD flowlines file.')

    # Project geospatial data to the local projected CRS for geoprocessing
    meta_gdf = meta_gdf.to_crs(epsg=local_proj_crs)
    usgs_data_gdf = usgs_data_gdf.to_crs(epsg=local_proj_crs)

    # NOTE: From here on out, the geoprocessing will take place in the local projected CRS
    # (gotten from the flowlines file) because it will allow for more accurate geoprocessing. 

    # Spatially join the metadata (lid), the USGS data (altitude and lat lon)
    joined_gdf = gpd.sjoin_nearest(
        meta_gdf,
        usgs_data_gdf,
        how="left",
        max_distance=10, 
        distance_col="distance"
    )

    # ... and then use the LID to join the thresholds to the table
    joined_gdf = pd.merge(joined_gdf, thresholds_df, on='nws_lid', how='left')
    joined_gdf = joined_gdf.drop('index_right', axis=1)

    # print('Joined WRDS metadata, USGS data, and thresholds data.')
    
    # Crosswalk the sites to the flowlines and calculate upstream/downstream trace
    joined_gdf_with_streams = crosswalk_trace_flows(joined_gdf, nhd_flowlines_gpkg, search)

    # Restructure the data so it is the right format for saving the pkl files    
    all_thresh_df, metadata_dict_list = restructure_data_for_pkl(joined_gdf_with_streams)

    # Save thresholds to pkl file
    threshold_output_pickle_path = os.path.join(workspace, f'thresholds_{label}.pkl') # TODO: Update to match outputs from download_process_wrds.py
    try:
        with open(threshold_output_pickle_path, 'wb') as f:
            pickle.dump(all_thresh_df, f)
        print(f"Saved thresholds to {threshold_output_pickle_path}")
    except Exception as e:
        print(f"Error saving pickle file {threshold_output_pickle_path}: {e}")

    # Save metadata to pkl file
    metadata_output_pickle_path = os.path.join(workspace, f'metadata_{label}.pkl') # TODO: Update to match outputs from download_process_wrds.py
    try:
        with open(metadata_output_pickle_path, 'wb') as f:
            pickle.dump(metadata_dict_list, f)
        print(f"Saved thresholds to {metadata_output_pickle_path}")
    except Exception as e:
        print(f"Error saving pickle file {metadata_output_pickle_path}: {e}")
    
    # Save joined geodataframe to file for debugging
    debug_output_path = os.path.join(workspace, f'joined_gdf_with_streams_{label}.gpkg')
    try:
        joined_gdf_with_streams.to_file(debug_output_path, driver='GPKG')
        print(f"Saved joined geodataframe to {debug_output_path}")
    except Exception as e:
        print(f"Error saving geodataframe file {debug_output_path}: {e}")

    overall_end_time = datetime.now(timezone.utc)
    dt_string = overall_end_time.strftime("%m/%d/%Y %H:%M:%S")
    time_duration = overall_end_time - overall_start_time

    print()
    print('Processing complete.')
    print(f"Total duration: {str(time_duration).split('.')[0]}") 
    print('================================')




if __name__ == '__main__':
    '''
    This script builds a local “mimic” of WRDS site metadata and thresholds that would
      typically by downloaded from the WRDS API.

    Steps:

    - GET ELEVATION FROM WRDS: reads and formats a USGS station text file and downloads NWS metadata from an API,
    - JOIN THRESHOLDS TO ELEVATIONS: joins metadata with USGS attributes and thresholds CSV,
    - CROSSWALKING, UPSTREAM/DOWNSTREAM TRACE: finds nearest NHD flowlines and traces upstream/downstream reaches by distance and stream order,
    - FORMATTING: reformats results into threshold and metadata structures, and saves them as pickle files.

    Inputs and requirements

    - site_thresholds_csv (required)
        Path to a CSV containing site threshold records. Required columns (exact names):
            nws_lid, source, wrds_timestamp,
            flow_thresh_action, flow_thresh_minor, flow_thresh_moderate, flow_thresh_major,
            flow_thresh_record, flow_thresh_units,
            stage_thresh_action, stage_thresh_minor, stage_thresh_moderate, stage_thresh_major,
            stage_thresh_record, stage_thresh_units
        Format: comma-separated UTF-8 CSV. Each row represents thresholds for one NWS LID.

    - usgs_data_txt (required) # TODO: Add description on how to download
        Path to a USGS station text file (tab-separated) exported from USGS. The reader in this
        script skips the first 43 rows and expects the following columns to exist:
            agency_cd, site_no, station_nm, site_tp_cd, dec_lat_va, dec_long_va,
            coord_meth_cd, coord_acy_cd, coord_datum_cd, dec_coord_datum_cd,
            district_cd, state_cd, county_cd, country_cd,
            alt_va, alt_meth_cd, alt_acy_va, alt_datum_cd, huc_cd
        Latitude/longitude must be decimal degrees. A horizontal datum should be present in
        dec_coord_datum_cd; if missing, DEFAULT_DATA_CRS ('NAD83') is used.

    - nhd_flowlines_gpkg (required)
        Path to a GeoPackage (or other file readable by geopandas) containing NHD flowlines.
        The flowline layer must include these fields:
            ID, FROMCOMID, to, order_, LengthKM, Lake, geometry
        The file will be reprojected to the provided local_proj_crs before spatial operations.

    - env_file (optional)
        Path to a .env file containing the environment variable API_BASE_URL used to fetch
        additional metadata. Default: /data/config/fim_enviro_values.env. If API_BASE_URL is
        missing the script will raise a ValueError.

    - workspace (optional)
        Directory where output pickle files will be written. Must be writable by the user.
        Default: /data/inputs/wrds/manual_input # TODO: Update default path?

    - label (optional)
        Short label string appended into output filenames (metadata_<label>.pkl,
        thresholds_<label>.pkl). If omitted, filenames use the label argument as provided. # TODO: Update label desc.

    - search (optional)
        Integer search distance in miles for upstream/downstream tracing (default: 5 miles).
        Converted internally to kilometers (1 mile = 1.60934 km).

    Notes and assumptions:
    - The code assumes a single horizontal datum for the USGS input file and the WRDS metadata;
        when missing, DEFAULT_DATA_CRS ('NAD83') is used and a warning is printed.
    '''
    
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Builds a local “mimic” of WRDS site metadata and thresholds'
        'that would typically by downloaded from the WRDS API.'
    )

    parser.add_argument(
        '-t',
        '--site-thresholds-csv',
        help='# a CSV with the LID, and any flow or stage thresholds available w/ units.'
         'e.g. /home/emily.deardorff/notebooks/guam_thresholds.csv'
         'Columns required:     nws_lid, source, wrds_timestamp, flow_thresh_action,'
         'flow_thresh_minor, flow_thresh_moderate, flow_thresh_major,'
         'flow_thresh_record, flow_thresh_units, stage_thresh_action,'
         'stage_thresh_minor, stage_thresh_moderate, stage_thresh_major,'
         'stage_thresh_record, stage_thresh_units',
        required=True,
    ) 


    # TODO: remove as input when I get the USGS API metadata download working
    parser.add_argument(
        '-u',
        '--usgs-data-txt',
        help='a CSV downloaded from the USGS site' # TODO: Add description, incl. where we need to get this data from (and how). Do we want to have a template CSV in this folder as well?
        'e.g. /home/emily.deardorff/notebooks/usgs_data_guam.txt',
        required=True,
    )

    parser.add_argument(
        '-f',
        '--nhd-flowlines-gpkg',
        help='NHD flowlines file for the area.' # TODO: Add description, incl. where we need to get this data from 
        'e.g. /data/inputs/nhdplus/Guam_6637/NHDFlowline_Guam_6637.gpkg',
        required=True,
    )

    # parser.add_argument(
    #     '-c',
    #     '--local-proj-crs',
    #     help='Local projected CRS for the area. Required for distance '
    #     'calculations. This is the CRS that is used for the coordinates '
    #     'on WRDS, but you might have to guess if the data is incomplete.'
    #     'e.g. 3993 for Guam', 
    #     type=int,
    #     required=True,
    # )
    # The Oct '25 Guam CatFIM run used EPSG:3993, but I think EPSG:6637 is better for Guam overall
    # because it is NAD83-based instead of WGS84-based. Since that is the case, we can pull
    # the value from the NHD flowlines file instead of requiring user input.

    parser.add_argument(
        '-e',
        '--env-file',
        help='OPTIONAL: Docker mount path to the environment file.'
        'default = /data/config/fim_enviro_values.env',
        required=False,
        default='/data/config/fim_enviro_values.env',
    )

    parser.add_argument('-w',
        '--workspace',
        help='OPTIONAL: Workspace where all outputs will be saved.',
        required=False,
        default = '/data/inputs/wrds/manual_input' # TODO: decide on a default save location
    )

    parser.add_argument('-l',
        '--label',
        help='OPTIONAL: Label for filenames. Stucture will be metadata_<label>_ddmmyy.pkl and thresholds_<label>_ddmmyy.pkl).',
        required=False
    )

    parser.add_argument(
        '-s',
        '--search',
        help='OPTIONAL: Upstream and downstream search in miles. Defaults to 5 miles.',
        required=False,
        type=int,
        default=5,
    )

    args = vars(parser.parse_args())

    # Main function call
    mimic_wrds_data(**args)

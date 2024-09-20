#!/usr/bin/env python3
import argparse
import os
import pathlib
import sys
import traceback
from collections import defaultdict
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from dotenv import load_dotenv
from tools_shared_functions import (
    aggregate_wbd_hucs,
    flow_data,
    get_datum,
    get_metadata,
    get_nwm_segs,
    get_rating_curve,
    get_thresholds,
    mainstem_nwm_segs,
    ngvd_to_navd_ft,
    process_extent,
    process_grid,
    raster_to_feature,
    select_grids,
)

from utils.shared_variables import PREP_PROJECTION, VIZ_PROJECTION


def get_env_paths():
    load_dotenv()
    # import variables from .env file
    API_BASE_URL = os.getenv("API_BASE_URL")
    EVALUATED_SITES_CSV = os.getenv("EVALUATED_SITES_CSV")
    WBD_LAYER = os.getenv("WBD_LAYER")
    return API_BASE_URL, EVALUATED_SITES_CSV, WBD_LAYER


########################################################
# Preprocess AHPS NWS
# This script will work on NWS AHPS fim data (some assumptions made about the data structure).
# Provide a source directory path (source_dir) where all NWS AHPS FIM data is located. NWS source data was previously downloaded and extracted. Some data is buried through several layers of subfolders in the source data. In general, the downloaded datasets were unzipped and starting from where the folder name was the AHPS code, this was copied and pasted into a new directory which is the source_dir.
# Provide a destination directory path (destination) which is where all outputs are located.
# Provide a reference raster path.
########################################################
# source_dir = Path(r'path/to/nws/downloads')
# destination = Path(r'path/to/preprocessed/nws/data')
# reference_raster= Path(r'path/to/reference raster')


def preprocess_nws(source_dir, destination, reference_raster):
    source_dir = Path(source_dir)
    destination = Path(destination)
    reference_raster = Path(reference_raster)
    metadata_url = f'{API_BASE_URL}/metadata'
    threshold_url = f'{API_BASE_URL}/nws_threshold'
    rating_curve_url = f'{API_BASE_URL}/rating_curve'
    log_file = destination / 'log.txt'

    # Write a run-time log file
    destination.mkdir(parents=True, exist_ok=True)
    log_file = destination / 'log.txt'
    f = open(log_file, 'a+')

    # Define distance (in miles) to search for nwm segments
    nwm_ds_search = 10
    nwm_us_search = 10
    # The NWS data was downloaded and unzipped. The ahps folder (with 5 digit code as folder name) was cut and pasted into a separate directory. So the ahps_codes iterates through that parent directory to get all of the AHPS codes that have data.
    ahps_codes = [i.name for i in source_dir.glob('*') if i.is_dir() and len(i.name) == 5]
    # Get mainstems NWM segments
    # Workaround for sites in 02030103 and 02030104, many are not rfc_forecast_point = True
    list_of_sites = pd.read_csv(EVALUATED_SITES_CSV)['Total_List'].to_list()
    print("getting mainstem nwm segments")
    ms_segs = mainstem_nwm_segs(metadata_url, list_of_sites)

    # Find depth grid subfolder
    for code in ahps_codes:
        f.write(f'{code} : Processing\n')
        print(f'processing {code}')
        # 'mnda2' is in Alaska outside of NWM domain.
        if code in ['mnda2']:
            f.write(f'{code} : skipping because outside of NWM domain\n')
            continue

        # Get metadata of site and search for NWM segments x miles upstream/x miles downstream
        select_by = 'nws_lid'
        selector = [code]
        metadata_list, metadata_df = get_metadata(
            metadata_url,
            select_by,
            selector,
            must_include=None,
            upstream_trace_distance=nwm_us_search,
            downstream_trace_distance=nwm_ds_search,
        )
        metadata = metadata_list[0]

        # Assign huc to site using FIM huc layer.
        dictionary, out_gdf = aggregate_wbd_hucs(metadata_list, Path(WBD_LAYER), retain_attributes=False)
        [huc] = list(dictionary.keys())

        # Get thresholds for action, minor, moderate, major. If no threshold data present, exit.
        # The threshold flows source will dictate what rating curve (and datum) to use as it uses a decision tree (USGS priority then NRLDB)
        # In multiple instances a USGS ID is given but then no USGS rating curve or in some cases no USGS datum is supplied.
        select_by = 'nws_lid'
        selector = code
        stages, flows = get_thresholds(threshold_url, select_by, selector, threshold='all')

        # Make sure at least one valid threshold is supplied from WRDS.
        threshold_categories = ['action', 'minor', 'moderate', 'major']
        if not any([stages[threshold] for threshold in threshold_categories]):
            f.write(f'{code} : skipping because no threshold stages available\n')
            continue

        # determine source of interpolated threshold flows, this will be the rating curve that will be used.
        rating_curve_source = flows.get('source')
        if rating_curve_source is None:
            f.write(f'{code} : skipping because no rating curve source\n')
            continue

        # Workaround for "bmbp1" where the only valid datum is from NRLDB (USGS datum is null). Modifying rating curve source will influence the rating curve and datum retrieved for benchmark determinations.
        if code == 'bmbp1':
            rating_curve_source = 'NRLDB'
        # Get the datum and adjust to NAVD if necessary.
        nws, usgs = get_datum(metadata)
        datum_data = {}
        if rating_curve_source == 'USGS Rating Depot':
            datum_data = usgs
        elif rating_curve_source == 'NRLDB':
            datum_data = nws

        # If datum not supplied, skip to new site
        datum = datum_data.get('datum', None)
        if datum is None:
            f.write(f'{code} : skipping because site is missing datum\n')
            continue

        # Custom workaround these sites have faulty crs from WRDS. CRS needed for NGVD29 conversion to NAVD88
        # USGS info indicates NAD83 for site: bgwn7, fatw3, mnvn4, nhpp1, pinn4, rgln4, rssk1, sign4, smfn7, stkn4, wlln7
        # Assumed to be NAD83 (no info from USGS or NWS data): dlrt2, eagi1, eppt2, jffw3, ldot2, rgdt2
        if code in [
            'bgwn7',
            'dlrt2',
            'eagi1',
            'eppt2',
            'fatw3',
            'jffw3',
            'ldot2',
            'mnvn4',
            'nhpp1',
            'pinn4',
            'rgdt2',
            'rgln4',
            'rssk1',
            'sign4',
            'smfn7',
            'stkn4',
            'wlln7',
        ]:
            datum_data.update(crs='NAD83')

        # Workaround for bmbp1; CRS supplied by NRLDB is mis-assigned (NAD29) and is actually NAD27. This was verified by converting USGS coordinates (in NAD83) for bmbp1 to NAD27 and it matches NRLDB coordinates.
        if code == 'bmbp1':
            datum_data.update(crs='NAD27')
        # Custom workaround these sites have poorly defined vcs from WRDS. VCS needed to ensure datum reported in NAVD88. If NGVD29 it is converted to NAVD88.
        # bgwn7, eagi1 vertical datum unknown, assume navd88
        # fatw3 USGS data indicates vcs is NAVD88 (USGS and NWS info agree on datum value).
        # wlln7 USGS data indicates vcs is NGVD29 (USGS and NWS info agree on datum value).
        if code in ['bgwn7', 'eagi1', 'fatw3']:
            datum_data.update(vcs='NAVD88')
        elif code == 'wlln7':
            datum_data.update(vcs='NGVD29')

        # Adjust datum to NAVD88 if needed
        if datum_data.get('vcs') in ['NGVD29', 'NGVD 1929', 'NGVD,1929']:
            # Get the datum adjustment to convert NGVD to NAVD. Sites not in contiguous US are previously removed otherwise the region needs changed.
            datum_adj_ft = ngvd_to_navd_ft(datum_info=datum_data, region='contiguous')
            datum88 = round(datum + datum_adj_ft, 2)
        else:
            datum88 = datum

        # get entire rating curve, same source as interpolated threshold flows (USGS Rating Depot first then NRLDB rating curve).
        if rating_curve_source == 'NRLDB':
            site = [code]
        elif rating_curve_source == 'USGS Rating Depot':
            site = [metadata.get('identifiers').get('usgs_site_code')]

        rating_curve = get_rating_curve(rating_curve_url, site)

        # Add elevation fields to rating curve
        # Add field with vertical coordinate system
        vcs = datum_data['vcs']
        if not vcs:
            vcs = 'Unspecified, Assumed NAVD88'
        rating_curve['vcs'] = vcs

        # Add field with original datum
        rating_curve['datum'] = datum

        # If VCS is NGVD29 add rating curve elevation (in NGVD) as well as the NAVD88 datum
        if vcs in ['NGVD29', 'NGVD 1929']:
            # Add field with raw elevation conversion (datum + stage)
            rating_curve['elevation_ngvd29'] = rating_curve['stage'] + datum
            # Add field with adjusted NAVD88 datum
            rating_curve['datum_navd88'] = datum88

        # Add field with NAVD88 elevation
        rating_curve['elevation_navd88'] = rating_curve['stage'] + datum88

        # Search through ahps directory find depth grid folder
        parent_path = source_dir / code

        # Work around for bgwn7 and smit2 where grids were custom created from polygons (bgwn7-no grids, smit2 - no projection and applying projection from polygons had errors)
        if code in ['bgwn7', 'smit2']:
            [grids_dir] = [directory for directory in parent_path.glob('*custom*') if directory.is_dir()]
        else:
            # Find the directory containing depth grids. Assumes only one directory will be returned.
            [grids_dir] = [directory for directory in parent_path.glob('*depth_grid*') if directory.is_dir()]

        # Get grids (all NWS ESRI grids were converted to Geotiff)
        grid_paths = [grids for grids in grids_dir.glob('*.tif*') if grids.suffix in ['.tif', '.tiff']]
        grid_names = [name.stem for name in grid_paths]
        # If grids are present, interpolate a flow for the grid.
        if grid_paths:
            # Construct Dataframe containing grid paths, names, datum, code
            df = pd.DataFrame({'code': code, 'path': grid_paths, 'name': grid_names, 'datum88': datum88})
            # Determine elevation from the grid name. All elevations are assumed to be in NAVD88 based on random inspection of AHPS inundation website layers.
            df['elevation'] = (
                df['name'].str.replace('elev_', '', case=False).str.replace('_', '.').astype(float)
            )
            # Add a stage column using the datum (in NAVD88). Stage is rounded to the nearest 0.1 ft.
            df['stage'] = round(df['elevation'] - df['datum88'], 1)
            # Sort stage in ascending order
            df.sort_values(by='elevation', ascending=True, inplace=True)
            # Interpolate flow from the rating curve using the elevation_navd88 values, if value is above or below the rating curve assign nan.
            df['flow'] = np.interp(
                df['elevation'],
                rating_curve['elevation_navd88'],
                rating_curve['flow'],
                left=np.nan,
                right=np.nan,
            )
            # Assign flow source to reflect interpolation from rc
            df['flow_source'] = f'interpolated from {rating_curve_source} rating curve'

        else:
            f.write(f'{code} : Site has no benchmark grids\n')

        # Select the appropriate threshold grid for evaluation. Using the supplied threshold stages and the calculated map stages.
        grids, grid_flows = select_grids(df, stages, datum88, 1.1)

        # workaroud for bigi1 and eag1 which have gridnames based on flows (not elevations)
        if code in ['eagi1', 'bigi1']:
            # Elevation is really flows (due to file names), assign this to stage
            df['flow'] = df['elevation']
            df['stage'] = df['elevation']
            # Select grids using flows
            grids, grid_flows = select_grids(df, flows, datum88, 500)
            f.write(f'{code} : Site workaround grids names based on flows not elevation\n')

        # Obtain NWM segments that are on ms to apply flows
        segments = get_nwm_segs(metadata)
        site_ms_segs = set(segments).intersection(ms_segs)
        segments = list(site_ms_segs)

        # Write out boolean benchmark raster and flow file
        try:
            # for each threshold
            for i in ['action', 'minor', 'moderate', 'major']:
                # Obtain the flow and grid associated with threshold.
                flow = grid_flows[i]
                grid = grids[i]
                extent = grids['extent']
                # Make sure that flow and flow grid are valid
                if grid not in ['No Map', 'No Threshold', 'No Flow']:
                    # define output directory (to be created later)
                    outputdir = destination / huc / code / i

                    # Create Binary Grids, first create domain of analysis, then create binary grid

                    # Domain extent is largest floodmap in the static library WITH holes filled
                    filled_domain_raster = outputdir.parent / f'{code}_filled_orig_domain.tif'

                    # Open benchmark data as a rasterio object.
                    benchmark = rasterio.open(grid)
                    benchmark_profile = benchmark.profile

                    # Open extent data as rasterio object
                    domain = rasterio.open(extent)
                    domain_profile = domain.profile

                    # if grid doesn't have CRS, then assign CRS using a polygon from the ahps inundation library
                    if not benchmark.crs:
                        # Obtain crs of the first polygon inundation layer associated with ahps code. Assumes only one polygon* subdirectory and assumes the polygon directory has at least 1 inundation shapefile.
                        [ahps_polygons_directory] = [
                            directory for directory in parent_path.glob('*polygon*') if directory.is_dir()
                        ]
                        shapefile_path = list(ahps_polygons_directory.glob('*.shp'))[0]
                        shapefile = gpd.read_file(shapefile_path)
                        # Update benchmark and domain profiles with crs from shapefile. Assumed that benchmark/extent have same crs.
                        benchmark_profile.update(crs=shapefile.crs)
                        domain_profile.update(crs=shapefile.crs)

                    # Create a domain raster if it does not exist.
                    if not filled_domain_raster.exists():
                        # Domain should have donut holes removed
                        process_extent(domain, domain_profile, output_raster=filled_domain_raster)

                    # Open domain raster as rasterio object
                    filled_domain = rasterio.open(filled_domain_raster)
                    filled_domain_profile = filled_domain.profile

                    # Create the binary benchmark raster
                    boolean_benchmark, boolean_profile = process_grid(
                        benchmark, benchmark_profile, filled_domain, filled_domain_profile, reference_raster
                    )

                    # Output binary benchmark grid and flow file to destination
                    outputdir.mkdir(parents=True, exist_ok=True)
                    output_raster = outputdir / (f'ahps_{code}_huc_{huc}_extent_{i}.tif')

                    with rasterio.Env():
                        with rasterio.open(output_raster, 'w', **boolean_profile) as dst:
                            dst.write(boolean_benchmark, 1)

                    # Close datasets
                    domain.close()
                    filled_domain.close()
                    benchmark.close()

                    # Create the guts of the flow file.
                    flow_info = flow_data(segments, flow)
                    # Write out the flow file to csv
                    output_flow_file = outputdir / (f'ahps_{code}_huc_{huc}_flows_{i}.csv')
                    flow_info.to_csv(output_flow_file, index=False)

        except Exception as e:
            f.write(f'{code} : Error preprocessing benchmark\n{repr(e)}\n')
            f.write(traceback.format_exc())
            f.write('\n')
            print(traceback.format_exc())
        # Process extents, only create extent if ahps code subfolder is present in destination directory.
        ahps_directory = destination / huc / code
        if ahps_directory.exists():
            # Delete original filled domain raster (it is an intermediate file to create benchmark data)
            orig_domain_grid = ahps_directory / f'{code}_filled_orig_domain.tif'
            orig_domain_grid.unlink()
            # Create domain shapefile from any benchmark grid for site (each benchmark has domain footprint, value = 0).
            filled_extent = list(ahps_directory.rglob('*_extent_*.tif'))[0]
            domain_gpd = raster_to_feature(grid=filled_extent, profile_override=False, footprint_only=True)
            domain_gpd['nws_lid'] = code
            domain_gpd.to_file(ahps_directory / f'{code}_domain.shp')

            # Populate attribute information for site
            grids_attributes = pd.DataFrame(data=grids.items(), columns=['magnitude', 'path'])
            flows_attributes = pd.DataFrame(data=grid_flows.items(), columns=['magnitude', 'grid_flow_cfs'])
            threshold_attributes = pd.DataFrame(data=stages.items(), columns=['magnitude', 'magnitude_stage'])
            # merge dataframes
            attributes = grids_attributes.merge(flows_attributes, on='magnitude')
            attributes = attributes.merge(threshold_attributes, on='magnitude')
            attributes = attributes.merge(df[['path', 'stage', 'elevation', 'flow_source']], on='path')
            # Strip out sensitive paths and convert magnitude stage to elevation
            attributes['path'] = attributes['path'].apply(lambda x: Path(x).name)
            attributes['magnitude_elev_navd88'] = (
                (datum88 + attributes['magnitude_stage']).astype(float).round(1)
            )
            # Add general site information
            attributes['nws_lid'] = code
            attributes['wfo'] = metadata['nws_data']['wfo']
            attributes['rfc'] = metadata['nws_data']['rfc']
            attributes['state'] = metadata['nws_data']['state']
            attributes['huc'] = huc
            # Rename and Reorder columns
            attributes.rename(
                columns={
                    'path': 'grid_name',
                    'flow_source': 'grid_flow_source',
                    'stage': 'grid_stage',
                    'elevation': 'grid_elev_navd88',
                },
                inplace=True,
            )
            attributes = attributes[
                [
                    'nws_lid',
                    'wfo',
                    'rfc',
                    'state',
                    'huc',
                    'magnitude',
                    'magnitude_stage',
                    'magnitude_elev_navd88',
                    'grid_name',
                    'grid_stage',
                    'grid_elev_navd88',
                    'grid_flow_cfs',
                    'grid_flow_source',
                ]
            ]
            # Save attributes to csv
            attributes.to_csv(ahps_directory / f'{code}_attributes.csv', index=False)

            # Write the rating curve to a file
            rating_curve_output = ahps_directory / (f'{code}_rating_curve.csv')
            rating_curve['lat'] = datum_data['lat']
            rating_curve['lon'] = datum_data['lon']
            rating_curve.to_csv(rating_curve_output, index=False)

            # Write the interpolated flows to file
            df_output = ahps_directory / (f'{code}_interpolated_flows.csv')
            df.to_csv(df_output, index=False)

        else:
            f.write(f'{code} : Unable to evaluate site, missing all flows\n')

    # Close log file.
    f.close()

    # Combine all attribute files
    attribute_files = list(destination.rglob('*_attributes.csv'))
    all_attributes_list = []
    for i in attribute_files:
        attribute_df = pd.read_csv(i, dtype={'huc': str})
        all_attributes_list.append(attribute_df)
    all_attributes_df = pd.concat(all_attributes_list)

    if not all_attributes_df.empty:
        all_attributes_df.to_csv(destination / 'attributes.csv', index=False)
    return


if __name__ == '__main__':

    # sample:
    # python  foss_fim/data/nws/preprocess_ahps_nws.py -s inputPath    -d outputPath    -r  referenceRaster

    # inputPath: path to a directory containing a folder for each gage (gage/folder names must be 5 character). Each gage folder must
    # have a sub-folder named "depth_grid" with depth grids as TIFF files prepared as below:
    #     - Collect/Download the grid depth dataset, typically available as ESRI gdb.
    #     - Use arcpy (or ArcGIS pro ) to convert the grid depths (in ESRI gdb) into TIFF file. Make sure the TIFF files have crs

    # referenceRaster: path to an arbitrary output TIFF file from a FIM run. Note that for a site in CONUS, this referenceRaster
    # must be from a FIM run for a CONUS HUC, and for a site in Alaska, this referenceRaster must be from a Alaska HUC FIM run.

    # Two notes:
    # 1- Sites in CONUS and Alaska cannot be mixed in a single run. Separate runs should be done for Alaska sites and CONUS sites.
    # 2- Before running this script, add the name of the new site(s) to the  '/data/inputs/ahps_sites/evaluated_ahps_sites.csv' file

    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Create preprocessed USGS benchmark datasets at AHPS locations.'
    )
    parser.add_argument(
        '-s', '--source_dir', help='Workspace where all source data is located.', required=True
    )
    parser.add_argument('-d', '--destination', help='Directory where outputs are to be stored', required=True)
    parser.add_argument(
        '-r', '--reference_raster', help='reference raster used for benchmark raster creation', required=True
    )
    args = vars(parser.parse_args())

    # Run get_env_paths and static_flow_lids
    API_BASE_URL, EVALUATED_SITES_CSV, WBD_LAYER = get_env_paths()
    preprocess_nws(**args)

import argparse
import datetime as dt
import os
import traceback
from contextlib import redirect_stderr, redirect_stdout

import geopandas as gpd
from dotenv import load_dotenv
from esri import ESRI_REST
from shapely import Polygon

from src.utils.shared_functions import FIM_Helpers as fh
from src.utils.shared_functions import (
    get_huc_vars,
    read_huc_file_list_or_array_of_hucs,
    run_with_mp,
    setup_mp_file_logger,
)


def load_wbd(huc_list):
    """
    Load wbd data.

    Parameters
    ----------
    huc : list
        List of huc8
    Returns
    tuple
        (wbd_conus, wbd_alaska, wbd_guam, wbd_american_samoa)
    """
    srcDir = os.getenv('srcDir')
    load_dotenv(f'{srcDir}/bash_variables.env')
    input_WBD_gdb = os.getenv('input_WBD_gdb')
    input_WBD_gdb_Alaska = os.getenv('input_WBD_gdb_Alaska')  # alaska
    input_WBD_gdb_Guam = os.getenv('input_WBD_gdb_Guam')  # guam
    input_WBD_gdb_AmericanSamoa = os.getenv('input_WBD_gdb_AmericanSamoa')  # american samoa
    wbd_conus = None
    wbd_alaska = None
    wbd_guam = None
    wbd_american_samoa = None
    # Check if any huc8 is in AK
    has_alaska = any(huc.startswith('19') for huc in huc_list)
    has_guam = any(huc == '22010000' for huc in huc_list)
    has_american_samoa = any(huc == '22030001' for huc in huc_list)

    # Load conus wbd if needed
    if any(not huc.startswith('19') for huc in huc_list):
        if os.path.exists(input_WBD_gdb):
            wbd_conus = gpd.read_file(input_WBD_gdb)

    # Load AK wbd if needed
    if has_alaska and os.path.exists(input_WBD_gdb_Alaska):
        wbd_alaska = gpd.read_file(input_WBD_gdb_Alaska)

    # Load Guam wbd if needed
    if has_guam and os.path.exists(input_WBD_gdb_Guam):
        wbd_guam = gpd.read_file(input_WBD_gdb_Guam)

    # Load American Samoa wbd if needed
    if has_american_samoa and os.path.exists(input_WBD_gdb_AmericanSamoa):
        wbd_american_samoa = gpd.read_file(input_WBD_gdb_AmericanSamoa)

    return wbd_conus, wbd_alaska, wbd_guam, wbd_american_samoa


def process_nfhl(
    nfhl_layer: gpd.GeoDataFrame,
    nfhl_label: str,
    huc: str,
    out_file: str,
    file_logger,
    fill_holes: bool = None,
):
    """
    Process NFHL layer and save to GPKG

    Parameters
    ----------
    nfhl_layer : GeoDataFrame
        The NFHL layer to process
    nfhl_label : str
        The label for the NFHL layer
    huc : str
        The HUC8 code
    out_file : str
        The output file path
    file_logger : logging.Logger
        Logger for file output
    fill_holes : bool
        Whether to fill holes in the geometries (default is None, which means no filling)
    """
    file_logger.info(f"Processing NFHL layer: {nfhl_label} for HUC {huc}")

    if not nfhl_layer.empty:
        nfhl_dissolved = nfhl_layer.dissolve()
        nfhl_exploded = nfhl_dissolved.explode().reset_index(drop=True)

        if fill_holes:
            new_geoms = [
                Polygon(geom.exterior)
                for geom in nfhl_exploded.geometry
                if geom is not None and geom.is_valid
            ]
            nfhl_exploded.geometry = new_geoms

        nfhl_exploded = nfhl_exploded[~nfhl_exploded.geometry.isna()]
        nfhl_final = nfhl_exploded.dissolve().reset_index(drop=True)
        nfhl_final = nfhl_final.dropna(axis=1, how='all')

        # Save layer to GPKG
        nfhl_final.to_file(out_file, layer=nfhl_label, index=False, driver='GPKG')

    else:
        file_logger.warning(f"No {nfhl_label} zones for HUC {huc}")


def download_nfhl(
    huc,
    out_file,
    wbd_conus,
    wbd_alaska,
    wbd_guam,
    wbd_american_samoa,
    geometry_type,
    file_logger,
    screen_queue,
    task_id,
):
    """
    Download the NFHL flood hazard zones for a given HUC8

    Parameters
    ----------
    huc : str
        The HUC8 to query
    output_folder : str
        The folder to save the output file
    wbd_conus : GeoDataFrame
        wbd for conus
    wbd_alaska : GeoDataFrame
        wbd for alaska
    wbd_guam : GeoDataFrame
        wbd for guam
    geometry_type: str
        the geometry Type for the query
    file_logger : logging.Logger
        Logger for file output
    screen_queue : multiprocessing.Queue
        Queue for screen updates
    task_id : str
        Task identifier (HUC8)
    """

    file_logger.info(f"Started processing HUC {task_id}")
    screen_queue.put(f"Started processing HUC {task_id}")
    try:
        # Select appropriate wbd and crs
        if huc.startswith('19'):
            wbd = wbd_alaska
        elif huc == '22010000':
            wbd = wbd_guam
        elif huc == '22030001':
            wbd = wbd_american_samoa
        else:
            wbd = wbd_conus
        geometryCRS = int(get_huc_vars(huc)['crs'].split(':')[1])

        if wbd is None:
            file_logger.error(f'No wbd available for huc {huc}')
            return 2, [False]

        def __get_nfhl_flood_hazard_zones(
            huc, wbd, out_file, geometryType='esriGeometryEnvelope', geometryCRS=geometryCRS
        ):
            """
            Query the NFHL flood hazard zones for a given HUC8

            Parameters
            ----------
            huc : str
                The HUC8 code to query
            wbd : GeoDataFrame
                The WBD GeoDataFrame
            out_file : str
                The output file path
            geometryType : str
                The geometry type to use for the query
            geometryCRS : int
                The coordinate reference system to use for the query
            """

            if not os.path.exists(out_file):
                polygon = wbd.loc[wbd.HUC8 == huc]
                minx, miny, maxx, maxy = polygon.geometry.bounds.values[0]

                geometry = {
                    "xmin": minx,
                    "ymin": miny,
                    "xmax": maxx,
                    "ymax": maxy,
                    "spatialReference": {"wkid": geometryCRS},
                }

                geometryType = 'esriGeometryEnvelope'

                geometry = str(geometry)

                # Filter for 100-year (A, V zones): FLD_ZONE LIKE 'A%' OR FLD_ZONE LIKE 'V%'
                # and 500-year (X): FLD_ZONE LIKE 'X' AND ZONE_SUBTY = '0.2 PCT ANNUAL CHANCE FLOOD HAZARD'
                where_clause = (
                    "(FLD_ZONE LIKE 'A%' OR FLD_ZONE LIKE 'V%') OR"
                    "(FLD_ZONE LIKE 'X' AND ZONE_SUBTY = '0.2 PCT ANNUAL CHANCE FLOOD HAZARD')"
                )

                with open(os.devnull, 'w') as devnull:
                    with redirect_stdout(devnull), redirect_stderr(devnull):
                        nfhl_df = ESRI_REST.query(
                            "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query",
                            f="json",
                            where=where_clause,
                            returnGeometry="true",
                            outFields="*",
                            outSR=str(geometryCRS),
                            geometryType=geometryType,
                            geometry=geometry,
                            resultRecordCount=100,
                            geometryPrecision=1,
                            maxAllowableOffset=1,
                        )

                        nfhl_availability_df = ESRI_REST.query(
                            "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/0/query",
                            f="json",
                            where="1=1",
                            returnGeometry="true",
                            outFields="*",
                            outSR=str(geometryCRS),
                            geometryType=geometryType,
                            geometry=geometry,
                            resultRecordCount=100,
                            geometryPrecision=1,
                            maxAllowableOffset=1,
                        )

                if nfhl_df.empty:
                    file_logger.error(f"No NFHL data retrieved for HUC {huc}")
                    return False
                if nfhl_availability_df.empty:
                    file_logger.error(f"No NFHL availability data retrieved for HUC {huc}")
                    return False

                # Clean the geometries to remove self-intersections
                nfhl_df['geometry'] = nfhl_df['geometry'].make_valid()
                nfhl_df = gpd.clip(nfhl_df, polygon)
                # Filter polygons and multipolygons
                # print(nfhl_df.geom_type.value_counts())
                nfhl_df = nfhl_df[nfhl_df.geom_type.isin(['Polygon', 'MultiPolygon'])]
                # Explode multipolygon geometries to single polygons
                nfhl_df = nfhl_df.explode(index_parts=True).reset_index(drop=True)

                # Save 100-year data
                nfhl_100 = nfhl_df[nfhl_df['FLD_ZONE'].str.startswith(('A', 'V'))]
                process_nfhl(nfhl_100, '100_year', huc, out_file, file_logger, fill_holes=True)

                # Save 500-year data
                nfhl_500 = nfhl_df[
                    (nfhl_df['FLD_ZONE'] == 'X')
                    & (nfhl_df['ZONE_SUBTY'] == '0.2 PCT ANNUAL CHANCE FLOOD HAZARD')
                ]
                process_nfhl(nfhl_500, '500_year', huc, out_file, file_logger, fill_holes=True)

                # Clean the geometries to remove self-intersections
                nfhl_availability_df['geometry'] = nfhl_availability_df['geometry'].make_valid()
                # nfhl_availability_df = gpd.clip(nfhl_availability_df, polygon)
                # Filter polygons and multipolygons
                # print(nfhl_df.geom_type.value_counts())
                nfhl_availability_df = nfhl_availability_df[
                    nfhl_availability_df.geom_type.isin(['Polygon', 'MultiPolygon'])
                ]
                # Explode multipolygon geometries to single polygons
                nfhl_availability_df = nfhl_availability_df.explode(index_parts=True).reset_index(drop=True)
                process_nfhl(
                    nfhl_availability_df, 'availability', huc, out_file, file_logger, fill_holes=False
                )

                # Process combined 100-year and 500-year zones
                nfhl_df_dissolved = nfhl_df.dissolve()
                nfhl_df_exploded = nfhl_df_dissolved.explode().reset_index(drop=True)

                new_geom = [Polygon(geom.exterior) for geom in nfhl_df_exploded.geometry]
                nfhl_df_exploded.geometry = new_geom

                # remove None geometries
                nfhl_df_exploded = nfhl_df_exploded[~nfhl_df_exploded.geometry.isna()]
                # final dissolve
                nfhl_df = nfhl_df_exploded.dissolve().reset_index(drop=True)
                nfhl_df = nfhl_df.dropna(axis=1, how='all')
                # Save to GPKG as 'combined' layer
                if nfhl_df.empty:
                    file_logger.error(f"No NFHL data for HUC {huc}")
                    return False
                nfhl_df.to_file(out_file, layer='combined', index=False, driver='GPKG')
                return True
            else:
                file_logger.info(f"Output file {out_file} already exists, skipping HUC {task_id}")
                screen_queue.put(f"Output file {out_file} already exists, skipping HUC {task_id}")
                return True

        # Get flood hazard zones
        success = __get_nfhl_flood_hazard_zones(
            huc=huc, out_file=out_file, wbd=wbd, geometryType=geometry_type, geometryCRS=geometryCRS
        )
        if success:  # True
            file_logger.info(f"Completed processing HUC {task_id}")
            return 1, [True]
            # screen_queue.put(f"Completed processing HUC {task_id}")
        else:  # False
            return 0, [False]

    except Exception as e:
        file_logger.error(f"Exception in HUC {task_id}: {str(e)}")
        file_logger.error(traceback.format_exc())
        return 1, []


def download_nfhl_wrapper(huc_list, output_folder, geometryType='esriGeometryEnvelope', num_processes=1):
    """
    wrapper to process multiple HUCs using multiprocessing.

    Parameters:
    huc_list : list
        list of huc8
    output_folder : str
        output folder
    geometryType : str
        geometry type for query
    num_processes : int
        Number of processes
    """

    start_time = dt.datetime.now(dt.timezone.utc)

    huc_list = read_huc_file_list_or_array_of_hucs(huc_list)

    if not huc_list or len(huc_list) == 0:
        raise ValueError('No valid HUC8 provided')

    os.makedirs(output_folder, exist_ok=True)
    # Set up logger
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_path = os.path.join(output_folder, f"nfhl_download-{file_dt_string}.log")
    # file_logger = setup_mp_file_logger(log_file_path)
    file_logger = setup_mp_file_logger(log_file_path, "nfhl_download")

    try:

        print("==================================")
        file_logger.info("Started NFHL download process")
        print("Started NFHL download process")
        print("")
        print("*** NOTE: This will auto skip previously downloaded huc fema records")
        print("")
        file_logger.info(f"   Start time (UTC): {start_time.strftime('%m/%d/%Y %H:%M:%S')}")
        print(f"   Start time (UTC): {start_time.strftime('%m/%d/%Y %H:%M:%S')}")
        print("")

        # Load wbd
        wbd_conus, wbd_alaska, wbd_guam, wbd_american_samoa = load_wbd(huc_list)

        # Tasks argument list
        huc_list = sorted(huc_list)

        tasks_args_list = []
        for huc in huc_list:
            if not huc.isdigit() or len(huc) != 8:
                file_logger.error(f"Skipping invalid HUC8: {huc}")
                print(f"Skipping invalid HUC8: {huc}")
                continue
            tasks_args_list.append(
                {
                    "huc": huc,
                    "out_file": os.path.join(output_folder, f"nfhl_{huc}.gpkg"),
                    "wbd_conus": wbd_conus,
                    "wbd_alaska": wbd_alaska,
                    "wbd_guam": wbd_guam,
                    "wbd_american_samoa": wbd_american_samoa,
                    "geometry_type": geometryType,
                }
            )

        # Run multiprocessing
        # A mix of abort on some errors, continue on others (case by case)
        results = run_with_mp(
            task_function=download_nfhl,
            tasks_args_list=tasks_args_list,
            file_logger=file_logger,
            max_workers=num_processes,
            task_id_key='huc',
            show_progress=False,  # Disables the progress bar display
        )
        file_logger.info(f"Multiprocessing results: {results}")

        # only report if all succeeded or the failed ones
        failed_keys = [k for k, v in results.items() if not v]

        if not failed_keys:
            file_logger.info("All multiprocessing tasks Succeeded")
            print("All multiprocessing tasks Succeeded")
        else:
            file_logger.info(f"{len(failed_keys)} failed:")
            print(f"{len(failed_keys)} failed:")
            for k in failed_keys:
                file_logger.info(f"  - {k}")
                print(f"  - {k}")
        print('Multiprocessing tasks finished :)')
        print("")

        end_time = dt.datetime.now(dt.timezone.utc)
        print(f"   End time (UTC): {end_time.strftime('%m/%d/%Y %H:%M:%S')}")
        file_logger.info(f"End time (UTC): {start_time.strftime('%m/%d/%Y %H:%M:%S')}")
        file_logger.info(fh.print_date_time_duration(start_time, end_time))

    except Exception:
        end_time = dt.datetime.now(dt.timezone.utc)
        print("An exception was thrown")
        file_logger.error("An exception was thrown")
        print(traceback.format_exc())
        file_logger.error(traceback.format_exc())

        print(f"   End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")


if __name__ == "__main__":

    """
    Sample Usage
    ----------
    python3 /foss_fim/data/nfhl/download_fema_nfhl.py -u 11070106 08080206
        -o /outputs/fema/test -j 8
    OR
    python3 /foss_fim/data/nfhl/download_fema_nfhl.py -u huc_list.lst
        -o /outputs/fema/test -j 8
    """

    parser = argparse.ArgumentParser(description="Query NFHL flood hazard zones for a HUC8")
    parser.add_argument('-u', "--huc", help="List of HUC8 or a .lst file", type=str, required=True, nargs='+')
    parser.add_argument('-o', "--output_folder", help="Output directory", type=str, required=True)
    parser.add_argument(
        '-g', "--geometryType", help="Geometry type", required=False, default='esriGeometryEnvelope'
    )
    parser.add_argument('-j', "--num_processes", help="Number of processes", type=int, default=1)

    args = parser.parse_args()
    huc_inputs = args.huc

    download_nfhl_wrapper(
        huc_list=huc_inputs,
        output_folder=args.output_folder,
        geometryType=args.geometryType,
        num_processes=args.num_processes,
    )

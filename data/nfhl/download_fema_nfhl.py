import argparse
import os
import time
from contextlib import redirect_stderr, redirect_stdout
from multiprocessing import Pool

import geopandas as gpd
from dotenv import load_dotenv
from esri import ESRI_REST
from shapely import Polygon


def load_wbd(huc_list):
    """
    Load wbd data.

    Parameters
    ----------
    huc : list
        List of huc8
    Returns
    tuple
        (wbd_conus, wbd_alaska)
    """
    srcDir = os.getenv('srcDir')
    load_dotenv(f'{srcDir}/bash_variables.env')
    input_WBD_gdb = os.getenv('input_WBD_gdb')
    input_WBD_gdb_Alaska = os.getenv('input_WBD_gdb_Alaska')  # alaska
    wbd_conus = None
    wbd_alaska = None
    # Check if any huc8 is in AK
    has_alaska = any(huc.startswith('19') for huc in huc_list)

    # Load conus wbd if needed
    if any(not huc.startswith('19') for huc in huc_list):
        if os.path.exists(input_WBD_gdb):
            wbd_conus = gpd.read_file(input_WBD_gdb)
        else:
            print(f'wbd file {input_WBD_gdb} does not exist')
    # Load AK wbd if needed
    if has_alaska and os.path.exists(input_WBD_gdb_Alaska):
        wbd_alaska = gpd.read_file(input_WBD_gdb_Alaska)
    elif has_alaska:
        print(f'wbd Alaska file {input_WBD_gdb_Alaska} does not exist')
    return wbd_conus, wbd_alaska


def download_nfhl(huc, out_file, wbd_conus, wbd_alaska, geometryType='esriGeometryEnvelope'):
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
    geometryType: str
        the geometry Type for the query
    """

    DEFAULT_FIM_PROJECTION_CRS = 5070
    ALASKA_CRS = 3338  # alaska

    # Select approporiate wbd and crs
    is_alaska = huc.startswith('19')
    wbd = wbd_alaska if is_alaska else wbd_conus
    geometryCRS = ALASKA_CRS if is_alaska else DEFAULT_FIM_PROJECTION_CRS

    if wbd is None:
        print(f'No wbd available for huc {huc}')
        return

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

            nfhl_query_url = (
                "https://hazards.fema.gov/arcgis/rest/services/FIRMette/NFHLREST_FIRMette/MapServer/20/query"
            )
            # Filter for 100-year (A, V zones): FLD_ZONE LIKE 'A%' OR FLD_ZONE LIKE 'V%'
            # and 500-year (X): FLD_ZONE LIKE 'X' AND ZONE_SUBTY = '0.2 PCT ANNUAL CHANCE FLOOD HAZARD'
            where_clause = (
                "(FLD_ZONE LIKE 'A%' OR FLD_ZONE LIKE 'V%') OR"
                "(FLD_ZONE LIKE 'X' AND ZONE_SUBTY = '0.2 PCT ANNUAL CHANCE FLOOD HAZARD')"
            )

            with open(os.devnull, 'w') as devnull:
                with redirect_stdout(devnull), redirect_stderr(devnull):
                    nfhl_df = ESRI_REST.query(
                        nfhl_query_url,
                        f="json",
                        where=where_clause,
                        returnGeometry="true",
                        outFields="*",
                        outSR=str(geometryCRS),
                        geometryType=geometryType,
                        geometry=str(geometry),
                        resultRecordCount=100,
                        geometryPrecision=1,
                        maxAllowableOffset=1,
                    )

            # Clean the geometries to remove self-intersections
            nfhl_df['geometry'] = nfhl_df['geometry'].make_valid()
            nfhl_df = gpd.clip(nfhl_df, polygon)
            # Filter polygons and multipolygons
            # print(nfhl_df.geom_type.value_counts())
            nfhl_df = nfhl_df[nfhl_df.geom_type.isin(['Polygon', 'MultiPolygon'])]
            # Explode multipolygon geometries to single polygons
            nfhl_df = nfhl_df.explode(index_parts=True).reset_index(drop=True)
            if not nfhl_df.empty:
                # Save 100-year data
                nfhl_100 = nfhl_df[nfhl_df['FLD_ZONE'].str.startswith(('A', 'V'))]
                if not nfhl_100.empty:
                    nfhl_100_dissolved = nfhl_100.dissolve()
                    nfhl_100_exploded = nfhl_100_dissolved.explode().reset_index(drop=True)
                    new_geoms_100 = [
                        Polygon(geom.exterior)
                        for geom in nfhl_100_exploded.geometry
                        if geom is not None and geom.is_valid
                    ]
                    nfhl_100_exploded.geometry = new_geoms_100
                    nfhl_100_exploded = nfhl_100_exploded[~nfhl_100_exploded.geometry.isna()]
                    nfhl_100_final = nfhl_100_exploded.dissolve().reset_index(drop=True)
                    nfhl_100_final = nfhl_100_final.dropna(axis=1, how='all')
                    # Save to GPKG as '100_year' layer
                    nfhl_100_final.to_file(out_file, layer='100_year', index=False, driver='GPKG')
                else:
                    print(f'No 100-year zones for HUC {huc}')

                # Save 500-year data
                nfhl_500 = nfhl_df[
                    (nfhl_df['FLD_ZONE'] == 'X')
                    & (nfhl_df['ZONE_SUBTY'] == '0.2 PCT ANNUAL CHANCE FLOOD HAZARD')
                ]
                if not nfhl_500.empty:
                    nfhl_500_dissolved = nfhl_500.dissolve()
                    nfhl_500_exploded = nfhl_500_dissolved.explode().reset_index(drop=True)
                    new_geoms_500 = [
                        Polygon(geom.exterior)
                        for geom in nfhl_500_exploded.geometry
                        if geom is not None and geom.is_valid
                    ]
                    nfhl_500_exploded.geometry = new_geoms_500
                    nfhl_500_exploded = nfhl_500_exploded[~nfhl_500_exploded.geometry.isna()]
                    nfhl_500_final = nfhl_500_exploded.dissolve().reset_index(drop=True)
                    nfhl_500_final = nfhl_500_final.dropna(axis=1, how='all')
                    # Save to GPKG as '500_year' layer
                    nfhl_500_final.to_file(out_file, layer='500_year', index=False, driver='GPKG')
                else:
                    print(f'No 500-year zones for HUC {huc}')
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
                nfhl_df.to_file(out_file, layer='combined', index=False, driver='GPKG')

            else:
                print(f'No NFHL data for HUC {huc}')
        else:
            print(f'Output file {out_file} already exist, skipping.')

    # Get flood hazard zones
    __get_nfhl_flood_hazard_zones(
        huc=huc, out_file=out_file, wbd=wbd, geometryType='esriGeometryEnvelope', geometryCRS=geometryCRS
    )


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
    strat_total_time = time.time()
    print(f'Processing {len(huc_list)} HUCs.')
    os.makedirs(output_folder, exist_ok=True)

    wbd_conus, wbd_alaska = load_wbd(huc_list)

    tasks = []
    for huc in huc_list:
        out_file = os.path.join(output_folder, f'nfhl_{huc}.gpkg')
        tasks.append((huc, out_file, wbd_conus, wbd_alaska, geometryType))
    with Pool(processes=num_processes) as pool:
        pool.starmap(download_nfhl, tasks)
    end_total_time = time.time()
    total_duration = (end_total_time - strat_total_time) / 60
    print(f'Finished processing {len(huc_list)} HUCs in {total_duration:.2f} minutes')


if __name__ == "__main__":

    """
    Sample Usage
    ----------
    python3 /foss_fim//data/nfhl/download_fema_nfhl.py -u 11070106 08080206
        -o /outputs/fema/test -j 8
    """

    parser = argparse.ArgumentParser(description="Query NFHL flood hazard zones for a HUC8")
    parser.add_argument('-u', "--huc", help="List of HUC8", type=str, required=True, nargs='+')
    parser.add_argument('-o', "--output_folder", help="Output directory", type=str, required=True)
    parser.add_argument(
        '-g', "--geometryType", help="Geometry type", required=False, default='esriGeometryEnvelope'
    )
    parser.add_argument('-j', "--num_processes", help="Number of processes", type=int, default=1)

    args = parser.parse_args()
    huc_list = args.huc

    download_nfhl_wrapper(
        huc_list=huc_list,
        output_folder=args.output_folder,
        geometryType=args.geometryType,
        num_processes=args.num_processes,
    )

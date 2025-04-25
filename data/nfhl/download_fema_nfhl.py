import argparse
import os
import time
from multiprocessing import Pool

import geopandas as gpd
import pandas as pd
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
        print(f'No wbd availble for huc {huc}')
        return

    def __get_dfirm_panels(huc, wbd, geometryType='esriGeometryEnvelope', geometryCRS=geometryCRS):
        """
        Query the NFHL DFIRM panels for a given HUC8

        Returns
        ----------
        list
            List of DFIRM_IDs for panels intersecting the HUC8.
        """

        polygon = wbd.loc[wbd.HUC8 == huc]

        minx, miny, maxx, maxy = polygon.geometry.bounds.values[0]

        geometry = {
            "xmin": minx,
            "ymin": miny,
            "xmax": maxx,
            "ymax": maxy,
            "spatialReference": {"wkid": geometryCRS},
        }

        geometry = str(geometry)

        dfirm_query_url = (
            "https://hazards.fema.gov/arcgis/rest/services/FIRMette/NFHLREST_FIRMette/MapServer/1/query"
        )
        dfirm_df = ESRI_REST.query(
            dfirm_query_url,
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

        return dfirm_df['DFIRM_ID'].unique().tolist()

    def __get_nfhl_flood_hazard_zones(
        huc, wbd, out_file, dfirm_ids, geometryType='esriGeometryEnvelope', geometryCRS=geometryCRS
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
        dfirm_ids: list
            List of DFIRM_IDs to query
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
            # Query for each DFIRM_ID to handle large datasets
            nfhl_dfs = []
            for dfirm_id in dfirm_ids:
                dfirm_where = f"DFIRM_ID = '{dfirm_id}' AND ({where_clause})"
                nfhl_df = ESRI_REST.query(
                    nfhl_query_url,
                    f="json",
                    where=dfirm_where,
                    returnGeometry="true",
                    outFields="*",
                    outSR=str(geometryCRS),
                    geometryType=geometryType,
                    geometry=str(geometry),
                    resultRecordCount=100,
                    geometryPrecision=1,
                    maxAllowableOffset=1,
                )
                if not nfhl_df.empty:
                    nfhl_dfs.append(nfhl_df)
            if nfhl_dfs:
                nfhl_df = gpd.GeoDataFrame(pd.concat(nfhl_dfs, ignore_index=True))
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
                if not nfhl_100.empty:
                    # Derive 100-year output file name
                    base, ext = os.path.splitext(out_file)
                    out_file_100yr = f'{base}_100yr{ext}'
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
                    nfhl_100_final.to_file(out_file_100yr, index=False, driver='GPKG')
                else:
                    print(f'No 100-year zones for HUC {huc}')

                # Save 500-year data
                nfhl_500 = nfhl_df[
                    (nfhl_df['FLD_ZONE'] == 'X')
                    & (nfhl_df['ZONE_SUBTY'] == '0.2 PCT ANNUAL CHANCE FLOOD HAZARD')
                ]
                if not nfhl_100.empty:
                    # Derive 100-year output file name
                    base, ext = os.path.splitext(out_file)
                    out_file_500yr = f'{base}_500yr{ext}'
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
                    nfhl_500_final.to_file(out_file_500yr, index=False, driver='GPKG')
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
                nfhl_df.to_file(out_file, index=False, driver='GPKG')
            else:
                print(f"No flood hazard data found for HUC {huc}")
        else:
            print(f'Output file {out_file} already exist, skipping.')

    # Get DFIRM panels
    dfirm_ids = __get_dfirm_panels(huc, wbd, geometryType, geometryCRS)
    if not dfirm_ids:
        print(f'No DFIRM panels found for HUC {huc}')
        return
    # Get flood hazard zones
    __get_nfhl_flood_hazard_zones(
        huc=huc,
        out_file=out_file,
        wbd=wbd,
        dfirm_ids=dfirm_ids,
        geometryType='esriGeometryEnvelope',
        geometryCRS=geometryCRS,
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
    parser = argparse.ArgumentParser(description="Query NFHL flood hazard zones for a HUC8")
    parser.add_argument('-u', "--huc", help="List of HUC8", type=str, required=True)
    parser.add_argument('-o', "--output_folder", help="Output directory", type=str, required=True)
    parser.add_argument(
        '-g', "--geometryType", help="Geometry type", required=False, default='esriGeometryEnvelope'
    )
    parser.add_argument('-j', "--num_processes", help="Number of processes", type=int, default=1)

    args = parser.parse_args()

    # handle single huc or list of hucs
    huc_list = args.huc.split(',') if ',' in args.huc else [args.huc]

    download_nfhl_wrapper(
        huc_list=huc_list,
        output_folder=args.output_folder,
        geometryType=args.geometryType,
        num_processes=args.num_processes,
    )

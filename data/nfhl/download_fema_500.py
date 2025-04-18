import argparse
import os

import geopandas as gpd
import pandas as pd
from esri import ESRI_REST
from shapely import Polygon


def download_nfhl(huc, out_file, geometryType='esriGeometryEnvelope', geometryCRS=5070):
    """
    Download the NFHL flood hazard zones for a given HUC8

    Parameters
    ----------
    huc : str
        The HUC8 code to query
    output_folder : str
        The folder to save the output file
    output_root : str
        The root name for the output file
    """
    wbd = gpd.read_file('/data/inputs/wbd/WBD_National_HUC8_EPSG_5070_HAND_domain.gpkg')

    def __get_dfirm_panels(huc, wbd, geometryType='esriGeometryEnvelope', geometryCRS=5070):
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

        geometryType = 'esriGeometryEnvelope'

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
            outSR="5070",
            geometryType=geometryType,
            geometry=geometry,
            resultRecordCount=100,
            geometryPrecision=1,
            maxAllowableOffset=1,
        )

        return dfirm_df['DFIRM_ID'].unique().tolist()

    def __get_nfhl_flood_hazard_zones(
        huc, wbd, out_file, dfirm_ids, geometryType='esriGeometryEnvelope', geometryCRS=5070
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
            # Filter for 100-year (A, V zones) and 500-year (X)
            # FLD_ZONE LIKE 'A%' OR FLD_ZONE LIKE 'V%'
            where_clause = "(FLD_ZONE LIKE 'X')"
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
                    outSR="5070",
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
                nfhl_df_dissolved = nfhl_df.dissolve()
                nfhl_df_exploded = nfhl_df_dissolved.explode().reset_index(drop=True)
                # new_geoms = []
                # for geom in nfhl_df_exploded.geometry:
                #     new_geoms.append(Polygon(geom.exterior))
                # nfhl_df_exploded.geometry = new_geoms
                new_geom = [Polygon(geom.exterior) for geom in nfhl_df_exploded.geometry]
                nfhl_df_exploded.geometry = new_geom
                # remove None geometries
                nfhl_df_exploded = nfhl_df_exploded[~nfhl_df_exploded.geometry.isna()]
                # final dissolve
                nfhl_df = nfhl_df_exploded.dissolve().reset_index(drop=True)
                nfhl_df.to_file(out_file, index=False, driver='GPKG')
            else:
                print(f"No flood hazard data found for HUC {huc}")
        else:
            nfhl_df = gpd.read_file(out_file)

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
        geometryCRS=5070,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query NFHL flood hazard zones for a HUC8")
    parser.add_argument('-u', "--huc", help="HUC8 code", type=str, required=True)
    parser.add_argument('-o', "--out-file", help="Output file name", type=str, required=True)
    parser.add_argument(
        '-g', "--geometryType", help="Geometry type", required=False, default='esriGeometryEnvelope'
    )
    parser.add_argument('-c', "--geometryCRS", help="Geometry CRS", required=False, default=5070)

    args = parser.parse_args()

    download_nfhl(**vars(args))

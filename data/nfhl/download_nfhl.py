#!/usr/bin/env python3

import argparse
import os

import geopandas as gpd
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

    def __get_nfhl_flood_hazard_zones(
        huc, wbd, out_file, geometryType='esriGeometryEnvelope', geometryCRS=5070
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
            # nfhl_query_url = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query"
            nfhl_df = ESRI_REST.query(
                nfhl_query_url,
                f="json",
                where="FLD_ZONE LIKE 'A%' OR FLD_ZONE LIKE 'V%'",
                returnGeometry="true",
                outFields="*",
                outSR="5070",
                geometryType=geometryType,
                geometry=geometry,
                resultRecordCount=100,
                geometryPrecision=1,
                maxAllowableOffset=1,
            )

            polygon = wbd.loc[wbd.HUC8 == huc]
            # Clean the geometries to remove self-intersections
            nfhl_df['geometry'] = nfhl_df['geometry'].make_valid()

            nfhl_df = gpd.clip(nfhl_df, polygon)

            nfhl_df.to_file(out_file, index=False, driver='GPKG')

        else:
            nfhl_df = gpd.read_file(out_file)

        nfhl_df = nfhl_df[nfhl_df.geom_type == 'Polygon']

        # Clean the geometries to remove self-intersections
        nfhl_df['geometry'] = nfhl_df['geometry'].make_valid()

        nfhl_df_dissolved = nfhl_df.dissolve()

        nfhl_df_exploded = nfhl_df_dissolved.explode().reset_index(drop=True)

        new_geoms = []
        for row in nfhl_df_exploded.iterrows():
            polygon = row[1].geometry

            new_geoms.append(Polygon(polygon.exterior))

        nfhl_df_exploded.geometry = new_geoms

        nfhl_df = nfhl_df_exploded.dissolve().reset_index(drop=True)

        nfhl_df.to_file(out_file, index=False, driver='GPKG')

    __get_nfhl_flood_hazard_zones(
        huc=huc, out_file=out_file, wbd=wbd, geometryType='esriGeometryEnvelope', geometryCRS=5070
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

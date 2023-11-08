#!/usr/bin/env python
# coding: utf-8

import argparse
import os

import pandas as pd
from overlapping_inundation import OverlapWindowMerge
from tqdm import tqdm

from utils.shared_functions import FIM_Helpers as fh
from utils.shared_variables import elev_raster_ndv


def Mosaic_inundation(
    map_file,
    mosaic_attribute,
    mosaic_output=None,
    mask=None,
    unit_attribute_name="huc8",
    nodata=elev_raster_ndv,
    workers=1,
    remove_inputs=False,
    subset=None,
    verbose=True,
    is_mosaic_for_branches=False,
    inundation_polygon=None,
):
    if not os.path.isdir(os.path.dirname(mosaic_output)):
        os.makedirs(os.path.dirname(mosaic_output))

    # check input
    if mosaic_attribute not in ("inundation_rasters", "depths_rasters"):
        raise ValueError("Pass inundation_rasters or depths_raster for mosaic_attribute argument")

    # load file
    if isinstance(map_file, pd.DataFrame):
        inundation_maps_df = map_file
        del map_file
    elif isinstance(map_file, str):
        inundation_maps_df = pd.read_csv(map_file, dtype={unit_attribute_name: str, "branchID": str})
    else:
        raise TypeError("Pass Pandas Dataframe or file path string to csv for map_file argument")

    # remove NaNs
    inundation_maps_df = inundation_maps_df.dropna(axis=0, how="all")

    # subset
    if subset is not None:
        subset_mask = inundation_maps_df.loc[:, unit_attribute_name].isin(subset)
        inundation_maps_df = inundation_maps_df.loc[subset_mask, :]

    # unique aggregation units
    aggregation_units = inundation_maps_df.loc[:, unit_attribute_name].unique()

    inundation_maps_df = inundation_maps_df.set_index(unit_attribute_name, drop=True)

    # decide upon whether to display
    if verbose & len(aggregation_units) == 1:
        tqdm_disable = False
    elif verbose:
        tqdm_disable = False
    else:
        tqdm_disable = True

    ag_mosaic_output = ""
    remove_at_end = []

    for ag in tqdm(aggregation_units, disable=tqdm_disable, desc="Mosaicing FIMs"):
        try:
            inundation_maps_list = inundation_maps_df.loc[ag, mosaic_attribute].tolist()
        except AttributeError:
            inundation_maps_list = [inundation_maps_df.loc[ag, mosaic_attribute]]

        # Some processes may have already added the ag value (if it is a huc) to
        # the file name, so don't re-add it.
        # Only add the huc into the name if branches are being processed, as
        # sometimes the mosaic is not for gms branches but maybe mosaic of an
        # fr set with a gms composite map.

        ag_mosaic_output = mosaic_output
        if (is_mosaic_for_branches) and (ag not in mosaic_output):
            ag_mosaic_output = fh.append_id_to_file_name(mosaic_output, ag)  # change it

        remove_list = mosaic_by_unit(
            inundation_maps_list,
            ag_mosaic_output,
            nodata,
            workers=1,
            remove_inputs=remove_inputs,
            mask=mask,
            verbose=verbose,
        )

        if remove_list is not None:
            remove_at_end.extend(remove_list)

    # # inundation maps
    # inundation_maps_df.reset_index(drop=True)

    if inundation_polygon is not None:
        mosaic_final_inundation_extent_to_poly(ag_mosaic_output, inundation_polygon)

    if remove_inputs:
        fh.vprint("Removing inputs ...", verbose)

        for remove_file in remove_at_end:
            os.remove(remove_file)

    # Return file name and path of the final mosaic output file.
    # Might be empty.
    return ag_mosaic_output


# Note: This uses threading and not processes. If the number of workers is more than
# the number of possible threads, no results will be returned. But it is usually
# pretty fast anyways. This needs to be fixed.
def mosaic_by_unit(
    inundation_maps_list,
    mosaic_output,
    nodata=elev_raster_ndv,
    workers=1,
    remove_inputs=False,
    mask=None,
    verbose=False,
):
    # overlap object instance
    overlap = OverlapWindowMerge(inundation_maps_list, (30, 30))

    if mosaic_output is not None:
        if workers > 1:
            threaded = True
        else:
            threaded = False

        overlap.merge_rasters(mosaic_output, threaded=threaded, workers=1, nodata=nodata)

        if mask:
            fh.vprint("Masking ...", verbose)
            overlap.mask_mosaic(mosaic_output, mask, outfile=mosaic_output)

    if remove_inputs:
        fh.vprint("Removing inputs ...", verbose)

        remove_list = []
        for inun_map in inundation_maps_list:
            if inun_map is not None and os.path.isfile(inun_map):
                remove_list.append(inun_map)

        return remove_list


def mosaic_final_inundation_extent_to_poly(inundation_raster, inundation_polygon, driver="GPKG"):
    import geopandas as gpd
    import numpy as np
    import rasterio
    from rasterio.features import shapes
    from shapely.geometry.multipolygon import MultiPolygon
    from shapely.geometry.polygon import Polygon

    with rasterio.open(inundation_raster) as src:
        # Open inundation_raster using rasterio.
        image = src.read(1)
        print("Producing merged polygon...")

        # Use numpy.where operation to reclassify depth_array on the condition that the pixel values are > 0.
        reclass_inundation_array = np.where((image > 0) & (image != src.nodata), 1, 0).astype("uint8")

        # Aggregate shapes
        results = (
            {"properties": {"extent": 1}, "geometry": s}
            for i, (s, v) in enumerate(
                shapes(reclass_inundation_array, mask=reclass_inundation_array > 0, transform=src.transform)
            )
        )

        # Convert list of shapes to polygon, then dissolve
        extent_poly = gpd.GeoDataFrame.from_features(list(results), crs=src.crs)
        extent_poly_diss = extent_poly.dissolve(by="extent")
        extent_poly_diss["geometry"] = [
            MultiPolygon([feature]) if type(feature) is Polygon else feature
            for feature in extent_poly_diss["geometry"]
        ]

        # Write polygon
        extent_poly_diss.to_file(inundation_polygon, driver=driver)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mosaic GMS Inundation Rasters")
    parser.add_argument(
        "-m",
        "--map-file",
        help="Pandas Dataframe or file path string to CSV of inundation/depth maps to mosaic.",
        required=True,
    )
    parser.add_argument(
        "-t",
        "--mosaic-attribute",
        help="Attribute name: should be either inundation_rasters or depths_rasters.",
        required=True,
        type=str,
    )
    parser.add_argument(
        "-a",
        "--mask",
        help="File path to vector polygon mask used to clip mosaic (optional). Default is None",
        required=False,
        default=None,
        type=str,
    )
    parser.add_argument(
        "-u",
        "--unit-attribute-name",
        help="Unit attribute name (optional). Default is huc8",
        required=False,
        default="huc8",
        type=str,
    )
    parser.add_argument(
        "-s",
        "--subset",
        help="Value(s) of unit attribute name used to subset (optional)",
        required=False,
        default=None,
        type=str,
        nargs="+",
    )
    parser.add_argument(
        "-n", "--nodata", help="NODATA value for output raster", required=False, default=elev_raster_ndv
    )
    parser.add_argument(
        "-w",
        "--workers",
        help="Number of Workers (optional). Default value is 1.",
        required=False,
        default=1,
        type=int,
    )
    parser.add_argument(
        "-o",
        "--mosaic-output",
        help="Mosaiced inundation Maps file name",
        required=False,
        default=None,
        type=str,
    )
    parser.add_argument(
        "-i",
        "--inundation-polygon",
        help="Filename of the final inundation extent polygon (optional). Default is None.",
        required=False,
        default=None,
        type=str,
    )
    parser.add_argument(
        "-r",
        "--remove-inputs",
        help="Remove original input inundation Maps (optional). Default is False",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Print out messages (optional). Default is False",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-g",
        "--is-mosaic-for-branches",
        help="If the mosaic is for branchs, include this arg. If is_mosaic_for_branches is true, "
        "the mosaic output name will add the HUC into the output name for overwrite reasons.",
        required=False,
        default=False,
        action="store_true",
    )

    args = vars(parser.parse_args())

    Mosaic_inundation(**args)

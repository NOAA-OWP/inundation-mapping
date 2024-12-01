#!/usr/bin/env python3

import argparse
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
from inundation import NoForecastFound, hydroTableHasOnlyLakes, inundate
from tqdm import tqdm

from utils.shared_functions import FIM_Helpers as fh


def Inundate_gms(
    hydrofabric_dir,
    forecast,
    num_workers=1,
    hucs=None,
    inundation_raster=None,
    inundation_polygon=None,
    depths_raster=None,
    verbose=False,
    log_file=None,
    hydro_table_df=None,
    output_fileNames=None,
):
    # input handling
    if hucs is not None:
        try:
            _ = (i for i in hucs)
        except TypeError:
            raise ValueError("hucs argument must be an iterable")

    if isinstance(hucs, str):
        hucs = [hucs]

    num_workers = int(num_workers)

    # log file
    if log_file is not None:
        if os.path.exists(log_file):
            os.remove(log_file)

        if verbose:
            print("HUC8,BranchID,Exception", file=open(log_file, "w"))
    # if log_file:
    #     logging.basicConfig(filename=log_file, level=logging.INFO)
    #     logging.info('HUC8,BranchID,Exception')

    # load fim inputs
    hucs_branches = pd.read_csv(
        os.path.join(hydrofabric_dir, "fim_inputs.csv"), header=None, dtype={0: str, 1: str}
    )

    if hucs is not None:
        hucs = set(hucs)
        huc_indices = hucs_branches.loc[:, 0].isin(hucs)
        hucs_branches = hucs_branches.loc[huc_indices, :]

    # get number of branches
    number_of_branches = len(hucs_branches)

    # make inundate generator
    inundate_input_generator = __inundate_gms_generator(
        hucs_branches,
        hydrofabric_dir,
        inundation_raster,
        inundation_polygon,
        depths_raster,
        forecast,
        verbose=False,
    )

    # start up process pool
    # better results with Process pool
    executor = ProcessPoolExecutor(max_workers=num_workers)

    # collect output filenames
    inundation_raster_fileNames = [None] * number_of_branches
    inundation_polygon_fileNames = [None] * number_of_branches
    depths_raster_fileNames = [None] * number_of_branches
    hucCodes = [None] * number_of_branches
    branch_ids = [None] * number_of_branches

    executor_generator = {executor.submit(inundate, **inp): ids for inp, ids in inundate_input_generator}
    idx = 0
    for future in tqdm(
        as_completed(executor_generator),
        total=len(executor_generator),
        desc=f"Inundating branches with {num_workers} workers",
        disable=(not verbose),
    ):
        hucCode, branch_id = executor_generator[future]

        try:
            future.result()

        except NoForecastFound as exc:
            if log_file is not None:
                print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}", file=open(log_file, "a"))
            elif verbose:
                print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}")

        except hydroTableHasOnlyLakes as exc:
            if log_file is not None:
                print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}", file=open(log_file, "a"))
            elif verbose:
                print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}")

        except Exception as exc:
            if log_file is not None:
                print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}", file=open(log_file, "a"))
            else:
                print(f"{hucCode},{branch_id},{exc.__class__.__name__}, {exc}")
        else:
            hucCodes[idx] = hucCode
            branch_ids[idx] = branch_id

            try:
                # print(hucCode,branch_id,future.result()[0][0])
                inundation_raster_fileNames[idx] = future.result()[0][0]
            except TypeError:
                pass

            try:
                depths_raster_fileNames[idx] = future.result()[1][0]
            except TypeError:
                pass

            try:
                inundation_polygon_fileNames[idx] = future.result()[2][0]
            except TypeError:
                pass

            idx += 1

    # power down pool
    executor.shutdown(wait=True)

    # make filename dataframe
    output_fileNames_df = pd.DataFrame(
        {
            "huc8": hucCodes,
            "branchID": branch_ids,
            "inundation_rasters": inundation_raster_fileNames,
            "depths_rasters": depths_raster_fileNames,
            "inundation_polygons": inundation_polygon_fileNames,
        }
    )

    if output_fileNames is not None:
        output_fileNames_df.to_csv(output_fileNames, index=False)

    return output_fileNames_df


def __inundate_gms_generator(
    hucs_branches,
    hydrofabric_dir,
    inundation_raster,
    inundation_polygon,
    depths_raster,
    forecast,
    verbose=False,
    hydro_table_df=None,
):
    # Iterate over branches
    for idx, row in hucs_branches.iterrows():
        huc = str(row[0])
        branch_id = str(row[1])

        huc_dir = os.path.join(hydrofabric_dir, huc)
        branch_dir = os.path.join(huc_dir, "branches", branch_id)

        rem_file_name = f"rem_zeroed_masked_{branch_id}.tif"
        rem_branch = os.path.join(branch_dir, rem_file_name)

        catchments_file_name = f"gw_catchments_reaches_filtered_addedAttributes_{branch_id}.tif"
        catchments_branch = os.path.join(branch_dir, catchments_file_name)

        # FIM versions > 4.3.5 use an aggregated hydrotable file rather than individual branch hydrotables

        if hydro_table_df is not None:
            hydroTable_all = hydro_table_df.set_index(["HUC", "feature_id", "HydroID"], inplace=False)
            hydroTable_branch = hydroTable_all.loc[hydroTable_all["branch_id"] == int(branch_id)]
        else:
            hydroTable_huc = os.path.join(huc_dir, "hydrotable.csv")
            if os.path.isfile(hydroTable_huc):
                htable_req_cols = [
                    "HUC",
                    "branch_id",
                    "feature_id",
                    "HydroID",
                    "stage",
                    "discharge_cms",
                    "LakeID",
                ]
                hydroTable_all = pd.read_csv(
                    hydroTable_huc,
                    dtype={
                        "HUC": str,
                        "branch_id": int,
                        "feature_id": str,
                        "HydroID": str,
                        "stage": float,
                        "discharge_cms": float,
                        "LakeID": int,
                    },
                    usecols=htable_req_cols,
                )
                hydroTable_all.set_index(["HUC", "feature_id", "HydroID"], inplace=True)
                hydroTable_branch = hydroTable_all.loc[hydroTable_all["branch_id"] == int(branch_id)]
            else:
                # Earlier FIM4 versions only have branch level hydrotables
                hydroTable_branch = os.path.join(branch_dir, f"hydroTable_{branch_id}.csv")

        xwalked_file_name = f"gw_catchments_reaches_filtered_addedAttributes_crosswalked_{branch_id}.gpkg"
        catchment_poly = os.path.join(branch_dir, xwalked_file_name)

        # branch output
        # Some other functions that call in here already added a huc, so only add it if not yet there
        if (inundation_raster is not None) and (huc not in inundation_raster):
            inundation_branch_raster = fh.append_id_to_file_name(inundation_raster, [huc, branch_id])
        else:
            inundation_branch_raster = fh.append_id_to_file_name(inundation_raster, branch_id)

        if (inundation_polygon is not None) and (huc not in inundation_polygon):
            inundation_branch_polygon = fh.append_id_to_file_name(inundation_polygon, [huc, branch_id])
        else:
            inundation_branch_polygon = fh.append_id_to_file_name(inundation_polygon, branch_id)

        if (depths_raster is not None) and (huc not in depths_raster):
            depths_branch_raster = fh.append_id_to_file_name(depths_raster, [huc, branch_id])
        else:
            depths_branch_raster = fh.append_id_to_file_name(depths_raster, branch_id)

        # identifiers
        identifiers = (huc, branch_id)

        # print(f"inundation_branch_raster is {inundation_branch_raster}")

        # inundate input
        inundate_input = {
            "rem": rem_branch,
            "catchments": catchments_branch,
            "catchment_poly": catchment_poly,
            "hydro_table": hydroTable_branch,
            "forecast": forecast,
            "mask_type": "filter",
            "hucs": None,
            "hucs_layerName": None,
            "subset_hucs": None,
            "num_workers": 1,
            "aggregate": False,
            "inundation_raster": inundation_branch_raster,
            "inundation_polygon": inundation_branch_polygon,
            "depths": depths_branch_raster,
            "out_raster_profile": None,
            "out_vector_profile": None,
            "quiet": not verbose,
        }

        yield (inundate_input, identifiers)


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser(description="Inundate FIM")
    parser.add_argument(
        "-y", "--hydrofabric_dir", help="Directory path to FIM hydrofabric by processing unit", required=True
    )
    parser.add_argument(
        "-u", "--hucs", help="List of HUCS to run", required=False, default=None, type=str, nargs="+"
    )
    parser.add_argument("-f", "--forecast", help="Forecast discharges in CMS as CSV file", required=True)
    parser.add_argument(
        "-i",
        "--inundation-raster",
        help="Inundation Raster output. Only writes if designated.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-p",
        "--inundation-polygon",
        help="Inundation polygon output. Only writes if designated.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-d",
        "--depths-raster",
        help="Depths raster output. Only writes if designated. Appends HUC code in batch mode.",
        required=False,
        default=None,
    )
    parser.add_argument(
        "-l", "--log-file", help="Log-file to store level-path exceptions", required=False, default=None
    )
    parser.add_argument(
        "-o",
        "--output-fileNames",
        help="Output CSV file with filenames for inundation rasters, inundation polygons, and depth rasters",
        required=False,
        default=None,
    )
    parser.add_argument("-w", "--num-workers", help="Number of Workers", required=False, default=1)
    parser.add_argument(
        "-v", "--verbose", help="Verbose printing", required=False, default=None, action="store_true"
    )

    Inundate_gms(**vars(parser.parse_args()))

#!/usr/bin/env python3

import argparse
import glob
import os
import re
import traceback
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from os.path import join
from dotenv import load_dotenv
import geopandas as gpd
import pandas as pd

from heal_bridges_osm import flows_from_hydrotable
from utils.shared_functions import progress_bar_handler

load_dotenv('/foss_fim/src/bash_variables.env')
DEFAULT_FIM_PROJECTION_CRS = os.getenv('DEFAULT_FIM_PROJECTION_CRS')
ALASKA_CRS = os.getenv('ALASKA_CRS')
class HucDirectory(object):
    def __init__(self, fim_directory, huc_id, limit_branches=[]):
        self.fim_directory = fim_directory
        self.huc_dir_path = join(fim_directory, huc_id)
        self.limit_branches = limit_branches

        self.usgs_dtypes = {
            'location_id': str,
            'nws_lid': str,
            'feature_id': int,
            'HydroID': int,
            'levpa_id': str,
            'dem_elevation': float,
            'dem_adj_elevation': float,
            'order_': str,
            'LakeID': object,
            'HUC8': str,
            'snap_distance': float,
        }
        self.agg_usgs_elev_table = pd.DataFrame(columns=list(self.usgs_dtypes.keys()))

        self.hydrotable_dtypes = {
            'HydroID': int,
            'branch_id': int,
            'feature_id': int,
            'NextDownID': int,
            'order_': int,
            'Number of Cells': int,
            'SurfaceArea (m2)': float,
            'BedArea (m2)': float,
            'TopWidth (m)': float,
            'LENGTHKM': float,
            'AREASQKM': float,
            'WettedPerimeter (m)': float,
            'HydraulicRadius (m)': float,
            'WetArea (m2)': float,
            'Volume (m3)': float,
            'SLOPE': float,
            'ManningN': float,
            'stage': float,
            'default_discharge_cms': float,
            'default_Volume (m3)': float,
            'default_WetArea (m2)': float,
            'default_HydraulicRadius (m)': float,
            'default_ManningN': float,
            'calb_applied': bool,
            'last_updated': str,
            'submitter': str,
            'obs_source': str,
            'precalb_discharge_cms': float,
            'calb_coef_usgs': float,
            'calb_coef_spatial': float,
            'calb_coef_final': float,
            'HUC': int,
            'LakeID': int,
            'subdiv_applied': bool,
            'channel_n': float,
            'overbank_n': float,
            'subdiv_discharge_cms': float,
            'discharge_cms': float,
        }
        self.agg_hydrotable = pd.DataFrame(columns=list(self.hydrotable_dtypes.keys()))

        self.src_crosswalked_dtypes = {
            'branch_id': int,
            'HydroID': int,
            'feature_id': int,
            'Stage': float,
            'Number of Cells': int,
            'SurfaceArea (m2)': float,
            'BedArea (m2)': float,
            'Volume (m3)': float,
            'SLOPE': float,
            'LENGTHKM': float,
            'AREASQKM': float,
            'ManningN': float,
            'NextDownID': int,
            'order_': int,
            'TopWidth (m)': float,
            'WettedPerimeter (m)': float,
            'WetArea (m2)': float,
            'HydraulicRadius (m)': float,
            'Discharge (m3s-1)': float,
            'bankfull_flow': float,
            'Stage_bankfull': float,
            'BedArea_bankfull': float,
            'Volume_bankfull': float,
            'HRadius_bankfull': float,
            'SurfArea_bankfull': float,
            'bankfull_proxy': str,
            'Volume_chan (m3)': float,
            'BedArea_chan (m2)': float,
            'WettedPerimeter_chan (m)': float,
            'Volume_obank (m3)': float,
            'BedArea_obank (m2)': float,
            'WettedPerimeter_obank (m)': float,
            'channel_n': float,
            'overbank_n': float,
            'subdiv_applied': bool,
            'WetArea_chan (m2)': float,
            'HydraulicRadius_chan (m)': float,
            'Discharge_chan (m3s-1)': float,
            'Velocity_chan (m/s)': float,
            'WetArea_obank (m2)': float,
            'HydraulicRadius_obank (m)': float,
            'Discharge_obank (m3s-1)': float,
            'Velocity_obank (m/s)': float,
            'Discharge (m3s-1)_subdiv': float,
        }
        self.agg_src_cross = pd.DataFrame(columns=list(self.src_crosswalked_dtypes.keys()))

        self.ras_dtypes = {
            'location_id': str,
            'nws_lid': str,
            'feature_id': int,
            'HydroID': int,
            'levpa_id': str,
            'dem_elevation': float,
            'dem_adj_elevation': float,
            'order_': str,
            'LakeID': object,
            'HUC8': str,
            'snap_distance': float,
        }
        self.agg_ras_elev_table = pd.DataFrame(columns=list(self.ras_dtypes.keys()))

        self.bridge_dtypes = {
            'osmid': int,
            'name': str,
            'max_hand': float,
            'max_hand_75': float,
            'feature_id': int,
            'HydroID': int,
            'order_': str,
            'branch': str,
            'mainstem': int,
            'geometry': object,
        }
        self.agg_bridge_pnts = gpd.GeoDataFrame(columns=list(self.bridge_dtypes.keys()))

    def iter_branches(self):
        if self.limit_branches:
            for branch in self.limit_branches:
                yield (branch, join(self.huc_dir_path, 'branches', branch))

        else:
            for branch in os.listdir(join(self.huc_dir_path, 'branches')):
                yield (branch, join(self.huc_dir_path, 'branches', branch))

    def usgs_elev_table(self, branch_path):
        usgs_elev_filename = join(branch_path, 'usgs_elev_table.csv')
        if not os.path.isfile(usgs_elev_filename):
            return

        usgs_elev_table = pd.read_csv(usgs_elev_filename, dtype=self.usgs_dtypes)
        self.agg_usgs_elev_table = pd.concat([self.agg_usgs_elev_table, usgs_elev_table])

    def aggregate_hydrotables(self, branch_path, branch_id):
        hydrotable_filename = join(branch_path, f'hydroTable_{branch_id}.csv')
        if not os.path.isfile(hydrotable_filename):
            return

        hydrotable = pd.read_csv(hydrotable_filename, dtype=self.hydrotable_dtypes)
        hydrotable['branch_id'] = branch_id
        hydrotable[['calb_applied']] = hydrotable[['calb_applied']].fillna(value=False)
        self.agg_hydrotable = pd.concat([self.agg_hydrotable, hydrotable])

    def aggregate_src_full_crosswalk(self, branch_path, branch_id):
        src_cross_filename = join(branch_path, f'src_full_crosswalked_{branch_id}.csv')
        if not os.path.isfile(src_cross_filename):
            return

        src_cross = pd.read_csv(src_cross_filename, dtype=self.src_crosswalked_dtypes)
        src_cross['branch_id'] = branch_id
        self.agg_src_cross = pd.concat([self.agg_src_cross, src_cross])

    def ras_elev_table(self, branch_path):
        ras_elev_filename = join(branch_path, 'ras_elev_table.csv')
        if not os.path.isfile(ras_elev_filename):
            return

        ras_elev_table = pd.read_csv(ras_elev_filename, dtype=self.ras_dtypes)
        self.agg_ras_elev_table = pd.concat([self.agg_ras_elev_table, ras_elev_table])

    def aggregate_bridge_pnts(self, branch_path, branch_id):
        bridge_filename = join(branch_path, f'osm_bridge_centroids_{branch_id}.gpkg')
        if not os.path.isfile(bridge_filename):
            return

        bridge_pnts = gpd.read_file(bridge_filename)
        for col, dtype in self.bridge_dtypes.items():
            bridge_pnts[col] = bridge_pnts[col].astype(dtype)
        if bridge_pnts.empty:
            return
        hydrotable_filename = join(branch_path, f'hydroTable_{branch_id}.csv')
        hydrotable = pd.read_csv(hydrotable_filename, dtype=self.hydrotable_dtypes)
        # Get the flows for each stage
        bridge_pnts = flows_from_hydrotable(bridge_pnts, hydrotable)
        self.agg_bridge_pnts = pd.concat([self.agg_bridge_pnts, bridge_pnts])

    def agg_function(
        self, usgs_elev_flag, hydro_table_flag, src_cross_flag, ras_elev_flag, bridge_flag, huc_id
    ):
        try:
            # try catch and its own log file output in error only.
            for branch_id, branch_path in self.iter_branches():
                if usgs_elev_flag:
                    self.usgs_elev_table(branch_path)
                if ras_elev_flag:
                    self.ras_elev_table(branch_path)

                ## Other aggregate funtions can go here
                if hydro_table_flag:
                    self.aggregate_hydrotables(branch_path, branch_id)
                if src_cross_flag:
                    self.aggregate_src_full_crosswalk(branch_path, branch_id)
                if bridge_flag:
                    self.aggregate_bridge_pnts(branch_path, branch_id)

            ## After all of the branches are visited, the code below will write the aggregates
            if usgs_elev_flag:
                usgs_elev_table_file = join(self.huc_dir_path, 'usgs_elev_table.csv')
                if os.path.isfile(usgs_elev_table_file):
                    os.remove(usgs_elev_table_file)

                if not self.agg_usgs_elev_table.empty:
                    self.agg_usgs_elev_table.to_csv(usgs_elev_table_file, index=False)

            if hydro_table_flag:
                hydrotable_file = join(self.huc_dir_path, 'hydrotable.csv')
                if os.path.isfile(hydrotable_file):
                    os.remove(hydrotable_file)

                if not self.agg_hydrotable.empty:
                    self.agg_hydrotable.to_csv(hydrotable_file, index=False)

            if src_cross_flag:
                src_crosswalk_file = join(self.huc_dir_path, 'src_full_crosswalked.csv')
                if os.path.isfile(src_crosswalk_file):
                    os.remove(src_crosswalk_file)

                if not self.agg_src_cross.empty:
                    self.agg_src_cross.to_csv(src_crosswalk_file, index=False)

            if ras_elev_flag:
                ras_elev_table_file = join(self.huc_dir_path, 'ras_elev_table.csv')
                if os.path.isfile(ras_elev_table_file):
                    os.remove(ras_elev_table_file)

                if not self.agg_ras_elev_table.empty:
                    self.agg_ras_elev_table.to_csv(ras_elev_table_file, index=False)

            if bridge_flag:
                bridge_pnts_file = join(self.huc_dir_path, 'osm_bridge_centroids.gpkg')
                if os.path.isfile(bridge_pnts_file):
                    os.remove(bridge_pnts_file)

                if not self.agg_bridge_pnts.empty:
                    # Just making things shorter so they are easier to read
                    bridge_pnts = self.agg_bridge_pnts
                    # Use branch 0 to get the feature_id each bridge crosses
                    b0 = bridge_pnts.loc[bridge_pnts.branch == '0', ['osmid', 'feature_id']]
                    b0 = b0.rename(columns={'feature_id': 'crossing_feature_id'})
                    bridge_pnts = bridge_pnts.merge(b0, on='osmid', how='left')
                    # Remove bridge points that have the same osmid and feature_id
                    g = bridge_pnts.groupby(['osmid', 'feature_id'])['max_discharge'].transform('min')
                    bridge_pnts = bridge_pnts.copy()[(bridge_pnts['max_discharge'] == g)]
                    # Set backwater bridge sites
                    bridge_pnts['is_backwater'] = 0
                    c = bridge_pnts.groupby(['osmid'])['feature_id'].transform('count')
                    bridge_pnts.loc[
                        (c > 1) & (bridge_pnts.feature_id != bridge_pnts.crossing_feature_id), 'is_backwater'
                    ] = 1
                    # Write file
                    bridge_pnts = bridge_pnts.astype(self.bridge_dtypes, errors='ignore')

                    # Set the CRS if it is not already set
                    huc2Identifier = huc_id[:2]
                    if bridge_pnts.crs is None:
                        # Alaska
                        if huc2Identifier == '19':
                            bridge_pnts.set_crs(ALASKA_CRS , inplace=True)
                        else:
                            bridge_pnts.set_crs(DEFAULT_FIM_PROJECTION_CRS, inplace=True)
                    bridge_pnts.to_file(bridge_pnts_file, index=False, engine='fiona')

            # print(f"agg_by_huc for huc id {huc_id} is done")

        except Exception:
            errMsg = (
                "--------------------------------------"
                f"\n huc_id {huc_id} has an error - outside multi proc\n"
            )
            errMsg = errMsg + traceback.format_exc()
            print(errMsg, flush=True)
            log_error(
                self.fim_directory,
                usgs_elev_flag,
                hydro_table_flag,
                src_cross_flag,
                ras_elev_flag,
                bridge_flag,
                huc_id,
                errMsg,
            )


# ==============================
# This is done independantly in each worker and does not attempt to write to a shared file
# as those can collide with multi proc
def log_error(
    fim_directory,
    usgs_elev_flag,
    hydro_table_flag,
    src_cross_flag,
    ras_elev_flag,
    bridge_flag,
    huc_id,
    errMsg,
):
    file_name = f"agg_by_huc_{huc_id}"
    if usgs_elev_flag:
        file_name += "_elev"
    if hydro_table_flag:
        file_name += "_hydro"
    if src_cross_flag:
        file_name += "_src_cross"
    if ras_elev_flag:
        file_name += "_ras"
    if bridge_flag:
        file_name += "_bridge"
    file_name += "_error.log"

    log_path = os.path.join(fim_directory, "logs", "agg_by_huc_errors")
    file_path = os.path.join(log_path, file_name)

    f = open(file_path, "a")
    f.write(errMsg)
    f.close()


def aggregate_by_huc(
    fim_directory,
    fim_inputs,
    usgs_elev_flag,
    hydro_table_flag,
    src_cross_flag,
    ras_elev_flag,
    bridge_flag,
    num_job_workers,
):
    assert os.path.isdir(fim_directory), f'{fim_directory} is not a valid directory'

    # -------------------
    # Validation
    total_cpus_available = os.cpu_count() - 2
    if num_job_workers > total_cpus_available:
        raise ValueError(
            f'The number of jobs {num_job_workers}'
            ' exceeds your machine\'s available CPU count minus two.'
            ' Please lower the number of jobs'
            ' values accordingly.'
        )

    # create log folder, might end up empty but at least create the folder
    # Yes.. this is duplicate in the log function
    log_folder = os.path.join(fim_directory, "logs", "agg_by_huc_errors")
    if os.path.exists(log_folder) is False:
        os.mkdir(log_folder)
    else:
        # empty only ones with this type (we want to keep others that
        # might have been called with different types. aka.. once for -elev
        # and another for -hydro)
        agg_type = ""
        if usgs_elev_flag:
            agg_type += "_elev"
        if hydro_table_flag:
            agg_type += "_hydro"
        if src_cross_flag:
            agg_type += "_src_cross"
        if ras_elev_flag:
            agg_type += "_ras"
        if bridge_flag:
            agg_type += "_bridge"
        filelist = glob.glob(os.path.join(log_folder, f"*{agg_type}*"))
        for f in filelist:
            os.remove(f)

    start_time = datetime.now()
    dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print(f"started: {dt_string}")

    # Set up multiprocessor
    with ProcessPoolExecutor(max_workers=num_job_workers) as executor:
        # Loop through applicable HUCs, build the agg_function arguments, and submit them to the process pool
        executor_dict = {}

        try:
            if fim_inputs:
                fim_inputs_csv = pd.read_csv(fim_inputs, header=None, names=['huc', 'levpa_id'], dtype=str)
                huc_list = fim_inputs_csv.huc.unique()

                # with multi proc, it won't be 100% in order as different hucs
                # process faster, but it does help a little
                huc_list_sorted = sorted(huc_list)
                for huc_id in huc_list_sorted:
                    branches = fim_inputs_csv.loc[fim_inputs_csv.huc == huc_id, 'levpa_id'].tolist()
                    huc_dir = HucDirectory(fim_directory, huc_id, limit_branches=branches)

                    args_agg = {
                        'usgs_elev_flag': usgs_elev_flag,
                        'hydro_table_flag': hydro_table_flag,
                        'src_cross_flag': src_cross_flag,
                        'ras_elev_flag': ras_elev_flag,
                        'bridge_flag': bridge_flag,
                        'huc_id': huc_id,
                    }

                    future = executor.submit(huc_dir.agg_function, **args_agg)
                    executor_dict[future] = huc_id

            else:
                huc_list = [d for d in os.listdir(fim_directory) if re.match(r'\d{8}', d)]

                # with multi proc, it won't be 100% in order as different hucs
                # process faster, but it does help a little
                huc_list_sorted = sorted(huc_list)
                for huc_id in huc_list_sorted:
                    if huc_id.isnumeric() is False:
                        continue

                    huc_dir = HucDirectory(fim_directory, huc_id)

                    args_agg = {
                        'usgs_elev_flag': usgs_elev_flag,
                        'hydro_table_flag': hydro_table_flag,
                        'src_cross_flag': src_cross_flag,
                        'ras_elev_flag': ras_elev_flag,
                        'bridge_flag': bridge_flag,
                        'huc_id': huc_id,
                    }
                    future = executor.submit(huc_dir.agg_function, **args_agg)
                    executor_dict[future] = huc_id

        except Exception:
            errMsg = (
                "--------------------------------------"
                f"\n huc_id {huc_id} has an error - outside multi proc\n"
            )
            errMsg = errMsg + traceback.format_exc()
            print(errMsg, flush=True)
            log_error(
                fim_directory,
                usgs_elev_flag,
                hydro_table_flag,
                src_cross_flag,
                ras_elev_flag,
                bridge_flag,
                huc_id,
                errMsg,
            )
            # sys.exit(1)

        # Send the executor to the progress bar and wait for all MS tasks to finish
        progress_bar_handler(executor_dict, f"Running aggregate_by_huc with {num_job_workers} workers")

    end_time = datetime.now()
    dt_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print(f"ended: {dt_string}")

    # Calculate duration
    time_duration = end_time - start_time
    print(f"Duration: {str(time_duration).split('.')[0]}")
    print()


if __name__ == '__main__':
    # Note: This processing is done here for all hucs and branches at once instead of
    # at each individual huc processing due to some hydrotable's being adjusted
    # in post processing.

    parser = argparse.ArgumentParser(description='Aggregates usgs_elev_table.csv at the HUC level')
    parser.add_argument('-fim', '--fim_directory', help='Input FIM Directory', required=True)
    parser.add_argument('-i', '--fim_inputs', help='Input fim_inputs CSV file', required=False)
    parser.add_argument(
        '-elev',
        '--usgs_elev_flag',
        help='Perform aggregate on branch usgs elev tables',
        required=False,
        default=False,
        action='store_true',
    )
    parser.add_argument(
        '-htable',
        '--hydro_table_flag',
        help='Perform aggregate on branch hydrotables',
        required=False,
        default=False,
        action='store_true',
    )
    parser.add_argument(
        '-src',
        '--src_cross_flag',
        help='Perform aggregate on branch src crosswalk files',
        required=False,
        default=False,
        action='store_true',
    )
    parser.add_argument(
        '-ras',
        '--ras_elev_flag',
        help='Perform aggregate on branch ras2fim elev tables',
        required=False,
        default=False,
        action='store_true',
    )
    parser.add_argument(
        '-bridge',
        '--bridge_flag',
        help='Perform aggregate on branch bridge centroid files',
        required=False,
        default=False,
        action='store_true',
    )
    parser.add_argument(
        '-j', '--num_job_workers', help='Number of processes to use', required=False, default=1, type=int
    )

    args = vars(parser.parse_args())

    aggregate_by_huc(**args)

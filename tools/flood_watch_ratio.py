import argparse
import datetime as dt
import os
import traceback
from multiprocessing import cpu_count

import geopandas as gpd
import numpy as np
import pandas as pd
from dotenv import load_dotenv

from src.utils.shared_functions import FIM_Helpers as fh
from src.utils.shared_functions import run_with_mp, setup_mp_file_logger

# catchment vectorized
def process_catchments(group):
    high_flow_nbm = group['high_flow_nbm'].iloc[0]
    high_flow = group['high_flow'].iloc[0]
    # sort by discharge for interpolation
    sorted_group = group.sort_values('discharge_cms')
    return_area_nbm = np.interp(
        high_flow_nbm, sorted_group['discharge_cms'], sorted_group['SurfaceArea (m2)']
    )
    return_area_nrp = np.interp(high_flow, sorted_group['discharge_cms'], sorted_group['SurfaceArea (m2)'])
    return pd.Series(
        {
            'HydroID': group['HydroID'].iloc[0],
            'branch_id': group['branch_id'].iloc[0],
            'HUC': group['HUC'].iloc[0],
            'feature_id': group['feature_id'].iloc[0],
            'SurfaceArea_nbm': return_area_nbm,
            'SurfaceArea_nrp': return_area_nrp,
        }
    )


def process_huc(huc, nbm_df_bflows, df_bflows, flow_huc12, fim_dir, output_dir, file_logger, screen_queue, task_id):
    """
    Process a HUC to calculate surface area ratios.
    
    Parameters
    -----------
    huc : str
        The HUC to process
    nbm_df_bflows : DataFrame
        NBM flow data
    df_bflows : DataFrame
        NWM high water threshold data
    flow_huc12 : DataFrame
        File with catchments and feature_id to HUC12 mapping
    fim_dir : str
        Directory containing FIM hydrofabric data
    output_dir : str
        Directory to save output data
    file_logger : logging.Logger
        Logger for file output
    screen_queue : multiprocessing.Queue
        Queue for screen updates
    task_id : str
        Task identifier (HUC8)
    """
    file_logger.info(f"Started processing {task_id}")
    # screen_queue.put(f'Processing HUC {task_id}')
    try:
        hydrotable_path = f'{fim_dir}/{huc}/hydrotable.csv'
        if not os.path.exists(hydrotable_path):
            file_logger.warning(f'Skipping HUC {huc}, hydrotable not found')
            screen_queue.put(f'Skipping HUC {huc}, hydrotable not found')
            return False

        hydrotable = pd.read_csv(hydrotable_path, low_memory=False)
        # merge with the flow data
        nbm_df_bflows['feature_id'] = nbm_df_bflows['feature_id'].astype('int64')
        nbm_ht = hydrotable.merge(nbm_df_bflows, how='left', on='feature_id')
        nrp_nbm_ht = nbm_ht.merge(df_bflows, how='left', on='feature_id')
        water_table = nrp_nbm_ht.groupby(['branch_id', 'HydroID']).apply(process_catchments).reset_index(drop=True)
        # Add HUC12 information 
        huc12_df = water_table.merge(
            flow_huc12[['HydroID', 'feature_id', 'HUC12', 'branch_id']],
            on=['HydroID', 'feature_id', 'branch_id'],
            how='left',
        )
        # Filter out rows missing surface area data
        valid_sur = huc12_df.dropna(subset=['SurfaceArea_nrp', 'SurfaceArea_nbm'])
        aggreagtion = valid_sur.groupby(['HUC12', 'branch_id'], as_index=False).agg(
            {'SurfaceArea_nbm': lambda x: np.nanmean(x), 'SurfaceArea_nrp': lambda x: np.nanmean(x)}
        )

        aggregate_final = (
            aggreagtion.groupby('HUC12')
            .agg({'SurfaceArea_nbm': lambda x: np.nanmean(x), 'SurfaceArea_nrp': lambda x: np.nanmean(x)})
            .reset_index()
        )

        aggregate_final['ratio'] = aggregate_final['SurfaceArea_nbm'] / aggregate_final['SurfaceArea_nrp']

        output_file = f'{output_dir}/temp/water_table_{huc}.csv'
        aggregate_final.to_csv(output_file, index=False)
        file_logger.info(f"Completed processing {task_id}")
        return True
    except Exception as e:
        file_logger.error(f"Exception in {task_id}: {str(e)}")
        screen_queue.put(f"Failed HUC {task_id}: {str(e)}")
        return False

def main(args):
    """
    Main function to run HUC processing.

    Parameters:
    -----------
    args: Parsed command-line arguments
    """
    start_time = dt.datetime.now(dt.timezone.utc)
    with open(args.huc_file, 'r') as f:
        huc_list = sorted([line.strip() for line in f])

    srcDir = os.getenv('srcDir')
    load_dotenv(f'{srcDir}/bash_variables.env')
    # Read the data
    catchments_to_huc12 = os.getenv('input_catchments_to_huc12')
    flow_huc12 = pd.read_csv(catchments_to_huc12, dtype={'HUC12': 'string'})
    flow_huc12['HUC12'] = flow_huc12['HUC12'].astype(str).str.strip()
    flow_huc12['HUC12'] = flow_huc12['HUC12'].str.zfill(12)

    nbm_high_flow = gpd.read_file(args.nbm_file)
    nbm_df_bflows = nbm_high_flow[['feature_id', 'discharge']].rename(columns={'discharge': 'high_flow_nbm'})
    nbm_df_bflows['feature_id'] = nbm_df_bflows['feature_id'].astype('int64')
    # Convert to cms
    # nbm_df_bflows['high_flow_nbm'] = (nbm_df_bflows['high_flow_nbm'] * 0.028316847)

    high_flow_file = pd.read_csv(args.nwm_file, usecols=['feature_id', 'discharge'])
    df_bflows = high_flow_file.rename(columns={'discharge': 'high_flow'})
    df_bflows['high_flow'] = df_bflows['high_flow']
    # Create temp directory
    output_dir = args.output_dir
    os.makedirs(f'{output_dir}/temp', exist_ok=True)
    # Set up logger
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_path = os.path.join(output_dir, f"process_log-{file_dt_string}.log")
    file_logger = setup_mp_file_logger(log_file_path)
    try:
        print("==================================")
        file_logger.info("Started the process")
        print("Started the process")
        print("")
        print("*** NOTE: This will generate flood watch ratio")
        print("")
        file_logger.info(f"   Start time (UTC): {start_time.strftime('%m/%d/%Y %H:%M:%S')}")
        print(f"   Start time (UTC): {start_time.strftime('%m/%d/%Y %H:%M:%S')}")
        print("")

        # Prepare task arguments list
        tasks_args_list = []
        for huc in huc_list:
            if not huc.isdigit() or len(huc) != 8:
                file_logger.error(f"Skipping invalid HUC8: {huc}")
                print(f"Skipping invalid HUC8: {huc}")
                continue
            tasks_args_list.append(
                {
                    "huc": huc,
                    "nbm_df_bflows": nbm_df_bflows,
                    "df_bflows": df_bflows,
                    "flow_huc12": flow_huc12,
                    "output_dir": output_dir,
                    "fim_dir": args.fim_dir
                }
            )
        # Run multiprocessing
        results = run_with_mp(
            task_function=process_huc,
            tasks_args_list=tasks_args_list,
            file_logger=file_logger,
            max_workers=args.job_number,
            task_id_key='huc',
            exit_on_failure=False,
            show_progress=False,  # Disables the progress bar display
        )

        # Collect successful results
        successful_hucs = [huc for huc, status in results.items() if status]
        output_files = [f"{output_dir}/temp/water_table_{huc}.csv" for huc in successful_hucs]
        # Save final output
        if output_files:
            all_hucs_final = pd.concat([pd.read_csv(f) for f in output_files], axis=0, ignore_index=True)
            all_hucs_final.to_csv(f"{output_dir}/final_output.csv", index=False)
            # Clean up temp files
            for f in output_files:
                os.remove(f)
        else:
            print("No valid results to concatenate.")
            file_logger.info("No valid results to concatenate.")
        
        # Log summary
        failed_hucs = [huc for huc, status in results.items() if not status]
        if not failed_hucs:
            file_logger.info("All multiprocessing tasks Succeeded")
            print("All multiprocessing tasks Succeeded")
        else:
            file_logger.info(f"{len(failed_hucs)} failed:")
            print(f"{len(failed_hucs)} failed:")
            for huc in failed_hucs:
                file_logger.info(f"  - {huc}")
                print(f"  - {huc}")
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


if __name__ == '__main__':

    """
    This script processes a list of HUC8 to calculate the ratio of surface areas
        from NWM high water threshold (NRP) and the NBM for flood watch workflow.

    Example Usage
    ----------
    python3 /foss_fim/tools/flood_watch_ratio.py -huc /input/Flood_watch/huc_list_test.txt
        -d /data/previous_fim/hand_4_7_4_0 -nbm /projects/Flood_watch/20250402T1519Z_mrf_nbm_5day_max_high_flow_magnitude.csv
        -nwm /data/inputs/rating_curve/bankfull_flows/nwm3_high_water_threshold_cms.csv -out /outputs/fl_watch
    """
    parser = argparse.ArgumentParser(
        description='Process HUCs for NBM and NRP surface area and calculating ratio value required for flood watch workflow.'
    )
    parser.add_argument(
        '-huc', '--huc_file', required=True, help='Path to the text file containing list of HUCs'
    )
    parser.add_argument(
        "-d",
        "--fim_dir",
        required=True,
        help="Directory path to FIM hydrofabric by processing unit.",
        type=str,
    )
    parser.add_argument("-nbm", "--nbm_file", required=True, help="path to NBM high flow csv")
    parser.add_argument("-nwm", "--nwm_file", required=True, help="path to NWM high flow csv")
    parser.add_argument("-out", "--output_dir", required=True, help="path to save output")
    parser.add_argument(
        "-j",
        "--job_number",
        required=False,
        type=int,
        default=min(8, cpu_count()),
        help='Optional, (default: min(8, CPU cores))',
    )
    args = parser.parse_args()

    main(args)

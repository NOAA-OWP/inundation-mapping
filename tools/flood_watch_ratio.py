import argparse
import multiprocessing as mp
import os
import time
from multiprocessing import Pool, cpu_count

import geopandas as gpd
from dotenv import load_dotenv
import numpy as np
import pandas as pd


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


def process_huc(huc, nbm_df_bflows, df_bflows, huc_index, total_hucs, flow_huc12, output_dir):

    print(f'Processing HUC {huc} ({huc_index + 1}/{total_hucs})')
    hydrotable_path = f'{args.fim_dir}/{huc}/hydrotable.csv'
    if not os.path.exists(hydrotable_path):
        print(f'skipping HUC {huc}, hydrotable not found')
        return None
    hydrotable = pd.read_csv(hydrotable_path, low_memory=False)
    # branch_list = hydrotable['branch_id'].unique().tolist()
    # merge with the flow data
    nbm_df_bflows['feature_id'] = nbm_df_bflows['feature_id'].astype('int64')
    df_src1 = hydrotable.merge(nbm_df_bflows, how='left', on='feature_id')
    df_src = df_src1.merge(df_bflows, how='left', on='feature_id')
    water_table = df_src.groupby(['branch_id', 'HydroID']).apply(process_catchments).reset_index(drop=True)
    huc12_df = water_table.merge(
        flow_huc12[['HydroID', 'feature_id', 'HUC12', 'branch_id']],
        on=['HydroID', 'feature_id', 'branch_id'],
        how='left',
    )
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
    return output_file


def process_chunk(
    huc_chunk, nbm_df_bflows, df_bflows, total_hucs, chunk_idx, flow_huc12, output_dir, args
):
    print(f"Processing chunk {chunk_idx + 1} with {len(huc_chunk)} HUCs")
    job_number = args.job_number
    with Pool(processes=job_number) as pool:
        result_files = pool.starmap(
            process_huc,
            [
                (huc, nbm_df_bflows, df_bflows, idx, total_hucs, flow_huc12, output_dir)
                for idx, huc in enumerate(huc_chunk)
            ],
        )

    # Filter out None results (skipped HUCs) and concatenate this chunk
    valid_files = [f for f in result_files if f is not None]
    if valid_files:
        chunk_df = pd.concat([pd.read_csv(f) for f in valid_files], axis=0, ignore_index=True)
        # Save this chunk's result
        chunk_output = f'{output_dir}/temp/chunk_{chunk_idx}.csv'
        chunk_df.to_csv(chunk_output, index=False)
        # Clean up temporary HUC files
        for f in valid_files:
            os.remove(f)
        return chunk_output
    return None


def main(args):
    total_start_time = time.time()
    with open(args.huc_file, 'r') as f:
        huc_list = [line.strip() for line in f]
    total_hucs = len(huc_list)

    srcDir = os.getenv('srcDir')
    load_dotenv(f'{srcDir}/bash_variables.env')
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
    os.makedirs(f'{args.output_dir}/temp', exist_ok=True)
    # Define chunking parameters
    chunk_size = args.chunk_size
    huc_chunks = [huc_list[i : i + chunk_size] for i in range(0, len(huc_list), chunk_size)]
    chunk_results = []
    # Process remaining chunks
    for chunk_idx, huc_chunk in enumerate(huc_chunks):
        chunk_file = process_chunk(
            huc_chunk,
            nbm_df_bflows,
            df_bflows,
            total_hucs,
            chunk_idx,
            flow_huc12,
            args.output_dir,
            args,
        )
        if chunk_file:
            chunk_results.append(chunk_file)
    # Combine all chunk files
    if chunk_results:
        all_hucs_finals = pd.concat([pd.read_csv(f) for f in chunk_results], axis=0, ignore_index=True)
        all_hucs_finals.to_csv(f'{args.output_dir}/final_output_0402.csv', index=False)
        # Clean up chunk files
        for f in chunk_results:
            os.remove(f)
    else:
        print("No valid results to concatenate.")

    total_end_time = time.time()
    total_time = total_end_time - total_start_time

    print(f"total prrocessing time: {total_time / 60:.2f} minutes!")


if __name__ == '__main__':
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
        "-chunk", "--chunk_size", type=int, required=False, default=20, help="Number of HUCs per chunk"
    )
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

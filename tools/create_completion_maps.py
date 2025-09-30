import argparse
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd


WBD_PATH = r'/data/inputs/wbd/WBD_National.gpkg'
# WBD_PATH = r'/efs/fim-data/hand_fim/inputs/wbd/WBD_National_South_Alaska_WBDHU12.gpkg'

HUC_NUMBER = 8
HUC_LEVEL = f'HUC{HUC_NUMBER}'

PW_INSTANCE_TYPE_DICT = {
    "r6a.2xlarge": 0.47,
    "r6a.4xlarge": 0.92,
    "r6a.8xlarge": 1.83,
    "r6a.16xlarge": 3.63,
    "r7a.8xlarge": 2.45,
    "r7iz.8xlarge": 2.99,
    "r7i.8xlarge": 2.13,
    "c6i.2xlarge": 0.35,
    "c6i.4xlarge": 0.69,
    "c6i.8xlarge": 1.37,
    "c6i.24xlarge": 4.09,
    "c7i.12xlarge": 2.16,
    "m7i.12xlarge": 2.43,
}


def join_dict_to_geopackage(dict_data):
    # Define the geopackage layer
    layer = f'WBDHU{HUC_NUMBER}'
    print(f"Loading {layer} layer from {WBD_PATH} ")
    print("This may take a few minutes...")

    gdf = gpd.read_file(WBD_PATH, layer=layer)

    # Convert the dictionary to a DataFrame
    df = pd.DataFrame.from_dict(dict_data, orient='index')

    # Apply the correct data types
    df = df.apply(lambda row: apply_data_types(row), axis=1)

    # Make sure the index is a column in the DataFrame, this will be our HUC_LEVEL
    df[HUC_LEVEL] = df.index

    # Merge the geodataframe with the dataframe
    merged_gdf = gdf.merge(df, on=HUC_LEVEL, how='left')

    # print(merged_gdf.columns)

    return merged_gdf


'''
def parse_log_to_dict(filepath):

    with open(filepath, 'r') as file:
        # Read the file contents
        lines = file.readlines()

    # The first line contains the headers
    headers = lines[0].strip().split('\t')

    # Initialize the dictionary to hold the parsed data
    log_dict = {}
   # Process each line except the first (header line)
    for line in lines[1:]:
        fields = [field.strip() for field in line.strip().split('\t')]
        # Extract the last 8 digits from the Command field as the key (huc8)
        huc8 = fields[-1].split()[-1]
        # Create a dictionary for the values excluding the huc8
        entry = dict(zip(headers[:-1], fields[:-1]))
        # Add the command without huc8 as a separate field
        entry['Command'] = ' '.join(fields[-1].split()[:-1])
        # Insert into the dictionary using huc8 as the key
        log_dict[huc8] = entry

    return log_dict
'''


def parse_log(file_path):
    data = {}
    # Define the keys that correspond to the data fields we are interested in
    keys = [
        "Command being timed",
        "User time (seconds)",
        "System time (seconds)",
        "Percent of CPU this job got",
        "Elapsed (wall clock) time (h:mm:ss or m:ss)",
        "Average shared text size (kbytes)",
        "Average unshared data size (kbytes)",
        "Average stack size (kbytes)",
        "Average total size (kbytes)",
        "Maximum resident set size (kbytes)",
        "Average resident set size (kbytes)",
        "Major (requiring I/O) page faults",
        "Minor (reclaiming a frame) page faults",
        "Voluntary context switches",
        "Involuntary context switches",
        "Swaps",
        "File system inputs",
        "File system outputs",
        "Socket messages sent",
        "Socket messages received",
        "Signals delivered",
        "Page size (bytes)",
        "Exit status",
    ]

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        # Reverse the list to start checking from the bottom of the file
        for line in reversed(lines):
            for key in keys:
                # Check if the current line starts with the key
                if line.strip().startswith(key):
                    # Split the line on the colon and strip any whitespace, then store the value
                    value = line.split(':', 1)[1].strip()
                    data[key] = value
                    # Remove the key from the list once its value is found
                    keys.remove(key)
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

    return data


# Use two crawl directory functions depending on num_threads.
# The single threaded version is kept solely for ease of maintenance.
def crawl_directory_single_threaded(directory):
    results = {}
    hung_files = []

    print(f"Scraping data from log files in {directory}")
    # Loop through each log file in the directory
    for filename in os.listdir(directory):
        # Check if the file name matches the expected format using regex
        if re.match(fr'\d{{{HUC_NUMBER}}}_unit\.log', filename):
            huc = filename[:HUC_NUMBER]  # The first n digits of the filename
            # print(f"Scraping log files for {huc}" )
            file_path = os.path.join(directory, filename)

            parsed_data = parse_log(file_path)
            if parsed_data:
                results[huc] = parsed_data
            else:
                print(f"Unable to parse data from {file_path}")
                hung_files.append(filename)
                # results[huc] =
    return results, hung_files


def crawl_directory_multi_threaded(directory, num_threads):
    results = {}
    hung_files = []

    print(f"Scraping data from log files in {directory}")

    # List all matching files and their HUCs
    log_files = []
    for filename in os.listdir(directory):
        if re.match(fr'\d{{{HUC_NUMBER}}}_unit\.log', filename):
            huc = filename[:HUC_NUMBER]
            file_path = os.path.join(directory, filename)
            log_files.append((huc, file_path, filename))

    def parse_and_return(huc, file_path, filename):
        parsed_data = parse_log(file_path)
        return (huc, parsed_data, filename)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future_to_info = {
            executor.submit(parse_and_return, huc, file_path, filename): (huc, filename)
            for huc, file_path, filename in log_files
        }
        for future in as_completed(future_to_info):
            huc, filename = future_to_info[future]
            try:
                _, parsed_data, _ = future.result()
                if parsed_data:
                    results[huc] = parsed_data
                else:
                    print(f"Unable to parse data from {filename}")
                    hung_files.append(filename)
            except Exception as exc:
                print(f"Exception occurred while parsing {filename}: {exc}")
                hung_files.append(filename)

    return results, hung_files


def update_log_with_costs(log_dict, cost_per_hour):

    # Assign total counter variables
    total_wc_time_hours = 0
    total_run_cost = 0

    for key, entry in log_dict.items():
        # Initialize default costs
        entry['system_cost_usd'] = None
        entry['user_cost_usd'] = None
        entry['elapsed_wall_clock_cost_usd'] = None

        # Calculate cost based on system time
        if "System time (seconds)" in entry:
            try:
                system_time_minutes = float(entry["System time (seconds)"]) / 60
                system_time_hours = float(entry["System time (seconds)"]) / 3600
                system_cost = system_time_hours * cost_per_hour
                entry['system_cost_usd'] = round(system_cost, 2)  # rounding to two decimal places
                entry['system_mins'] = round(system_time_minutes, 2)  # rounding to two decimal places
            except ValueError:
                print(f"Error converting system time to float for entry {key}")

        # Calculate cost based on user time
        if "User time (seconds)" in entry:
            try:
                user_time_minutes = float(entry["User time (seconds)"]) / 60
                user_time_hours = float(entry["User time (seconds)"]) / 3600
                user_cost = user_time_hours * cost_per_hour
                entry['user_cost_usd'] = round(user_cost, 2)  # rounding to two decimal places
                entry['user_mins'] = round(user_time_minutes, 2)
            except ValueError:
                print(f"Error converting user time to float for entry {key}")

        # Calculate cost based on Elapsed wall clock time
        if "Elapsed (wall clock) time (h:mm:ss or m:ss)" in entry:
            try:
                raw_time_str = entry["Elapsed (wall clock) time (h:mm:ss or m:ss)"]
                time_str = raw_time_str.split('): ')[
                    -1
                ]  # Taking the last part after '): ' to ensure we get the time

                # Split the time string into its components
                parts = time_str.split(':')

                if '.' in parts[-1]:
                    seconds, fractions = parts[-1].split('.')
                else:
                    seconds = parts[-1]
                    fractions = "0"

                # Calculate the total seconds based on the number of parts
                total_seconds = float(fractions) / 100 + int(seconds)
                if len(parts) == 3:
                    # h:mm:ss format
                    total_seconds += int(parts[1]) * 60 + int(parts[0]) * 3600
                elif len(parts) == 2:
                    # m:ss format
                    total_seconds += int(parts[0]) * 60

                elapsed_wc_time_minutes = total_seconds / 60
                elapsed_wc_time_hours = total_seconds / 3600
                total_wc_time_hours += round(elapsed_wc_time_hours, 2)
                elapsed_wc_cost = elapsed_wc_time_hours * cost_per_hour
                entry['elapsed_wall_clock_cost_usd'] = round(
                    elapsed_wc_cost, 2
                )  # rounding to two decimal places
                total_run_cost += round(elapsed_wc_cost, 2)
                # print(f"Cost for {key} is ${round(elapsed_wc_cost, 2)}")
                entry['elapsed_wall_clock_mins'] = round(elapsed_wc_time_minutes, 2)
            except ValueError:
                print(f"Error converting user time to float for entry {key}")

    print(f"Total elapsed Wall Clock Time (hours): {round(total_wc_time_hours, 2)}")
    print(f"Total Run Cost: ${round(total_run_cost, 2)}")
    return log_dict


def apply_data_types(data):
    # Define a dictionary with the correct data types for each field
    data_types = {
        "User time (seconds)": float,
        "System time (seconds)": float,
        "Percent of CPU this job got": int,
        "Elapsed (wall clock) time (h:mm:ss or m:ss)": str,
        "Average shared text size (kbytes)": int,
        "Average unshared data size (kbytes)": int,
        "Average stack size (kbytes)": int,
        "Average total size (kbytes)": int,
        "Maximum resident set size (kbytes)": int,
        "Average resident set size (kbytes)": int,
        "Major (requiring I/O) page faults": int,
        "Minor (reclaiming a frame) page faults": int,
        "Voluntary context switches": int,
        "Involuntary context switches": int,
        "Swaps": int,
        "File system inputs": int,
        "File system outputs": int,
        "Socket messages sent": int,
        "Socket messages received": int,
        "Signals delivered": int,
        "Page size (bytes)": int,
        "Exit status": str,
    }

    for key, value in data_types.items():
        if key in data:
            try:
                # Special handling for percent field to strip the '%' character
                if key == "Percent of CPU this job got":
                    data[key] = int(data[key].replace('%', '').strip())
                else:
                    data[key] = value(data[key])
            except ValueError:
                data[key] = None  # Handle conversion errors by setting to None or a default value
    return data


# Calculate storage and storage costs
def calculate_storage(log_dict, unit_log_dir, efs_storage_cost_monthly=None, s3_cost_monthly=None):

    # Initialize total storage cost variables
    total_efs_storage_cost_monthly = 0
    total_s3_storage_cost_monthly = 0

    outputs_dir_derived = os.path.dirname(os.path.dirname(unit_log_dir))
    for huc in log_dict:
        huc_outputs = os.path.join(outputs_dir_derived, huc)
        if os.path.exists(huc_outputs):
            root_directory = Path(huc_outputs)
            dir_size_gb = round(
                sum(f.stat().st_size for f in root_directory.glob('**/*') if f.is_file()) / (1024**3), 2
            )

            if efs_storage_cost_monthly is not None:
                huc_storage_efs_cost_per_month_usd = dir_size_gb * efs_storage_cost_monthly
                total_efs_storage_cost_monthly += round(huc_storage_efs_cost_per_month_usd, 2)
                huc_storage_efs_cost_per_3month_usd = huc_storage_efs_cost_per_month_usd * 3.0
                huc_storage_efs_cost_per_6month_usd = huc_storage_efs_cost_per_month_usd * 6.0
            else:
                huc_storage_efs_cost_per_month_usd = np.nan
                huc_storage_efs_cost_per_3month_usd = np.nan
                huc_storage_efs_cost_per_6month_usd = np.nan

            if s3_cost_monthly is not None:
                huc_storage_s3_cost_per_month_usd = dir_size_gb * s3_cost_monthly
                total_s3_storage_cost_monthly += round(huc_storage_s3_cost_per_month_usd, 2)
                huc_storage_s3_cost_per_3month_usd = huc_storage_s3_cost_per_month_usd * 3.0
                huc_storage_s3_cost_per_6month_usd = huc_storage_s3_cost_per_month_usd * 6.0
            else:
                huc_storage_s3_cost_per_month_usd = np.nan
                huc_storage_s3_cost_per_3month_usd = np.nan
                huc_storage_s3_cost_per_6month_usd = np.nan

        else:
            # write message to dict
            dir_size_gb = np.nan

        # Update log_dict
        log_dict[huc]['storage_rate_monthly'] = efs_storage_cost_monthly
        log_dict[huc]['dir_size_gb'] = dir_size_gb
        log_dict[huc]['efs_storage_cost_month_usd'] = huc_storage_efs_cost_per_month_usd
        log_dict[huc]['efs_storage_cost_3month_usd'] = huc_storage_efs_cost_per_3month_usd
        log_dict[huc]['efs_storage_cost_6month_usd'] = huc_storage_efs_cost_per_6month_usd
        log_dict[huc]['s3_storage_cost_month_usd'] = huc_storage_s3_cost_per_month_usd
        log_dict[huc]['s3_storage_cost_3month_usd'] = huc_storage_s3_cost_per_3month_usd
        log_dict[huc]['s3_storage_cost_6month_usd'] = huc_storage_s3_cost_per_6month_usd

    if efs_storage_cost_monthly is not None:
        print(f"Total EFS Storage Cost per Month: ${round(total_efs_storage_cost_monthly, 2)}")
    else:
        print("No EFS storage cost value provided.")

    if s3_cost_monthly is not None:
        print(f"Total S3 Storage Cost per Month:  ${round(total_s3_storage_cost_monthly, 2)}")
    else:
        print("No S3 storage cost value provided.")

    return log_dict


# Count candidate branches and actually generated branches
def count_branches(log_dict, unit_log_dir):
    outputs_dir_derived = os.path.dirname(os.path.dirname(unit_log_dir))
    for huc in log_dict:
        candidate_branch_list = []
        created_branch_list = []

        # Calculate candidate branches
        try:
            branch_ids_csv = os.path.join(outputs_dir_derived, huc, "branch_ids.csv")
            df = pd.read_csv(branch_ids_csv, header=None)
            candidate_branch_list = df.iloc[:, 1].tolist()
            log_dict[huc]['candidate_branches_available'] = 'yes'
        except FileNotFoundError:
            log_dict[huc]['candidate_branches_available'] = 'no'

        # Calculate created branches
        try:
            huc_branch_outputs = os.path.join(outputs_dir_derived, huc, "branches")
            created_branch_list = os.listdir(huc_branch_outputs)
            branches_created_count = len(created_branch_list)
        except FileNotFoundError:
            pass

        candidate_branch_count = len(candidate_branch_list)

        # Update log_dict
        log_dict[huc]['candidate_branches'] = candidate_branch_count
        log_dict[huc]['created_branches'] = branches_created_count

        # Convert both lists to sets of strings
        candidate_branch_set = set(str(branch) for branch in candidate_branch_list)
        created_branch_set = set(created_branch_list)

        # Find items in candidate_branch_set not in created_branch_set
        missing_items = created_branch_set - candidate_branch_set

        candidate_vs_created = len(missing_items)

        log_dict[huc]['created_minus_candidate_branches'] = candidate_vs_created
        log_dict[huc]['missing_branches'] = ', '.join(sorted(missing_items))

    return log_dict


def scrape_traceback_for_non_zeros(log_dict, unit_log_dir):

    for huc in log_dict:
        if int(log_dict[huc]['Exit status']) != 0:
            huc_log_file = os.path.join(unit_log_dir, huc + "_unit.log")
            if os.path.exists(huc_log_file):
                with open(huc_log_file, 'r') as file:
                    lines = file.readlines()
                    # Get the last 41 lines; if file has fewer than 50 lines, it gets all of them
                    traceback = lines[-41:] if len(lines) > 41 else lines
                    # Converting list of lines to a single string
                    traceback_string = ''.join(traceback)
        else:
            traceback_string = ""
        log_dict[huc]['traceback'] = traceback_string

    return log_dict


def get_max_memory_in_gb(log_dict):

    # Set "counter" for largest memory used
    largest_memory_used = 0

    for huc in log_dict:
        if int(log_dict[huc]['Maximum resident set size (kbytes)']) != 0:
            max_gigabytes = int(log_dict[huc]["Maximum resident set size (kbytes)"]) / 1048576
            max_gigabytes = round(max_gigabytes, 2)
            # print(f"Max memory for {huc} is {max_gigabytes}")
            if max_gigabytes > largest_memory_used:
                largest_memory_used = max_gigabytes
                largest_memory_used_huc = huc
        else:
            max_gigabytes = ""
        # log_dict[huc]['max_gb'] = max_gigabytes

    print(
        f"Most memory used for all {HUC_LEVEL}s is {largest_memory_used} GBs from {largest_memory_used_huc}"
    )

    return log_dict


def generate_geopackage(
    instance_type, unit_log_dir, output_file, efs_storage_cost_monthly, s3_cost_monthly, num_threads
):

    # log_dict = parse_log_to_dict(pipeline_summary_unit_file)

    # If multithreading is desired, use appropriate crawl directory method
    if num_threads < 1:
        log_dict, hung_files = crawl_directory_single_threaded(unit_log_dir)
    else:
        log_dict, hung_files = crawl_directory_multi_threaded(unit_log_dir, num_threads)

    # Optionally print parsed data from HUC level unit log files
    # from pprint import pprint
    # pprint(log_dict)

    try:
        cost_per_hour = PW_INSTANCE_TYPE_DICT[instance_type]
    except KeyError:
        print("Instance type isn't recognized")
        quit()

    # Calculate maximum memory usage
    get_max_memory_in_gb(log_dict)

    # Calculate cost
    if instance_type is not None:
        log_dict = update_log_with_costs(log_dict, cost_per_hour)

    # Calculate storage costs
    if efs_storage_cost_monthly is not None or s3_cost_monthly is not None:
        print("Calculating storage...")
        log_dict = calculate_storage(log_dict, unit_log_dir, efs_storage_cost_monthly, s3_cost_monthly)

    # exit()

    # Calculate number of branches in each HUC
    print("Calculating branches per HUC...")
    log_dict = count_branches(log_dict, unit_log_dir)

    # For non-zero exit status, add traceback to text field
    scrape_traceback_for_non_zeros(log_dict, unit_log_dir)

    joined_gdf = join_dict_to_geopackage(log_dict)

    # Write the merged data back to a new geopackage
    print("Writing output geopackage...")
    joined_gdf.to_file(output_file, layer=f'{HUC_LEVEL}s_joined', driver="GPKG")


if __name__ == '__main__':
    '''
    SAMPE USAGE from within docker container:
        python3 create_completion_maps.py -i c6i.4xlarge -l /outputs/hlp_sample_10_20250321_huc12/logs/unit -o /fim_temp/completion_map_test.gpkg -j 4
    '''
    # Parse arguments.
    parser = argparse.ArgumentParser(
        description='Create maps with info about attempted HUCs in a given output directory.'
    )

    parser.add_argument(
        '-i',
        '--instance-type',
        help='Optional: The primary instance type for the run. If there are additional instance types '
        'not included in the PW_INSTANCE_TYPE_DICT, you will need to update it.',
        required=False,
        default=None,
    )

    parser.add_argument(
        '-l',
        '--unit-log-dir',
        help='Path to directory containing logs '
        'of fim_pipeline.sh (e.g. /efs/fim-data/hand_fim/outputs/PI3_fim60_10m_wbt/logs/unit)',
        required=True,
    )

    parser.add_argument(
        '-o', '--output-file', help='Required: The path to an output geopackage', required=True
    )

    parser.add_argument(
        '-efs',
        '--efs-storage-cost-monthly',
        help='Optional: Cost per GB per month',
        required=False,
        default=None,
        type=float,
    )

    parser.add_argument(
        '-s3',
        '--s3-cost-monthly',
        help='Optional: Cost per GB per month',
        required=False,
        default=None,
        type=float,
    )

    parser.add_argument(
        '-j',
        '--num-threads',
        help='Optional: Amount of threads to use for parallelizing reading log files',
        required=False,
        default=1,
        type=int,
    )

    # Extract to dictionary and run
    generate_geopackage(**vars(parser.parse_args()))

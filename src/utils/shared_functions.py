#!/usr/bin/env python3

import glob
import inspect
import logging
import os
import re
import sys
import threading
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from multiprocessing import Manager
from os.path import splitext
from pathlib import Path

import fiona
import geopandas as gp
import numpy as np
import pandas as pd
from tqdm import tqdm

import utils.shared_variables as sv


gp.options.io_engine = "pyogrio"


def setup_mp_file_logger(log_file_path, logger_name="custom_logger", level=logging.DEBUG):
    """
    Creates and returns a logger that logs to the specified file.
    """
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # Prevent duplicate handlers if already exists
    if not logger.handlers:
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(level)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.propagate = False  # avoid logging to root logger too

    return logger


def run_with_mp(
    task_function,
    tasks_args_list,
    file_logger,
    max_workers=4,
    task_id_key=None,  # must be one of the keys in the args list
    exit_on_failure=True,
    show_progress=True,
):
    '''
    Run a set of tasks in parallel using multiprocessing with robust logging and error handling.

    NOTES:
    This simple setup is using a shared log file and it is ok for now assuming that:
        - we have limitted amount of logs (3-4 lines per subprocess) in multiprocessing work
        - total number of subprocesses is modest (e.g., less than 50), not hundreds or thousands.
        - if we encounter a case that this does not work correctly, then we can improve it by creating one log file per task and combining them afterward.”

    - Use try/except in both the task function and this wrapper:
        • The task function should handle known/expected errors and always return True or False.
        • This wrapper catches unexpected crashes (e.g., segfaults or crashes in subprocesses).
        • No more try/except inside helper functions inside task function. Let them fail and task_function exception handles them.
        • Inside helper functions feel free to log any information. but No need to raise errors.
        • The only exception is that when we really need to address a special case like API limits and wait and retry.
    - Inside your task function or helpers, log live messages using screen_queue.put(msg).
    - These will appear in the main process via tqdm.write() and won't interrupt the progress bar.
    - Always pass three additional arguments into task_function and its helpers: file_logger ,screen_queue and task_id.
        - Do not use any print statements after start of multiprocessing in the task function or inside its helper functions. Instead use screen_queue.put().
        - use file_logger.info() to log the message in the log file
    '''

    screen_queue = (
        Manager().Queue()
    )  # creates a process-safe Queue that allows subprocesses to put() messages into it.

    # Background thread to print logs without interrupting tqdm
    def log_worker(queue):
        while True:
            msg = queue.get()
            if msg == "DONE":  # this must match the last message passed to screen_queue
                break
            tqdm.write(msg)

    screen_queue_thread = threading.Thread(
        target=log_worker, args=(screen_queue,)
    )  # this (from the main process)) reads screen_queues and prints on screen.
    screen_queue_thread.start()

    results = {}
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_id = {}
        for i, task_kwargs in enumerate(tasks_args_list):  # for each dictionary of keyword arguments (kwargs)
            task_id = f"Task-{i}"
            if task_id_key:
                task_id = task_kwargs.get(
                    task_id_key, task_id
                )  # this make a unique id (e.g. HUC number) for the task

            # also pass the loggers and task id
            kwargs_updated = task_kwargs.copy()
            kwargs_updated["file_logger"] = file_logger
            kwargs_updated["screen_queue"] = screen_queue
            kwargs_updated["task_id"] = task_id

            future = executor.submit(
                task_function, **kwargs_updated
            )  # submits tasks to workers in parallel. IMMEDIATELY after submitting (not after finishing the subprocess job) we get back a Future object, which is like a order number to track your requested food in a restaurant while waiting).
            future_to_id[future] = task_id

        # up to this point, the code is run immediately--submision is done right away. Now we wait for each job to be completed and be processed as below
        # Setup tqdm progress bar
        pbar = tqdm(total=len(future_to_id), desc="Processing tasks", unit="task") if show_progress else None
        # with tqdm(total=len(future_to_id), desc="Processing tasks", unit="task") as pbar:
        for future in as_completed(future_to_id):
            task_id = future_to_id[future]
            try:
                # note that try/except here only worries about running and catching catastrophic errors. Specifc errors must be addressed inside the task function
                # note that the try except inside task function always return (which is result here) True or False
                result = future.result()
                results[task_id] = result
                if result:
                    if show_progress:
                        tqdm.write(
                            f"✅ success for {task_id}"
                        )  # do not use print otherwise a new updated bar is created after each print line
                    else:
                        print(f"✅ success for {task_id}")
                    file_logger.info(f"✅ success for {task_id}")
                else:
                    if show_progress:
                        tqdm.write(f"❌ Error reported for {task_id}.")
                    else:
                        print(f"❌ Error reported for {task_id}.")
                    file_logger.info(f"❌ Error reported for {task_id}.")

            except Exception as ex:
                error_msg = f"❌ Error for {task_id}: {ex}"
                traceback_msg = traceback.format_exc()

                if show_progress:
                    tqdm.write(error_msg)
                else:
                    print(error_msg)
                file_logger.error(error_msg)
                file_logger.error(traceback_msg)

                results[task_id] = None

                if exit_on_failure:
                    dt_string = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                    final_msg = f"Program aborted at {dt_string} due to error in {task_id}"
                    file_logger.critical(final_msg)
                    executor.shutdown(
                        wait=False
                    )  # tells the ProcessPoolExecutor to stop accepting new tasks. Even cancel the running tasks as soon as possible
                    sys.exit(1)

            if pbar:
                pbar.update(1)  # ✅ Progress update for each completed task
        if pbar:
            pbar.close()

    screen_queue.put("DONE")  # sends the stop SIGNAL to thread
    screen_queue_thread.join()  # official closure of thread
    return results


def getDriver(fileName):
    driverDictionary = {'.gpkg': 'GPKG', '.geojson': 'GeoJSON', '.shp': 'ESRI Shapefile'}
    driver = driverDictionary[splitext(fileName)[1]]

    return driver


def pull_file(url, full_pulled_filepath):
    """
    This helper function pulls a file and saves it to a specified path.

    Args:
        url (str): The full URL to the file to download.
        full_pulled_filepath (str): The full system path where the downloaded file will be saved.
    """
    import urllib.request

    print("Pulling " + url)
    urllib.request.urlretrieve(url, full_pulled_filepath)


def delete_file(file_path):
    """
    This helper function deletes a file.

    Args:
        file_path (str): System path to a file to be deleted.
    """

    try:
        os.remove(file_path)
    except FileNotFoundError:
        pass


def run_system_command(args):
    """
    This helper function takes a system command and runs it. This function is designed for use
    in multiprocessing.

    Args:
        args (list): A single-item list, the first and only item being a system command string.
    """

    # Parse system command.
    command = args[0]

    # Run system command.
    os.system(command)


def get_fossid_from_huc8(
    huc8_id,
    foss_id_attribute='fossid',
    hucs=os.path.join(os.environ['inputsDir'], 'wbd', 'WBD_National.gpkg'),
    hucs_layerName=None,
):
    hucs = fiona.open(hucs, 'r', layer=hucs_layerName)

    for huc in hucs:
        if huc['properties']['HUC8'] == huc8_id:
            return huc['properties'][foss_id_attribute]


########################################################################
# Function to check the age of a file (use for flagging potentially outdated input)
########################################################################
def check_file_age(file):
    '''
    Checks if file exists, determines the file age
    Returns
    -------
    None.
    '''
    file = Path(file)
    if file.is_file():
        modified_date = datetime.fromtimestamp(file.stat().st_mtime, tz=timezone.utc)

        return modified_date


########################################################################
# Function to find huc subdirectories with the same name btw two parent folders
########################################################################
def find_matching_subdirectories(parent_folder1, parent_folder2):
    # List all subdirectories in the first parent folder
    subdirs1 = {
        d
        for d in os.listdir(parent_folder1)
        if os.path.isdir(os.path.join(parent_folder1, d)) and len(d) == 8
    }

    # List all subdirectories in the second parent folder
    subdirs2 = {
        d
        for d in os.listdir(parent_folder2)
        if os.path.isdir(os.path.join(parent_folder2, d)) and len(d) == 8
    }

    # Find common subdirectories with exactly 8 characters
    matching_subdirs = list(subdirs1 & subdirs2)

    return matching_subdirs


########################################################################
# Function to concatenate huc csv files to a single dataframe/csv
########################################################################
def concat_huc_csv(fim_dir, csv_name):
    '''
    Checks if huc csv file exist, concatenates contents of csv
    Returns
    -------
    None.
    '''

    merged_csv = []
    huc_list = [d for d in os.listdir(fim_dir) if re.match(r'^\d{8}$', d)]
    for huc in huc_list:
        if huc != 'logs':
            csv_file = os.path.join(fim_dir, huc, str(csv_name))
            if Path(csv_file).is_file():
                # Aggregate all of the individual huc elev_tables into one for accessing all data in one csv
                read_csv = pd.read_csv(
                    csv_file,
                    dtype={'HUC8': object, 'location_id': object, 'feature_id': int, 'levpa_id': object},
                )
                # Add huc field to dataframe
                read_csv['HUC8'] = huc
                merged_csv.append(read_csv)

    # Create and return a concatenated pd dataframe
    if merged_csv:
        print("Creating aggregate csv")
        concat_df = pd.concat(merged_csv)
        return concat_df


# -----------------------------------------------------------
def progress_bar_handler(executor_dict, desc):
    for future in tqdm(as_completed(executor_dict), total=len(executor_dict), desc=desc):
        try:
            future.result()
        except Exception as exc:
            print('{}, {}, {}'.format(executor_dict[future], exc.__class__.__name__, exc))


# #####################################
class FIM_Helpers:
    # -----------------------------------------------------------
    @staticmethod
    def append_id_to_file_name(file_name, identifier):
        '''
        Processing:
            Takes an incoming file name and inserts an identifier into the name
            just ahead of the extension, with an underscore added.
            ie) filename = "/output/myfolder/a_raster.tif"
                indentifer = "13090001"
                Becomes: "/output/myfolder/a_raster_13090001.tif"
            Note:
                - Can handle a single identifier or a list of identifier
                ie) identifier = ["13090001", "123000001"]
                Becomes: "/output/myfolder/a_raster_13090001_123000001.tif"
                - This allows for file name to not be submitted and will return None
        -------

        Inputs:
            file_name: a single file name
            identifier: a value or list of values to be inserted with an underscore
                added ahead of the extention

        -------
        Output:
            out_file_name: A single name with each identifer added at the end before
                the extension, each with an underscore in front of the identifier.

        -------
        Usage:
            from utils.shared_functions import FIM_Helpers as fh
            composite_file_output = fh.append_id_to_file_name(composite_file_output, huc)
        '''

        if file_name is not None:
            root, extension = os.path.splitext(file_name)

            if isinstance(identifier, list):
                out_file_name = root
                for i in identifier:
                    out_file_name += "_{}".format(i)
                out_file_name += extension
            else:
                out_file_name = root + "_{}".format(identifier) + extension
        else:
            out_file_name = None

        return out_file_name

    # -----------------------------------------------------------
    @staticmethod
    def vprint(message, is_verbose, show_caller=False):
        '''
        Processing: Will print a standard output message only when the
            verbose flag is set to True
        -------

        Parameters:
            message : str
                The message for output
                Note: this method puts a '...' in front of the message
            is_verbose : bool
                This exists so the call to vprint always exists and does not
                need a "if verbose: test for inline code
                If this value is False, this method will simply return
            show_caller : bool
                Sometimes, it is desired to see the calling function, method or class

        -------
        Returns:
            str : the message starting with "... " and optionallly ending with
                the calling function, method or class name

        -------
        Usage:
            from utils.shared_functions import FIM_Helpers as fh
            fh.vprint(f"Starting alpha test for {self.dir}", verbose)
        '''
        if not is_verbose:
            return

        msg = f"... {message}"
        if show_caller:
            caller_name = inspect.stack()[1][3]
            if caller_name == "<module":
                caller_name = inspect.stack()[1][1]
            msg += f"  [from : {caller_name}]"
        print(msg)

    # -----------------------------------------------------------
    @staticmethod
    def load_list_file(file_name_and_path):
        '''
        Process:
        -------
        Attempts to load a .txt or .lst file of line delimited values into a python list

        Parameters:
        -------
        file_name_and_path : str
            path and file name of data to be loaded.

        Returns:
        -------
        a Python list

        -------
        Usage:
            from utils.shared_functions import FIM_Helpers as fh
            fh.vprint(f"Starting alpha test for {self.dir}", verbose)

        '''

        if not os.path.isfile(file_name_and_path):
            raise ValueError(f"Sorry, file {file_name_and_path} does not exist. Check name and path.")

        line_values = []

        with open(file_name_and_path, "r") as data_file:
            data = data_file.read()
            # replacing end splitting the text
            # when newline ('\n') is seen.
            line_values_raw = data.split("\n")
            line_values_stripped = [i.strip() for i in line_values_raw]  # removes extra spaces

            # dending on comments in the file or an extra line break at the end, we might
            # get empty entries in the line_values collection. We remove them here
            line_values = [ele for ele in line_values_stripped if ele != ""]

        if len(line_values) == 0:
            raise Exception("Sorry, there are no value were in the list")

        return line_values

    # -----------------------------------------------------------
    @staticmethod
    def get_file_names(src_folder, file_extension):
        '''
        Process
        ----------
        Get a list of file names and paths matching the file extension

        Parameters
        ----------
            - src_folder (str)
                Location of the files.

            - file_extension (str)
                All files matching this file_extension will be added to the list.


        Returns
        ----------
        A list of file names and paths
        '''

        if (not file_extension) and (len(file_extension.strip()) == 0):
            raise ValueError("file_extension value not set")

        # remove the starting . if it exists
        if file_extension.startswith("."):
            file_extension = file_extension[1:]

        # test that folder exists
        if not os.path.exists(src_folder):
            raise ValueError(f"{file_extension} src folder of {src_folder} not found")

        if not src_folder.endswith("/"):
            src_folder += "/"

        glob_pattern = f"{src_folder}*.{file_extension}"
        file_list = glob.glob(glob_pattern)

        if len(file_list) == 0:
            raise Exception(
                f"files with the extension of {file_extension} "
                f" in the {src_folder} did not load or do not exist"
            )

        file_list.sort()

        return file_list

    # -----------------------------------------------------------
    @staticmethod
    def print_current_date_time():
        '''
        Process:
        -------
        prints the following:

            Current date and time: 2022-08-19 15:22:49

        -------
        Usage:
            from utils.shared_functions import FIM_Helpers as fh
            fh.print_current_date_time()

        -------
        Returns:
            Current date / time as a formatted string

        '''
        d1 = datetime.now()
        dt_stamp = "Current date and time : "
        dt_stamp += d1.strftime("%Y-%m-%d %H:%M:%S")
        print(dt_stamp)

        return dt_stamp

    # -----------------------------------------------------------
    @staticmethod
    def print_date_time_duration(start_dt, end_dt):
        '''
        Process:
        -------
        Calcuates the diffenence in time between the start and end time
        and prints is as:

            Duration: 4 hours 23 mins 15 secs

        -------
        Usage:
            from utils.shared_functions import FIM_Helpers as fh
            fh.print_current_date_time()

        -------
        Returns:
            Duration as a formatted string

        '''
        time_delta = end_dt - start_dt
        total_seconds = int(time_delta.total_seconds())

        total_days, rem_seconds = divmod(total_seconds, 60 * 60 * 24)
        total_hours, rem_seconds = divmod(rem_seconds, 60 * 60)
        total_mins, seconds = divmod(rem_seconds, 60)

        time_fmt = f"{total_days:02d} days {total_hours:02d} hours {total_mins:02d} mins {seconds:02d} secs"

        duration_msg = "Duration: " + time_fmt
        print(duration_msg)

        return duration_msg

    # -----------------------------------------------------------
    @staticmethod
    def print_start_header(friendly_program_name, start_time):
        print("================================")
        dt_string = start_time.strftime("%m/%d/%Y %H:%M:%S")
        print(f"Start {friendly_program_name} : {dt_string}")
        print()

    # -----------------------------------------------------------
    @staticmethod
    def print_end_header(friendly_program_name, start_time, end_time):
        print("================================")
        dt_string = end_time.strftime("%m/%d/%Y %H:%M:%S")
        print(f"End {friendly_program_name} : {dt_string}")
        print()

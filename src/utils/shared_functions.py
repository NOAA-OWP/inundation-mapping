#!/usr/bin/env python3

import glob
import inspect
import logging
import os

# import pathlib
# import re
import shutil

# import sys
import threading
import traceback
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone
from multiprocessing import Manager
from os.path import splitext
from pathlib import Path
from typing import Union

# import fiona
import geopandas as gp
import numpy as np
import pandas as pd
from fsspec.core import url_to_fs
from tqdm import tqdm


# Registry: name -> path
_LOGGER_REGISTRY = {}

gp.options.io_engine = "pyogrio"


@contextmanager
def use_pandas_3_behavior():
    """Enable pandas 3.0 behavior temporarily.
    1. Enable Copy-on-Write behavior for improved memory management.
    2. Use PyArrow strings for better performance/memory usage.
    3. Disable silent downcasting with fillna, replace, and clip.
    """
    with pd.option_context(
        "mode.copy_on_write", True, "future.infer_string", True, "future.no_silent_downcasting", True
    ):
        yield


# #################################
# log file tools


# This one is a standard Python logger, NOT MEANT for multi-proc
# def setup_file_logger(log_file_path):
def setup_file_logger(log_file_dir, log_file_name_prefix):
    """

    This creates a file name for you. I will take the log_file_name_prefix, then append a dt, then extension

    ie) setup_file_logger("/ouputs/mylogs", "pull_osm_bridges")
    The log name becomes "/outputs/mylogs/pull_osm_bridges_20250925_1842.log

    This one is generally not meant to be used for MP's but this needs to be confirmed.
    The problem with loggers being used inside some MP's is that each MP process can potentially
    be writing to the same file at the same time.  If you have a process even if it is in an
    MP, as long as it has its own unique MP log file names, you can use this one.
    ie)  huc_12090301_inundate_20260217.log

    It prints to file and screen at the same time.

    This one is very similar to 'setup_mp_file_logger' but has a few critical differences.
       1) This one does not had a console / stream handler as it uses the system default. ie. logging.info, etc.
       2) It does not return a handler and we don't want to as we want this one to simply
          help configure the default logging handler. There are some places were we end up using
          both this default log handler plus a custom one which is created generally in setup_mp_file_logger

    This also has a system that detects if there is a msg with level error or critical
    and save those values in the regular log file but also in its own error file.
    This helps bring out errors, especially in tools that allow for a task to fail but continue on.
    Note: The file is created automatically and if no actual errors are found, that file will be empty.

    In the end, the error log file is not removed if it is empty, just watch its file size.

    Returns the name/path of the new log file.
    """

    if not log_file_dir:
        raise ValueError("log directory path can not be None or empty")

    if not log_file_name_prefix:
        raise ValueError("log file name prefix can not be None or empty")

    # Example with a different permission (e.g., full access for everyone)
    permissions_code = 0o776
    os.makedirs(log_file_dir, mode=permissions_code, exist_ok=True)
    # even though we used os.makedirs, it does not mean it had permission to make the dir
    # the mode is for permissions of the folder once is created.
    if not os.path.isdir(log_file_dir):
        raise Exception("This script likely does have permission to add a log folder")

    file_dt_string = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")
    log_file_name = f"{log_file_name_prefix}_{file_dt_string}.log"
    log_file_path = os.path.join(log_file_dir, log_file_name)

    # we will assume the parent folder already exists
    os.makedirs(log_file_dir, exist_ok=True, mode=permissions_code)
    print(f"Logs saved to: {log_file_path}")

    # even though we used os.makedirs, it does not mean it had permission to make the dir
    # the mode is for permissions of the folder once is created.
    if not os.path.isdir(log_file_dir):
        raise Exception("This script likely does have permission to add a log folder")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # logger.propagate = False # Prevent propagation to the root logger

    # basic screen handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')  # override the default
    console_handler.setFormatter(formatter)

    # formatter for files
    formatter = logging.Formatter("%(asctime)s - %(levelname)s : %(message)s")

    # error file handler
    error_file_name = log_file_path.replace(".log", "-errors.log")
    err_file_handler = logging.FileHandler(error_file_name)
    err_file_handler.setLevel(logging.ERROR)
    err_file_handler.setFormatter(formatter)
    os.chmod(error_file_name, mode=permissions_code)

    # warning file handler
    warning_file_name = log_file_path.replace(".log", "-warnings.log")
    warn_file_handler = logging.FileHandler(warning_file_name)
    warn_file_handler.setLevel(logging.WARNING)
    warn_file_handler.setFormatter(formatter)
    # Filter to exclude ERROR and CRITICAL from warnings file
    warn_file_handler.addFilter(lambda record: record.levelno < logging.ERROR)
    os.chmod(warning_file_name, mode=permissions_code)

    # basic file handler
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    os.chmod(log_file_path, mode=permissions_code)

    logger.handlers.clear()  # reset the custom logger settings below
    # order matters here
    logger.addHandler(err_file_handler)
    logger.addHandler(warn_file_handler)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Current functionality:
    # .log file will get: INFO, WARNING, ERROR, CRITICAL
    # -warnings.log file will get: WARNING (but not ERROR or CRITICAL)
    # -errors.log file will get: ERROR and CRITICAL
    # DEBUG will not end up in any of them

    return log_file_path


# This one is more designed to be for multi-proc as it has logger handler names
# Notice how it does not have a "console / stream handler"? hence, a special function for MP
def setup_mp_file_logger(log_file_path: str, logger_name: str, level=logging.DEBUG):
    """

    Create a logger bound by a strict bijection:
      - Each logger_name maps to exactly one log_file_path.
      - Each log_file_path may only be used by one logger_name.

    This version is meant for use in MP as it makes a custom logger and does not use the default
    logger. However, both can co-exist.

    This one appears very similar to the function above of 'setup_file_logger', but there are
    some critical key differences. Please read the notes in setup_file_logger for more details.

    Creates and returns a logger that logs to the specified file.
    Prints to screen info and higher by default. The file logging
    is set to level of debug so it gets debug as well.

    This also has a system that detects if there is a msg with level error or critical
    and save those values in the regular log file but also in its own error file.
    This helps bring out errors, especially in tools that allow for a task to fail but continue on.
    Note: The file is created automatically and if no actual errors are found, that file will be empty.

    For now.. it can leave empty error files when all is said and done and that is fine. Just watch
    It's file size.

    """

    if not logger_name:
        raise ValueError("logger_name can not be None or empty")

    if not log_file_path.endswith(".log"):
        raise Exception("log file name must end with .log")

    abs_path = os.path.abspath(log_file_path)
    permissions_code = 0o776
    log_folder = os.path.dirname(abs_path)
    os.makedirs(log_folder, mode=permissions_code, exist_ok=True)
    # even though we used os.makedirs, it does not mean it had permission to make the dir
    # the mode is for permissions of the folder once is created.
    if not os.path.isdir(log_folder):
        raise OSError("This script likely does have permission to add a log folder")

    # Check name -> path
    if logger_name in _LOGGER_REGISTRY and _LOGGER_REGISTRY[logger_name] != abs_path:
        raise ValueError(
            f"Logger '{logger_name}' already bound to '{_LOGGER_REGISTRY[logger_name]}', cannot bind to '{abs_path}'."
        )

    # Check path -> name (reverse lookup)
    for name, path in _LOGGER_REGISTRY.items():
        if path == abs_path and name != logger_name:
            raise ValueError(
                f"Path '{abs_path}' is already bound to logger '{name}', cannot assign to '{logger_name}'."
            )

    # Register if new
    _LOGGER_REGISTRY.setdefault(logger_name, abs_path)

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # Prevent duplicate handlers if already exists
    if not logger.handlers:

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        # Order of adding handlers may be important ???
        # error file handler
        error_file_name = log_file_path.replace(".log", "-errors.log")
        err_file_handler = logging.FileHandler(error_file_name)
        err_file_handler.setLevel(logging.ERROR)
        err_file_handler.setFormatter(formatter)
        os.chmod(error_file_name, mode=permissions_code)
        logger.addHandler(err_file_handler)
        os.chmod(error_file_name, mode=permissions_code)

        # warning file handler
        warning_file_name = log_file_path.replace(".log", "-warnings.log")
        warning_file_handler = logging.FileHandler(warning_file_name)
        warning_file_handler.setLevel(logging.WARNING)
        warning_file_handler.setFormatter(formatter)
        logger.addHandler(warning_file_handler)
        os.chmod(warning_file_name, mode=permissions_code)

        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        os.chmod(log_file_path, mode=permissions_code)
        logger.propagate = False  # avoid logging to root logger too

    return logger


# This saves the msg to a log file, but also either a standard "print" or a "screen queue"
# Note: the screen queue is really just a Manager.queue and we are usign a "put"
# If the screen_queue is None, it defaults to "print"
# TODO: Does debug work in the loggers?
def l_print(msg, file_logger, log_level="info", screen_queue=None):

    if screen_queue is None:
        print(msg)
    else:
        screen_queue.put(msg)

    match log_level:
        case "trace":
            file_logger.debug(msg)  # TODO: most of our logging tools need to be fixed to handle trace.
        case "debug":
            file_logger.debug(msg)
        case "info":
            file_logger.info(msg)
        case "warning":
            file_logger.warning(msg)
        case "error":
            file_logger.error(msg)
        case "critical":
            file_logger.critical(msg)
        case _:
            raise Exception("Invalid log level value. Options are debug, info, warning, error and critical")


def rollup_log_files(src_file, trg_file, remove_old_src_file=True):
    # Sometimes we want to append log file onto another file.
    # For example, a temp mp log file into the parent log.

    # This will not error out if the files do not exist
    # only send back a True / False (successful)

    # this will also look for rollups automatically for -warning and -error files

    if not os.path.exists(src_file) or not os.path.exists(trg_file):
        return False

    with open(src_file, 'r') as src:
        with open(trg_file, 'a') as trg:
            shutil.copyfileobj(src, trg)

    if remove_old_src_file:
        os.remove(src_file)

    # ----------------
    # This will auto rollup warning files if they exist
    warning_src_file_name = src_file.replace(".log", "-warnings.log")
    warning_trg_file_name = trg_file.replace(".log", "-warnings.log")
    if os.path.exists(warning_src_file_name) and os.path.exists(warning_trg_file_name):
        with open(warning_src_file_name, 'r') as src:
            with open(warning_trg_file_name, 'a') as trg:
                shutil.copyfileobj(src, trg)

        if remove_old_src_file:
            os.remove(warning_src_file_name)

    # ----------------
    # This will auto rollup errors files if they exist
    error_src_file_name = src_file.replace(".log", "-errors.log")
    error_trg_file_name = trg_file.replace(".log", "-errors.log")
    if os.path.exists(error_src_file_name) and os.path.exists(error_trg_file_name):
        with open(error_src_file_name, 'r') as src:
            with open(error_trg_file_name, 'a') as trg:
                shutil.copyfileobj(src, trg)

        if remove_old_src_file:
            os.remove(error_src_file_name)

    return True


# #################################
# Multi proc tools
def run_with_mp(
    task_function,
    tasks_args_list,
    file_logger,
    task_id_key,  # must be one of the keys in the args list.
    max_workers=4,
    show_progress=True,
):
    '''
    Run a set of tasks in parallel using multiprocessing with robust logging and error handling.

    NOTES:
    This setup is using a shared log file and it is ok for now assuming that:
        - we have limitted amount of logs (3-4 lines per subprocess) in multiprocessing work.

        - total number of subprocesses is modest (e.g., less than 50), not hundreds or thousands.

        - if we encounter a case that this does not work correctly, then we can improve it by creating one
           log file per task and combining them afterward.

    - Use try/except in both the task function and this wrapper:
        " Child MP process functions should always have it's own try/except to handle issues gracefully.
        " This wrapper catches unexpected crashes (e.g., segfaults or crashes in subprocesses).
        " Inside helper functions feel free to log any information. but no need to raise errors.
        " The only exception is that when we really need to address a special case like API limits and wait and retry.

    - Inside your task function or helpers, log live messages using screen_queue.put(msg) if the screen_queue
        has been passed to the child mp function.

    - These will appear in the main process via tqdm.write() and won't interrupt the progress bar.

    - Always pass three additional arguments into task_function and its helpers: file_logger ,screen_queue and task_id.
        - Do not use any print statements after start of multiprocessing in the task function or inside its helper
          functions. Instead use screen_queue.put().

        - Use file_logger.info() to log the message in the log file.
    '''

    # +++++++++++++++++++++
    ####  TASK RETURN VALUES TO THE POOL ####

    # Different tools have different needs for how it uses it's MP functions and what it returns from run_with_mp.

    # This tool requires that two things are returned: a return code, then a list.
    #    The list can be empty ([]), contain a single item, or contain multiple items — whatever
    #    the caller needs (e.g. a bool, a string, a dataframe, or multiple values like
    #    [bool, df1, df2]). The entire list is stored as-is in the results dict under the task_id.
    #    In the end, results = {task_id: rtn_value_list, ...}

    # -  A status code. options are:
    #       1: Success and show tqdm or print success line
    #       0: Fail but don't shut down, advance the pbar AND show the tqdm / print error or warning message
    #      -1: Critical Fail and the entire script should be aborted

    # Some examples of usage:
    #    data\roads\pull_osm_roads.py wants a T/F returned for every mp item, so its mp process
    #    named "single_huc_job" returns:
    #            1, [True]  (meaning success and add "True" to the run_by_mp result set)
    #            0, [False] (meaning fail don't shut down the entire process, add the value of
    #                 False to the run_with_mp return results and show the tqdm / print message

    # Another example:
    #    data\usgs\get_usgs_rating_curves.py have different needs. Inside its mp function,
    #    named "__mp___mp_get_site_rating_curve" could have three scenerios (at a min)
    #           1, [some dataframe]  (success and add the dataframe to the run_by_mp result set)
    #           0, [None]  (Fail but there is nothing to add to the run_by_mp result set)
    #          -1, [None]  (Catestrophic fail, shut down the entire script)

    # ++++++++++++++++++++++

    # Validation
    if not task_function:
        raise Exception("task_function argument can not be None or empty")
    if tasks_args_list is None or len(tasks_args_list) == 0:
        raise Exception("tasks_args_list can not be None or empty")
    if file_logger is None or not isinstance(file_logger, logging.Logger):
        raise Exception("file_logger is either None or is not a logging class")
    if not task_id_key:
        raise Exception("task_id_key can not be None or empty")

    missing = [i for i, t in enumerate(tasks_args_list) if task_id_key not in t]
    if missing:
        raise Exception(f"task_id_key '{task_id_key}' missing from tasks_args_list at indices: {missing}")

    try:
        # the thread must be inside the try catch to be closed correct in system fail.
        screen_queue = (
            Manager().Queue()
        )  # Manager proxy queue — picklable so it can be passed to spawned worker processes via ProcessPoolExecutor

        # Background thread to print logs without interrupting tqdm
        def log_worker(queue):
            while True:
                msg = queue.get()
                if msg == "DONE":  # this must match the last message passed to screen_queue
                    break
                tqdm.write(msg)

        console_queue_thread = threading.Thread(
            target=log_worker, args=(screen_queue,)
        )  # this (from the main process)) reads screen_queues and prints on screen.
        # screen_queue_thread.daemon = True
        console_queue_thread.start()

        # There are a wide number of ways a mp can die. It might be programatically
        #   - code level exception explicity thrown
        #   - an implicit code level exceipt
        #   - can be a system level such as a CTRL-C
        #   - CPU collisons, etc

        # Because there are multiple ways that an MP can crash, it very easy to leave either
        #   an orphaned process (memory leak), or a thread that is still forceing the program to stay open.
        #   It is not possible to kill an mp function already in progress short of some very, very complex
        #   complete operating system process management (extremely not recommended).
        #   In this case we are manageing an process pool, a memory thread, and logging. It is possible
        #   and has happened in development that something dies and the program hangs.
        #   One would think just a simple try/except is enough but that is not always true.
        #   This is why we have a bizarre exception handling system here and it works pretty well.
        #
        # Also as mentioned... can not kill an mp that has already started. There is no abort a currently running
        #   mp (threads you can but not MP's and threads come with a whole new set of challenges and only advised
        #   for explicit reasons for our FIM applications). This tool keeps the option open to decide if the error
        #   is logged and continues or shuts down the entire application.  Simply putting sys.exit(1) does not
        #   work and can often hang up the script. It all comes down to memory management, python garbage dumps,
        #   and how objects are used (passed versus reference).

        # Main point here:  You can shut down the overall MP, but you can not stop a mp task in action. You can only
        #   send issues commands telling the code to stop and it will stop new tasks from starting. Then wait for
        #   the wip tasks to complete. ie) If you have 20 Jobs and one kills the app, you have to wait until all
        #   the remaining 19 tasks to finish before it fully stops and this can take a few mins.

        # When an MP dies and we want to shut down the app, we have to let it finish the wip mps, then we can
        #   abort and stop new ones from firing.

        # CTRL-C entered multiple of times from console will stop the two program faster, but will leave orphaned memory
        #    leaks.  If you do this, close your container to release the memory leaks and restart a new container.

        results = {}
        with ProcessPoolExecutor(max_workers=max_workers) as executor:

            future_to_id = {}
            # up to this point, the code is run immediately--submision is done right away. Now we wait for each job to be completed and be processed as below
            pbar = tqdm(
                total=len(tasks_args_list), desc="Processing tasks", unit="task", disable=(not show_progress)
            )

            # Some mp functions might throw an exception, which means it may not get to as_completed
            # We still need to catch that and if so, shut down the script.
            try:
                for i, task_kwargs in enumerate(
                    tasks_args_list
                ):  # for each dictionary of keyword arguments (kwargs)
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

                    # Submits tasks to workers in parallel. IMMEDIATELY after submitting (not after finishing
                    # the subprocess job) we get back a Future object, which is like a order number to
                    # track your requested food in a restaurant while waiting).
                    future = executor.submit(task_function, **kwargs_updated)
                    future_to_id[future] = task_id

                # Catestophic errors mean we can not 100% guarantee all mp's will come back as_completed.
                for future in as_completed(future_to_id):
                    task_id = future_to_id[future]

                    # The rtn_value can be T/F, a string, dataset, list, dictionary (?), pretty much anything.
                    # See notes above about return values.
                    rtn_code, rtn_value = future.result()

                    if rtn_code == 1:  # Positive = good
                        # success and show tqdm or print line
                        if show_progress:
                            tqdm.write(
                                f"✅ Success for {task_id}"
                            )  # do not use print otherwise a new updated bar is created after each print line
                        else:
                            print(f"✅ Success for {task_id}")
                        file_logger.info(f"✅ Success for {task_id}")

                    elif rtn_code == 0:  # Fail but not shut down the pool.
                        if show_progress:
                            tqdm.write(f"❌ Error reported for {task_id}.")
                        else:
                            print(f"❌ Error reported for {task_id}.")
                        file_logger.info(f"❌ Error reported for {task_id}.")

                    else:  # rtn_code == -1, but really any negative int
                        # Catestrophic fails, shut the tool down (and assumes the mp logged the reason why)
                        # throw an exception to shut down and cleanup all objects (pool, tqdm, queue)
                        raise Exception(
                            f"Critical Error: Abort Program from task id = {task_id}."
                            " See exception details in the logs."
                        )

                    results[task_id] = rtn_value

                    if pbar:
                        # print("task bar being updated")
                        pbar.update(1)  # ✅ Progress update for each completed task

                if pbar:  # All mp tasks are done.
                    pbar.close()

            except Exception as ex:
                # The child mp function should have it's own try/except but in case something slips
                # through or they forgot to add it.

                error_msg = f"❌ Critical error: {ex}"
                traceback_msg = traceback.format_exc()
                print(error_msg)
                print(traceback_msg)
                file_logger.critical(error_msg)
                file_logger.critical(traceback_msg)

                dt_string = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                final_msg = f"Program aborted at {dt_string} due to error in {task_id}"
                file_logger.critical(final_msg)

                file_logger.info("Process pool shutting down")
                print(
                    "Process pool shutting down. This may take a while depending on how many jobs."
                    " Jobs currently in progress will need to complete for this can fully shut down.",
                    flush=True,
                )
                print("", flush=True)
                executor.shutdown(
                    wait=False, cancel_futures=True
                )  # tells the ProcessPoolExecutor to stop accepting new tasks. Even cancel the running tasks as soon as possible

                # CTRL-C can trigger secondary exceptions when objects inside this page are still held
                # open. Make sure you close and restart the docker container if you use CTRL-C to abort.

                # Yes.. seems weird to have this here and a new exception.
                # But it helps force shut down other objects like manual logging and a
                # a queue.
                if pbar:
                    pbar.close()  # aborts the progress bar

                if console_queue_thread:
                    screen_queue.put("DONE")  # sends the stop SIGNAL to thread
                    console_queue_thread.join()  # official closure of thread
                # re raising instead of sys.exit to help ensure all objects are cleaned up correctly
                raise Exception("Shutting down. Cleaning up caches and objects....")

        # if the pool finished correctly, shut down the remaining queue.
        if console_queue_thread:
            screen_queue.put("DONE")  # sends the stop SIGNAL to thread
            console_queue_thread.join()  # official closure of thread

    # This is primarily used when using CTRL-C to which can leave orphaned processes
    except Exception as ex2:
        print("Still shutting down, hang in there", flush=True)
        print(ex2, flush=True)
        if console_queue_thread:
            screen_queue.put("DONE")  # sends the stop SIGNAL to thread
            console_queue_thread.join()  # official closure of thread

        # This hanging in some scenarios such as a bug in this function. Triggered by a mp child
        # function not returning values correctly.
        # sys.exit(1)
        # need to rethrow
        raise ex2

    return results


# #################################
# Misc tools#


def getDriver(fileName):
    driverDictionary = {'.gpkg': 'GPKG', '.geojson': 'GeoJSON', '.shp': 'ESRI Shapefile'}
    driver = driverDictionary[splitext(fileName)[1]]

    return driver


# ============================
# Assumes the env file has been loaded into the os.environ objects
def get_value_from_env(arg_key):
    '''
    Notes:
        - This assumes the env has already been loaded. The env_file_path is for error messages only.
        - we don't actually load the file here as we could be loading more than once.
    Returns
        - The arg_key value. The return value may also have placeholders such as "mypath/{some version}/",
          which can be subsituted somewhere else.
    '''
    if arg_key is None or arg_key == "":
        raise Exception("env_var_name key is missing or empty")

    env_value = os.environ[arg_key]

    if env_value is None or env_value.strip() == "":
        raise ValueError(f"Env variable of {arg_key} does not exist or empty")

    return env_value.strip()


# ============================
def get_env_value(env_var_name):
    """
    This function can load a variable value from the enviro.
    If the enviro value has {} in it, it can auto use recursive subsitution
    to fill out the entire return value.

    ie) looking to load HV_PUSH_HAND_CMD:
       First pass it comes back with  =
         "aws s3 sync {FIM_HAND_DATASET_LOCAL_PATH} s3://{HV_S3_BUCKET_NAME}/{HV_S3_ROOT_HANDSET_PATH}..."
       It will iterate up to 3 more times to fill in those values. Note: Some of those values
       require additional subsitution.
       ie) FIM_HAND_DATASET_LOCAL_PATH returned with {} above and needs to be further subsitution.
           FIM_HAND_DATASET_LOCAL_PATH = "/data/previous_fim/hand_{HAND_VERSION}"
       It will iterate again to subsitute {HAND_VERSION}

    This can do three levels of embedded subsitution

    Note: While not pretty, it knows variable names that could be used in recursion.
    TODO: think up something smarter. likely just use recursion to call this function.
    """

    env_value = get_value_from_env(env_var_name)
    if "{" not in env_value and "}" not in env_value:
        return env_value

    # had trouble getting recursion working, so just loop through it up to ten times
    value_adj_done = False  # helps manage when we know there are no more subsitutions required
    for i in range(10):
        if value_adj_done:
            break

        # extract sub_key
        # Find the indices of the start and end characters
        start_index = env_value.find("{")
        end_index = env_value.find("}")

        # Check if both characters are found
        if start_index != -1 and end_index != -1:
            extracted_key = env_value[start_index + len("{") : end_index]
            extracted_value = get_env_value(extracted_key)
            env_value = env_value.replace("{" + extracted_key + "}", extracted_value)

        if "{" not in env_value and "}" not in env_value:
            value_adj_done
            break

    return env_value


# ============================
def get_huc_vars(huc):
    """Return the region-specific env vars (CRS and input paths) for a given 8-digit HUC number.

    Keys are consistent across all regions ('crs', 'landsea', 'roads', 'levees',
    'levees_preprocessed', 'levee_protected_areas', 'lakes', 'catchments',
    'streams', 'headwaters', 'wbd', 'dem_domain'); a key is None where a region
    has no dedicated value (e.g. American Samoa has no NLD/levee inputs).
    'landsea' resolves to the Great Lakes boundary for HUCs in region 04,
    regardless of CRS region.
    Expects bash_variables.env to have been loaded by the calling script.
    """
    if str(huc).startswith('19'):
        huc_vars = {
            'crs': os.getenv('ALASKA_CRS'),
            'landsea': os.getenv('input_landsea_Alaska'),
            'roads': os.getenv('osm_roads_alaska'),
            'levees': os.getenv('input_NLD_Alaska'),
            'levees_preprocessed': os.getenv('input_levees_preprocessed_Alaska'),
            'levee_protected_areas': os.getenv('input_nld_levee_protected_areas_Alaska'),
            'wbd': os.getenv('input_WBD_gdb_Alaska'),
            'dem_domain': os.getenv('input_DEM_domain_Alaska'),
        }

        if str(huc) in ['19010301', '19080306', '19080307']:
            huc_vars['lakes'] = os.getenv('input_lakes_NorthAlaska')
            huc_vars['catchments'] = os.getenv('input_catchments_NorthAlaska')
            huc_vars['streams'] = os.getenv('input_flows_NorthAlaska')
            huc_vars['headwaters'] = os.getenv('input_headwaters_NorthAlaska')
        else:
            huc_vars['lakes'] = os.getenv('input_nwm_lakes_SouthAlaska')
            huc_vars['catchments'] = os.getenv('input_nwm_catchments_SouthAlaska')
            huc_vars['streams'] = os.getenv('input_nwm_flows_SouthAlaska')
            huc_vars['headwaters'] = os.getenv('input_nwm_headwaters_SouthAlaska')

        return huc_vars

    elif str(huc) == '22010000':
        return {
            'crs': os.getenv('GUAM_CRS'),
            'landsea': os.getenv('input_landsea_Guam'),
            'roads': os.getenv('osm_roads_guam'),
            'levees': os.getenv('input_NLD_Guam'),
            'levees_preprocessed': os.getenv('input_levees_preprocessed_Guam'),
            'levee_protected_areas': os.getenv('input_nld_levee_protected_areas_Guam'),
            'lakes': os.getenv('input_nhd_lakes_Guam'),
            'catchments': os.getenv('input_catchments_Guam'),
            'streams': os.getenv('input_nhd_flows_Guam'),
            'headwaters': os.getenv('input_nhd_headwaters_Guam'),
            'wbd': os.getenv('input_WBD_gdb_Guam'),
            'dem_domain': os.getenv('input_DEM_domain_Guam'),
        }
    elif str(huc) == '22030001':
        return {
            'crs': os.getenv('AMERICAN_SAMOA_CRS'),
            'landsea': os.getenv('input_landsea_AmericanSamoa'),
            'roads': os.getenv('osm_roads_americansamoa'),
            'levees': None,
            'levees_preprocessed': None,
            'levee_protected_areas': None,
            'lakes': os.getenv('input_nhd_lakes_AmericanSamoa'),
            'catchments': os.getenv('input_catchments_AmericanSamoa'),
            'streams': os.getenv('input_nhd_flows_AmericanSamoa'),
            'headwaters': os.getenv('input_nhd_headwaters_AmericanSamoa'),
            'wbd': os.getenv('input_WBD_gdb_AmericanSamoa'),
            'dem_domain': os.getenv('input_DEM_domain_AmericanSamoa'),
        }
    else:
        return {
            'crs': os.getenv('DEFAULT_FIM_PROJECTION_CRS'),
            'landsea': (
                os.getenv('input_GL_boundaries') if str(huc).startswith('04') else os.getenv('input_landsea')
            ),
            'roads': os.getenv('osm_roads'),
            'levees': os.getenv('input_NLD'),
            'levees_preprocessed': os.getenv('input_levees_preprocessed'),
            'levee_protected_areas': os.getenv('input_nld_levee_protected_areas'),
            'lakes': os.getenv('input_nwm_lakes'),
            'catchments': os.getenv('input_nwm_catchments'),
            'streams': os.getenv('input_nwm_flows'),
            'headwaters': os.getenv('input_nwm_headwaters'),
            'wbd': os.getenv('input_WBD_gdb'),
            'dem_domain': os.getenv('input_DEM_domain'),
        }


# ============================
# Adds a starting and ending slash if not already there
def add_slashes_to_path(file_path):
    if not file_path.endswith("/"):
        file_path += "/"
    if not file_path.startswith("/"):
        file_path = "/" + file_path
    return file_path


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
# Function to search and concatenate huc csvs files to a single dataframe/csv
########################################################################
def search_concat_huc_csvs(directory_path, pattern, is_recursive=True):
    """
    Scans a directory and subdirectories for files files matching a pattern.

    Taking in the directory path and scanning for all files:

    Args:
        directory_path (str): The root folder to start scanning.
        pattern (str): The search pattern (e.g., 'huc_*' or '*branch_ids.csv').
        is_recursive (bool):
    Returns:
        A list of dataframes loaded from the csv's found.
    """

    # Create a Path object for the base directory
    search_path = Path(directory_path)

    if pattern == "":
        raise Exception("file search pattern has not been supplied")

    file_list = []
    if is_recursive is True:
        # rglob stands for "recursive glob" and finds matches in all subfolders
        # Use .glob() instead if you only want to scan the top-level folder
        file_list = list(search_path.rglob(pattern))
    else:
        file_list = list(search_path.glob(pattern))

    num_files_found = len(file_list)
    if num_files_found == 0:
        return []

    csv_df_list = []
    for file in file_list:
        try:
            df = pd.read_csv(file, dtype=str)
            if len(df) != 0:
                # It might have just a header row but no columns, basically empty
                csv_df_list.append(df)
        except Exception as e:
            raise Exception(f"Error reading {file.name}: {e}")

    return csv_df_list


# -----------------------------------------------------------
def progress_bar_handler(executor_dict, desc):
    for future in tqdm(as_completed(executor_dict), total=len(executor_dict), desc=desc):
        try:
            future.result()
        except Exception as exc:
            print('{}, {}, {}'.format(executor_dict[future], exc.__class__.__name__, exc))


########################################################################
# TODO: Sept 2025: There is a new data/aws/s3_shared_functions.py coming in which might
# be able to take over for these.
def s3_or_local_path_exists(path: str) -> bool:
    """
    Checks existence of path for local or s3 path

    Parameters
    ----------
    path: str
        Path to check existence of

    Returns
    -------
    bool
        Path exists or does not exist
    """
    fs, pth = url_to_fs(path)
    return fs.exists(pth)


def s3_or_local_isfile(path: str) -> bool:
    """
    Checks if path is a file for the case of a local or s3 path

    Parameters
    ----------
    path: str
        Path to check existence of

    Returns
    -------
    bool
        Path is a file or is not a file
    """
    fs, pth = url_to_fs(path)
    return fs.isfile(pth)


def s3_or_local_glob(path: str) -> list:
    """
    Returns glob list in the case of a local or s3 path

    Parameters
    ----------
    path: str
        Path to run glob operation on

    Returns
    -------
    list
        Paths and directories associated with glob operation
    """
    fs, pth = url_to_fs(path)
    return fs.glob(pth)


def read_huc_file_list_or_array_of_hucs(hucs):
    """
    This function can be used for things other than just HUCS, but is being
    tested for list of HUCs or a path to a HUC list file.

    Some code reads in either a huc list file path or a list of hucs.
    ie) /data/inputs/huc_list/mylist.huc
    or 12090301 05020305
    Sometimes using nargs="+" can treat the incoming argument as one big or a bunch of hucs.
    ie) -u 12090301 05030105 (without quotes around the two become two items in a huc list)
    BUT
        -u '12090301 05030105' becomes jsut one large sting in the single list. ie) hucs[0] = '12090301 05030105'

    This function can handle all three to result in a valid huc list array.
    """

    if isinstance(hucs, list) and len(hucs) == 0:
        raise ValueError("HUC or item list can not be empty")

    huc_list = set()

    if isinstance(hucs, list) and len(hucs) == 1:
        # can be a path to a file or a single item huc
        # but also could be one item with more than one huc in the string
        # ie: hucs[0] = '12090301 05030104' which we need to break apart
        if hucs[0].endswith(".lst"):
            if not os.path.isfile(hucs[0]):
                raise FileNotFoundError(f"Huc list file not found at {hucs[0]}")

            with open(hucs[0], 'r') as hucs_file:
                file_lines = hucs_file.readlines()
                f_list = [clean_huc_value(fl) for fl in file_lines]
                huc_list.update(f_list)
        else:
            raw_huc_list = hucs[0].split(" ")
            for huc in raw_huc_list:
                huc_list.add(clean_huc_value(huc))

    elif isinstance(hucs, list) and len(hucs) > 1:
        for huc in hucs:
            huc_list.add(clean_huc_value(huc))
    else:  # assume it is a string but it could have more than one huc value in it
        hucs = hucs.strip()
        if hucs == "":
            raise ValueError("HUC list is empty")

        if " " in hucs:  # we have something like '12090301 05030104'
            raw_huc_list = hucs.split(" ")
            for huc in raw_huc_list:
                huc_list.add(clean_huc_value(huc))
        else:
            huc_list.add(clean_huc_value(hucs))

    return huc_list


# Sometimes value in HUC list files can have extra spaces or line break problems
# Mostly has to do with file encoding.
def clean_huc_value(huc):
    # Strips the newline character plus
    # single or double quotes (which sometimes happens)
    huc = huc.strip().replace("\"", "")
    huc = huc.replace("\'", "")
    return huc


def calculate_duration_msg(start_dt):
    '''
    Process:
    -------
    Calcuates the difference in time between the start and curerent end time (in UTC)
    and returns is as:

        Duration: 4 hours 23 mins 15 secs

    Make sure your start_dt is in UTC

    '''

    # if include_log and not isinstance( logging_instance, logging.Logger):
    #     raise Exception("You have requested to log the duration to file, but the logging_instance"
    #                     " does not appear to be an instance of logging.Logger (or a custom version).")

    end_dt = datetime.now(timezone.utc)
    # dt_string = end_dt.strftime("%m/%d/%Y %H:%M:%S")

    time_delta = end_dt - start_dt
    total_seconds = int(time_delta.total_seconds())

    total_days, rem_seconds = divmod(total_seconds, 60 * 60 * 24)
    total_hours, rem_seconds = divmod(rem_seconds, 60 * 60)
    total_mins, seconds = divmod(rem_seconds, 60)

    if total_days > 0:
        total_hours = (total_days * 24) + total_hours

    time_fmt = f"Duration: {total_hours:02d} hours {total_mins:02d} mins {seconds:02d} secs"

    return time_fmt


# #####################################
# TODO: Oct 2025: Get Rid of this class and move/adjust desired functions above.
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
    def print_date_time_duration(start_dt, end_dt, print_dur_msg=True):
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

        if total_days > 0:
            total_hours = (total_days * 24) + total_hours

        time_fmt = f"{total_hours:02d} hours {total_mins:02d} mins {seconds:02d} secs"

        duration_msg = "Duration: " + time_fmt
        if print_dur_msg:
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

#!/usr/bin/env python3

import datetime as dt
import os
import random
import traceback
from pathlib import Path


# Careful... You might not put shared_functions here as it can create circular references,
#  so shared_functions is put inside functions only/as is need

# Why does this class exist instead of useing standard python logging tools or other python packes?
# Some of those loggers cache the log file output until the end which can be a problem for crashes.
# Also, none of them are very good at handling multi-processing including file collisions and mixed
# multi-proc output data into the log file.

# Our answer.. A bit weird, but highly effective. Write to the log file as it goes along.
# For multi-processing, create a temp log for each process, then when the total process is done, merge
# those outputs in the master log file.


# This is not a perfect system. When rolling multi-proc logs into a rollup log, there can still be
# collisions but it is rare.


class FIM_logger:
    CUSTOM_LOG_FILES_PATHS = {}
    LOG_SYSTEM_IS_SETUP = False

    LOG_DEFAULT_FOLDER = ""
    LOG_FILE_PATH = ""  # full path and file name
    LOG_WARNING_FILE_PATH = ""
    LOG_ERROR_FILE_PATH = ""

    LOG_SYS_NOT_SETUP_MSG = "******  Logging to the file system not yet setup.\n"
    "******  Sometimes this is not setup until after initial validation.\n"

    """
    Levels available for use
      trace - does not show in console but goes to the default log file
      lprint - goes to console and default log file
        (use use "print" if you want console only)
      notice - goes to console and default log file (Adds word "NOTICE" to front of output)
      success - goes to console and default log file (Adds word "SUCCESS" to front of output)
      warning - goes to console, log file and warning log file (Adds word "WARNING" to front of output)
      error - goes to console, log file and error log file. Normally used when the error
          does not kill the application. (Adds word "ERROR" to front of output)
      critical - goes to console, log file and error log file. Normally used when the
          program is aborted. (Adds word "CRITICAL" to front of output)


    NOTE: While unconfirmed in the Linux world, special backslashs
    combo's like \r, \t, \n can get create problems.

    """

    # -------------------------------------------------
    def __get_dt(self):
        cur_dt = dt.datetime.now()
        ret_dt = f"{cur_dt.strftime('%Y-%m-%d')} {cur_dt.strftime('%H:%M:%S')}"
        return ret_dt

    # -------------------------------------------------
    def setup(self, log_file_path: str):
        """
        During this process, a second log file will be created as an error file which will
        duplicate all log message with the levels of ERROR, and CRITICAL.

        input:
            - log_file_path : ie) /data/catfim/test/logs/gen_catfim.log
        """

        # -----------
        # Validation
        if log_file_path is None:
            raise ValueError("Error: log_file_path not defined")
        log_file_path = log_file_path.strip()
        if log_file_path == "":
            raise ValueError("Error: log_file_path can not be empty")

        folder_path = os.path.dirname(log_file_path)
        log_file_name = os.path.basename(log_file_path)

        os.makedirs(folder_path, exist_ok=True)

        self.LOG_DEFAULT_FOLDER = folder_path

        # pull out the file name without extension
        file_name_parts = os.path.splitext(log_file_name)
        if len(file_name_parts) != 2:
            raise ValueError("The submitted log_file_name appears to be an invalid file name")

        self.__calc_warning_error_file_names(log_file_path)
        self.LOG_FILE_PATH = log_file_path

        # We need to remove the older ones if they already exist. Why? one attempt of running an script
        # might trigger and log file and an error file. So, it is correct, run again and now we have an
        # old invalid error file
        # if os.path.isfile(log_file_path):
        #     os.remove(log_file_path)

        # if os.path.isfile(self.LOG_ERROR_FILE_PATH):
        #     os.remove(self.LOG_ERROR_FILE_PATH)

        # if os.path.isfile(self.LOG_WARNING_FILE_PATH):
        #     os.remove(self.LOG_WARNING_FILE_PATH)

        self.LOG_SYSTEM_IS_SETUP = True
        return

    # -------------------------------------------------
    def MP_Log_setup(self, parent_log_output_file, file_prefix):
        """
        Overview:
            This is for logs used inside code that is multi-processing, aka. inside the actual functions
            of the call from Pool

            This method is sort of a wrapper in that it just manually creates a file name
            using a defined file path.
            The file name is calculated as such {file_prefix}-{date_with_milliseconds and random key}.log()
            ie) produce_geocurves-231122_1407441234_12345.log

            The extra file portion is added as in MultiProc, you can have dozens of processes
            and each are loggign to their own file. At then end of an MP, you call a function called merge_log_files
            which will merge them into a parent log file if requested.

        Inputs:
            file_prefix (str): a value to prepend to the file names. Often is the name of the function
               that called this method. Note: Later, when a person has these MP_Logs cleaned up
               they will use this file_prefix again to search and remove the temp MP_log files as they
               get rolled back up to the master log file.
            log_folder_path (str): folder location for the files to be created. Note: it has to be in
               the same folder as the master log file.
        """
        # -----------------
        log_folder = os.path.dirname(parent_log_output_file)
        file_id = self.get_date_with_milli()
        log_file_name = f"{file_prefix}-{file_id}.log"
        log_file_path = os.path.join(log_folder, log_file_name)

        self.setup(log_file_path)
        return

    # -------------------------------------------------
    def __calc_warning_error_file_names(self, log_file_and_path):
        """
        Process:
            Parses the log_file_and_path to add either the name of _warnings or _errors
            into the file name.
            Why not update LOG_WARNING_FILE_PATH and LOG_ERROR_FILE_PATH
        Input:
            log_file_and_path: ie) /data/outputs/rob_test/logs/catfim.log
        Output:
            Updates LOG_WARNING_FILE_PATH and LOG_ERROR_FILE_PATH variables
        """

        folder_path = os.path.dirname(log_file_and_path)
        log_file_name = os.path.basename(log_file_and_path)

        # pull out the file name without extension
        file_name_parts = os.path.splitext(log_file_name)
        if len(file_name_parts) != 2:
            raise ValueError("The submitted log_file_name appears to be an invalid file name")

        # now calc the warning log file
        self.LOG_WARNING_FILE_PATH = os.path.join(
            folder_path, file_name_parts[0] + "_warnings" + file_name_parts[1]
        )

        # now calc the error log file
        self.LOG_ERROR_FILE_PATH = os.path.join(
            folder_path, file_name_parts[0] + "_errors" + file_name_parts[1]
        )
        return

    # -------------------------------------------------
    def merge_log_files(self, parent_log_output_file, file_prefix):
        """
        Overview:
            This tool is mostly for merging log files during multi processing which each had their own file.

            This will search all of the files in directory in the same folder as the
            incoming log_file_and_path. It then looks for all files starting with the
            file_prefix and adds them to the log file (via log_file_and_path)
        Inputs:
            - log_file_and_path: ie) /data/outputs/rob_test/logs/catfim.log
            - file_prefix: This value must be the start of file names. ie) mp_create_gdf_of_points
                as in /data/outputs/rob_test/logs/mp_generate_categorical_fim(_231122_1407444333_12345).log
        """

        # -----------
        # Validation
        if parent_log_output_file is None:
            raise ValueError("Error: parent_log_file_and_path not defined")

        parent_log_output_file = parent_log_output_file.strip()

        if parent_log_output_file == "":
            raise ValueError("Error: parent log_file_and_path can not be empty")

        folder_path = os.path.dirname(parent_log_output_file)
        os.makedirs(folder_path, exist_ok=True)

        log_file_list = list(Path(folder_path).rglob(f"{file_prefix}*"))
        if len(log_file_list) > 0:
            log_file_list.sort()

            # self.lprint(".. merging log files")
            # we are merging them in order (reg files, then warnings, then errors)

            # open and write to the parent log
            # This will write all logs including errors and warning
            with open(parent_log_output_file, 'a+') as main_log:
                # Iterate through list
                for temp_log_file in log_file_list:
                    # Open each file in read mode
                    with open(temp_log_file) as infile:
                        main_log.write(infile.read())
                    os.remove(temp_log_file)

            # now the warning files if there are any
            log_warning_file_list = list(Path(folder_path).rglob(f"{file_prefix}*_warnings*"))
            if len(log_warning_file_list) > 0:
                log_warning_file_list.sort()
                parent_warning_file = parent_log_output_file.replace(".log", "_warnings.log")
                with open(parent_warning_file, 'a+') as warning_log:
                    # Iterate through list
                    for temp_log_file in log_warning_file_list:
                        # Open each file in read mode
                        with open(temp_log_file) as infile:
                            warning_log.write(infile.read())
                        os.remove(temp_log_file)

            # now the warning files if there are any
            log_error_file_list = list(Path(folder_path).rglob(f"{file_prefix}*_errors*"))
            if len(log_error_file_list) > 0:
                log_error_file_list.sort()
                parent_error_file = parent_log_output_file.replace(".log", "_errors.log")
                # doesn't yet exist, then create a blank one
                with open(parent_error_file, 'a+') as error_log:
                    # Iterate through list
                    for temp_log_file in log_error_file_list:
                        # Open each file in read mode
                        with open(temp_log_file) as infile:
                            error_log.write(infile.read())
                        os.remove(temp_log_file)

        # now delete the all file with same prefix (reg, error and warning)
        # iterate through them a second time (do it doesn't mess up the for loop above)
        # if len(log_file_list) > 0:
        #     for temp_log_file in log_file_list:
        #         try:
        #             os.remove(temp_log_file)
        #         except OSError:
        #             self.error(f"Error deleting {temp_log_file}")
        #             self.error(traceback.format_exc())
        return

    # -------------------------------------------------
    def trace(self, msg):
        # goes to file only, not console
        level = "TRACE   "  # keeps spacing the same
        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def lprint(self, msg):
        # goes to console and log file
        level = "LPRINT  "  # keeps spacing the same
        print(f"{msg} ")

        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def notice(self, msg):
        # goes to console and log file
        level = "NOTICE  "  # keeps spacing the same
        # print(f"{cl.fore.TURQUOISE_2}{msg}{cl.style.RESET}")
        print(f"{level}{msg}")

        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def success(self, msg):
        # goes to console and log file
        level = "SUCCESS "  # keeps spacing the same

        # c_msg_type = f"{cl.fore.SPRING_GREEN_2B}<{level}>{cl.style.RESET}"
        # print(f"{self.__get_clog_dt()} {c_msg_type} : {msg}")
        print(f"{level}{msg}")

        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def warning(self, msg):
        # goes to console and log file and warning log file
        level = "WARNING "  # keeps spacing the same

        # c_msg_type = f"{cl.fore.LIGHT_YELLOW}<{level}>{cl.style.RESET}"
        # print(f"{self.__get_clog_dt()} {c_msg_type} : {msg}")
        print(f"{level}{msg}")

        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

        # and also write to warning logs
        with open(self.LOG_WARNING_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def error(self, msg):
        # goes to console and log file and error log file
        level = "ERROR   "  # keeps spacing the same

        # c_msg_type = f"{cl.fore.RED_1}<{level}>{cl.style.RESET}"
        # print(f"{self.__get_clog_dt()} {c_msg_type} : {msg}")
        print(f"{level}{msg}")

        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

        # and also write to error logs
        with open(self.LOG_ERROR_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def critical(self, msg):
        level = "CRITICAL"  # keeps spacing the same

        # c_msg_type = f"{cl.style.BOLD}{cl.fore.RED_3A}{cl.back.WHITE}{self.__get_dt()}"
        # c_msg_type += f" <{level}>"
        # print(f" {c_msg_type} : {msg} {cl.style.RESET}")
        print(f"{level}{msg}")

        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

        # and also write to error logs
        with open(self.LOG_ERROR_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

    # -------------------------------------------------
    def get_date_with_milli(self, add_random=True):
        # This returns a pattern of YYMMDD_HHMMSSf_{random 5 digit} (f meaning milliseconds to 6 decimals)
        # Some multi processing functions use this for file names.

        # We found that some processes can get stuck which can create collisions, so we added a 5 digit
        # random num on the end (10000 - 99999). Yes.. it happened.

        # If add_random is False, the the 5 digit suffix will be dropped, output is 231122_1407444333
        # If add_ramdon is True, the output example would be 231122_1407444333_12345

        str_date = dt.datetime.utcnow().strftime("%y%m%d_%H%M%S%f")
        if add_random is True:
            random_id = random.randrange(10000, 99999)
            str_date += "_" + str(random_id)

        return str_date

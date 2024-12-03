#!/usr/bin/env python3

import datetime as dt
import os

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
    def calc_log_name_and_path(self, output_log_dir, file_prefix):
        # setup general logger
        os.makedirs(output_log_dir, exist_ok=True)
        start_time = dt.datetime.now(dt.timezone.utc)
        file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
        log_file_name = f"{file_prefix}_{file_dt_string}.log"
        log_output_file = os.path.join(output_log_dir, log_file_name)

        return log_output_file

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
    def MP_calc_prefix_name(self, parent_log_output_file, file_prefix, huc: str = ""):
        """_summary_
        Uses the file name of the parent log to create the new prefix name (without MP date)

        You don't need to use this method if you don't want to prepend the parent file name prefix
        """

        parent_log_file_name = os.path.basename(parent_log_output_file).replace(".log", "")

        if huc != "":
            prefix = f"{parent_log_file_name}--{huc}--{file_prefix}"
        else:
            prefix = f"{parent_log_file_name}--{file_prefix}"
        return prefix

    # -------------------------------------------------
    def MP_Log_setup(self, parent_log_output_file, file_prefix):
        """
        Overview:
            This is for logs used inside code that is multi-processing, aka. inside the actual functions
            of the call from Pool

            This method is sort of a wrapper in that it just manually creates a file name
            using a defined file path.

            As this is an MP file, the parent_log_output_file may have a date in it
            The file name is calculated as such 
            {file_prefix}-{currernt datetime with milli}.log()
            ie) catfim_2024_07_09-16_30_02__012345678901.log

            The extra file portion is added as in MultiProc, you can have dozens of processes
            and each are loggign to their own file. At then end of an MP, you call a function called merge_log_files
            which will merge them into a parent log file if requested.

        Inputs:
            file_prefix (str): a value to prepend to the file names. Often is the name of the function
               that called this method. Note: Later, when a person has these MP_Logs cleaned up
               they will use this file_prefix again to search and remove the temp MP_log files as they
               get rolled back up to the master log file.
            parent_log_output_file (str): folder location for the files to be created. Note: it has to be in
               the same folder as the master log file.
        """
        # -----------------
        log_folder = os.path.dirname(parent_log_output_file)

        # random_id = random.randrange(1000000000, 99999999999)
        # this is an epoch time
        dt_str = dt.datetime.now().strftime('%H%M%S%f')
        log_file_name = f"{file_prefix}___{dt_str}.log"
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
    def merge_log_files(self, parent_log_output_file, file_prefix, remove_old_files=True):
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

        log_file_list_paths = list(Path(folder_path).glob(f"*{file_prefix}*"))
        log_file_list = [str(x) for x in log_file_list_paths]
        
        if len(log_file_list) > 0:
            log_file_list.sort()

            # we are merging them in order (reg files, then warnings, then errors)

            # It is ok if it fails with a file not found. Sometimes during multi proc
            # merging, we get some anomylies. Rare but it happens.
            try:
                # open and write to the parent log
                # This will write all logs including errors and warning
                with open(parent_log_output_file, 'a') as main_log:
                    # Iterate through list
                    for temp_log_file in log_file_list:
                        # Open each file in read mode
                        with open(temp_log_file) as infile:
                            main_log.write(infile.read())
                        if "warning" not in temp_log_file and "error" not in temp_log_file:
                            if remove_old_files:
                                os.remove(temp_log_file)

                # now the warning files if there are any
                log_warning_file_list = list(Path(folder_path).rglob(f"{file_prefix}*_warnings*"))
                if len(log_warning_file_list) > 0:
                    log_warning_file_list.sort()
                    parent_warning_file = parent_log_output_file.replace(".log", "_warnings.log")
                    with open(parent_warning_file, 'a') as warning_log:
                        # Iterate through list
                        for temp_log_file in log_warning_file_list:
                            # Open each file in read mode
                            with open(temp_log_file) as infile:
                                warning_log.write(infile.read())

                            if remove_old_files:
                                os.remove(temp_log_file)

                # now the warning files if there are any
                log_error_file_list = list(Path(folder_path).rglob(f"{file_prefix}*_errors*"))
                if len(log_error_file_list) > 0:
                    log_error_file_list.sort()
                    parent_error_file = parent_log_output_file.replace(".log", "_errors.log")
                    # doesn't yet exist, then create a blank one
                    with open(parent_error_file, 'a') as error_log:
                        # Iterate through list
                        for temp_log_file in log_error_file_list:
                            # Open each file in read mode
                            with open(temp_log_file) as infile:
                                error_log.write(infile.read())

                            if remove_old_files:
                                os.remove(temp_log_file)
            except FileNotFoundError as ex:
                print(f"Merge file not found. Details: {ex}. Program continuing")

        return

    # -------------------------------------------------
    def trace(self, msg):
        # goes to file only, not console
        level = "TRACE".ljust(9)  # keeps spacing the same  (9 chars wide)
        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

        return

    # -------------------------------------------------
    def lprint(self, msg):
        # goes to console and log file
        level = "LPRINT".ljust(9)  # keeps spacing the same  (9 chars wide)
        print(f"{msg} ")

        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

        return

    # -------------------------------------------------
    def notice(self, msg):
        # goes to console and log file
        level = "NOTICE".ljust(9)  # keeps spacing the same  (9 chars wide)
        # print(f"{cl.fore.TURQUOISE_2}{msg}{cl.style.RESET}")
        print(f"{level}: {msg}")

        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

        return

    # -------------------------------------------------
    def success(self, msg):
        # goes to console and log file
        level = "SUCCESS".ljust(9)  # keeps spacing the same  (9 chars wide)

        # c_msg_type = f"{cl.fore.SPRING_GREEN_2B}<{level}>{cl.style.RESET}"
        # print(f"{self.__get_clog_dt()} {c_msg_type} : {msg}")
        print(f"{level}: {msg}")

        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

        return

    # -------------------------------------------------
    def warning(self, msg):
        # goes to console and log file and warning log file
        level = "WARNING".ljust(9)  # keeps spacing the same  (9 chars wide)

        # c_msg_type = f"{cl.fore.LIGHT_YELLOW}<{level}>{cl.style.RESET}"
        # print(f"{self.__get_clog_dt()} {c_msg_type} : {msg}")
        print(f"{level}: {msg}")

        if self.LOG_FILE_PATH == "":
            print(self.LOG_SYS_NOT_SETUP_MSG)
            return

        with open(self.LOG_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

        # and also write to warning logs
        with open(self.LOG_WARNING_FILE_PATH, "a") as f_log:
            f_log.write(f"{self.__get_dt()} | {level} || {msg}\n")

        return

    # -------------------------------------------------
    def error(self, msg):
        # goes to console and log file and error log file
        level = "ERROR".ljust(9)  # keeps spacing the same  (9 chars wide)

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

        return

    # -------------------------------------------------
    def critical(self, msg):
        level = "CRITICAL".ljust(9)  # keeps spacing the same (9 chars wide)

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

        return

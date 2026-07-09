#!/usr/bin/env python3

import argparse
import os
import re

import pandas as pd


# The keywords / regex expressions to search for in each log
# NOTE: these will be case-insensitive
ERROR_KW = [
    r"command exited with non-zero status",
    r"exit status: (?!0$|0\b)\d+",  # any exit status except 0
    r"(?<!no )error",  # the word "error" not preceded by "no"
    r"exception",
    r"command not found",
    r"parallel: warning:",
]

# Combine all of the error keywords into one expression
combined_pattern = "(?:{})".format("|".join(ERROR_KW))
# Compile the regexes
error_re = re.compile(combined_pattern, re.IGNORECASE)


def scan_error_log(huc_number, source_log_file, output_csv_path):
    """Main function to check the logs of the huc unit or rerun unit log file"""
    # Note: This scans only huc_{huc name}_unit.log and assumes comforably
    # that other scripts have ensure all of the outputs and errors are already
    # included in the unit.log. Based on our arch, this is the right answer
    # and we do not want to scan various logs ad I would create a ton of duplication

    # print(".......................................................")
    # print("    Searching for the words or phrases of (non case-sensitive):")
    # print("       command exited with non-zero status")
    # print("       exit status: (with one to 3 numbers, not counting 0. The colon may or may not exist)")
    # print("       error")
    # print("       exception")
    # command not found
    # print("       parallel: warning")
    # print(".......................................................")
    # print("")

    if not os.path.exists(source_log_file):
        raise FileNotFoundError(
            f"The huc log file of {source_log_file} does not seem to exist"
            " which is possible but highly unlikely"
        )
    lines_found = log_kw_search(source_log_file, huc_number)

    # Note: might be an empty file and that is ok.
    if len(lines_found == 0):
        print("GREAT JOB... no errors found.")
    else:
        output_df = pd.DataFrame(lines_found).astype(str)
        # print(output_df)
        output_df.to_csv(output_csv_path, index=False)
        print(f"Error log report saved to {output_csv_path}")


def log_kw_search(logfile, huc_number):
    """Checks a logfile for the keywords identified in ERROR_KW"""
    # Note: Not all errors will be in place using the word "status"
    # and some files may have that phrase more than one, so final status is not really valable

    found_lines = []

    current_line_num = 1
    status_code_pattern = r"(?i)status(?::\s*|\s+)([1-9]\d{0,2})(.*)"
    with open(logfile, "r") as log:
        # Search for a match to any of the keywords in each line
        # for line_num, line in enumerate(log, start=1):
        for line in log:
            match = error_re.search(line)
            if match:
                status_code_match = re.search(status_code_pattern, line, re.IGNORECASE)
                exit_code = ""
                if status_code_match:  # then look to see if it has a code in it
                    match_result = status_code_match.group()

                    # pattern could be "status 123" or "status: 123"
                    num_match = re.search(r'\d+', match_result)
                    exit_code = num_match.group()

                    if exit_code == "0":  # then skip
                        continue
                line_data = {
                    'huc_num': str(huc_number),
                    'exit_code': str(exit_code),
                    'line_num': str(current_line_num),
                    'text': line.strip(),
                    'log_path': logfile,
                }
                found_lines.append(line_data)
            current_line_num += 1

    # found_lines = "\n".join(found_lines)
    # found_text = "\n".join(found_text)
    # exit_codes = "\n".join(exit_codes)
    # Since HUC logs also have branches, replacing with the final exit code
    # does a better job of showing when a HUC fails as opposed to a branch
    # if final_exit_code is not None and (final_exit_code != exit_code):
    #     exit_code = final_exit_code

    return found_lines

    '''
    testing patterns:
    here is a line with no status
    here is a line with just a 0
    and one with status 0
    and one with status 1   (good)
    and one with status 12   (good)
    and one with status 123   (good)
    and one with status 0 with somethign behind it
    and one with status: 0 with somethign behind it
    and one with status: 123 with somethign behind it   (good)
    and one with status: 0
    and one with status: 1   (good)
    and one with status: 12   (good)
    and one with status: 123   (good)
    and just the word status with nothing else
    status
    status 0
    status 1   (good)
    status 12   (good)
    status 134   (good)
    status:0
    status:1   (good)
    status: 0
    status: 1   (good)
    status: 12   (good)
    status: 345   (good)
    status: 3456   (good)
    status: 12 3456  (good)
    '''


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Look for all errors in HUC folders')
    parser.add_argument('-u', '--huc-number', help='REQUIRED: The HUC number.', required=True)
    parser.add_argument(
        '-s', '--source-log-file', help='REQUIRED: Path for the log file to be scanned.', required=True
    )
    parser.add_argument(
        '-o', '--output-csv-path', help='REQUIRED: path of the csv report to be saved', required=True
    )
    args = vars(parser.parse_args())

    scan_error_log(**args)

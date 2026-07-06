#!/usr/bin/env python3

import argparse
import re
import sys
import traceback
from pathlib import Path

import pandas as pd


# The keywords / regex expressions to search for in each log
# NOTE: these will be case-insensitive
ERROR_KW = [
    r"command exited with non-zero status",
    r"exit status: (?!0$|0\b)\d+",  # any exit status except 0
    r"(?<!no )error",  # the word "error" not preceded by "no"
    r"exception",
    r"parallel: warning:",
]

# Combine all of the error keywords into one expression
combined_pattern = "(?:{})".format("|".join(ERROR_KW))
# Compile the regexes
error_re = re.compile(combined_pattern, re.IGNORECASE)
# final_exit_re = re.compile(r"exit status: (\d+)", re.IGNORECASE)

class HandDir(object):

    def __init__(self, path):
        self.root = Path(path)

    def __getattr__(self, name):
        # This delegates any unknown method calls (like .exists() or .mkdir())
        # directly to the underlying Path object.
        return getattr(self.root, name)

    def __repr__(self):
        return f"HandDir('{self.root}')"

    def iter_hucs(self):
        # returns only HUC8 directories
        return self.root.glob('[0-9]' * 8 + '/')

    def iter_branches(self, huc=None):
        if huc:
            huc_path = Path(huc)
            return huc_path.glob('[0-9]' * 10 + '/')
        else:
            # Return all branches of all HUCs
            for huc in self.iter_hucs():
                yield from huc.glob('branches/' + '[0-9]' * 10 + '/')

    def check_logs(self, output_csv):
        """Main function to check the logs of both HUC and branch level"""
        # Note: This scans only huc_{huc name}_unit.log and assumes comforably
        # that other scripts have ensure all of the outputs and errors are already
        # included in the unit.log. Based on our arch, this is the right answer
        # and we do not want to scan various logs ad I would create a ton of duplication

        # These are the columns that will be in the DataFrame and report CSV
        columns = ['level', 'exit_code', 'huc', 'path', 'line', 'text']
        all_lines_founds = []

        do_huc_folders_exist=False
        for huc_dir in self.iter_hucs():
            print(f"hucdir is {huc_dir}")            
            # Search huc logs
            print(f"and the name is {huc_dir.name}")
            huc_logs = Path(huc_dir, 'logs', f"huc_{huc_dir.name}_unit.log")
            # huc_lines, huc_text, exit_code = self.log_kw_search(huc_logs)
            lines_found = self.log_kw_search(huc_logs, huc_dir.name)
            if len(lines_found) > 0:
                all_lines_founds.extend(lines_found)
                do_huc_folders_exist=True

            # Add the lines to the report for any exits codes other than zero
            # if exit_code:
            #     print("we are here")
            #     outputs.append(
            #         pd.DataFrame(
            #             columns=columns,
            #             data=[['huc', exit_code, huc_dir.name, huc_logs, huc_lines, huc_text]],
            #         )
            #     )

            # Note: No need to searh branch logs as they are reduntant.
            # All branch errors are already rolled up in to its parent huc log file
            # # via the "tee" command
            # for branch in Path(huc, 'logs', 'branch').glob("*.log"):
            #     # Skip the summary log
            #     if "summary" in branch.name:
            #         continue
            #     branch_name = re.search(r"(\d{10})(.log)$", branch.name).group(1)
            #     branch_lines, branch_text, exit_code = self.log_kw_search(branch)
            #     # Add the lines to the report for any exits codes other than zero
            #     if exit_code:
            #         outputs.append(
            #             pd.DataFrame(
            #                 columns=columns,
            #                 data=[
            #                     [
            #                         'branch',
            #                         exit_code,
            #                         huc.name,
            #                         branch_name,
            #                         branch,
            #                         branch_lines,
            #                         branch_text,
            #                     ]
            #                 ],
            #             )
            #         )

        if not do_huc_folders_exist:
            raise Exception(f"{self.root} does not appear to have any huc folders. Please check pathing.")

        if len(all_lines_founds) == 0:
            # humm... do we want to raise an exception? it is an error, or did not honestly find any?
            #raise Exception(f"No errors or issues were found in the HUC folders. Searching against huc_(huc #)_unit.log files")
            print(f"No errors or issues were found in the HUC folders. Searching against huc_(huc #)_unit.log files")
        else:
            # Concatenate the report and save to CSV
            # output_df = pd.concat(outputs)
            output_df = pd.DataFrame(all_lines_founds).astype(str)
            # print(output_df)
            output_df.to_csv(output_csv, index=False)
            print(f"Error log report saved to {output_csv}")

    @staticmethod
    def log_kw_search(logfile, huc_num):
        """Checks a logfile for the keywords identified in ERROR_KW"""
        # Note: Not all errors will be in place using the word "status"
        # and some files may have that phrase more than one, so final status is not really valable

        found_lines = []

        # found_lines = []
        # found_text = []
        # exit_codes = []
        current_line_num = 1
        # final_exit_code = None
        # print(f"Log file is {logfile}")        
        with open(logfile, "r") as log:
            # Search for a match to any of the keywords in each line
            #for line_num, line in enumerate(log, start=1):
            for line in log:
                # print(f"current line number is {current_line_num}")
                match = error_re.search(line)
                if match:
                    # Append the line number and text
                    # found_lines.append(str(current_line_num))
                    # found_text.append(line.strip())
                    # If we can find an exit status code, extract it. Many lines will not have it
                    # as the search is for far more than just "status"
                    #status_code = re.match(r"([status:?\w]).*?(\d+)$", line.strip(), flags=re.IGNORECASE)
                    # status_code_pattern = r"\bstatus\s\d{1,3}\b"
                    status_code_pattern = r"(status)(:?)\s(\d{1,3})\s"
                    status_code_match = re.search(status_code_pattern, line, re.IGNORECASE)

                    exit_code = ""
                    if status_code_match:  # then look to see if it has a code in it
                        # status_code = re.findall(status_code_pattern, line, re.IGNORECASE )
                        # print(f"did we find an status code? and its value is ({status_code})")
                        # print(f"what did we find for the match group: {status_code_match.group()}")
                        exit_code = status_code_match.group(2)
                        #xit_codes.append(exit_code)
                        # print(f"and the status code if we found one is {exit_code}")
                        # exit_codes.append("")
                        # Scan for last exit code in the file
                        # if final_exit_re.search(line):
                        #     final_exit_code = int(final_exit_re.search(line).group(1))
                    line_data = {
                        'level': "huc",
                        'exit_code': str(exit_code),
                        'huc_num': str(huc_num),
                        'path': logfile,
                        'line_num': str(current_line_num),
                        'text': line.strip()
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

    # @staticmethod
    ### HUC list check


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Look for all errors in HUC folders')
    parser.add_argument('-n', '--hand_dir', help='REQUIRED: folder path where the HUC folder exists.', required=True)
    parser.add_argument('-o', '--output_csv_path', help='REQUIRED: path of the csv report to be saved', required=True)
    args = vars(parser.parse_args())
    hand = HandDir(args["hand_dir"])
    hand.check_logs(args["output_csv_path"])

       


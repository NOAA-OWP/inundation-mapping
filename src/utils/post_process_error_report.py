#!/usr/bin/env python3

import argparse
import re
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
final_exit_re = re.compile(r"exit status: (\d+)", re.IGNORECASE)


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

        # These are the columns that will be in the DataFrame and report CSV
        columns = ['level', 'exit_code', 'huc', 'branch', 'path', 'line', 'text']
        outputs = []

        for huc in self.iter_hucs():
            # Search huc logs
            huc_logs = Path(huc, 'logs', f"huc_{huc.name}_unit.log")
            huc_lines, huc_text, exit_code = self.log_kw_search(huc_logs)
            # Add the lines to the report for any exits codes other than zero
            if exit_code:
                outputs.append(
                    pd.DataFrame(
                        columns=columns,
                        data=[['huc', exit_code, huc.name, None, huc_logs, huc_lines, huc_text]],
                    )
                )

            # Search branch logs
            for branch in Path(huc, 'logs', 'branch').glob("*.log"):
                # Skip the summary log
                if "summary" in branch.name:
                    continue
                branch_name = re.search(r"(\d{10})(.log)$", branch.name).group(1)
                branch_lines, branch_text, exit_code = self.log_kw_search(branch)
                # Add the lines to the report for any exits codes other than zero
                if exit_code:
                    outputs.append(
                        pd.DataFrame(
                            columns=columns,
                            data=[
                                [
                                    'branch',
                                    exit_code,
                                    huc.name,
                                    branch_name,
                                    branch,
                                    branch_lines,
                                    branch_text,
                                ]
                            ],
                        )
                    )

        # Concatenate the report and save to CSV
        output_df = pd.concat(outputs)
        print(output_df)
        output_df.to_csv(output_csv, index=False)

    @staticmethod
    def log_kw_search(logfile):
        """Checks a logfile for the keywords identified in ERROR_KW"""

        found_lines = []
        found_text = []
        exit_code = None
        current_line = 1
        final_exit_code = None
        with open(logfile, "r") as log:
            # Search for a match to any of the keywords in each line
            for line in log:
                match = error_re.search(line)
                if match:
                    # Append the line number and text
                    found_lines.append(str(current_line))
                    found_text.append(line.strip())
                    # Search for an exit code in the line and pull it as an int
                    exit_code = re.match(r"([status:?\w]).*?(\d+)$", line.strip(), flags=re.IGNORECASE)
                    if exit_code:
                        exit_code = int(exit_code.group(2))
                # Scan for last exit code in the file
                if final_exit_re.search(line):
                    final_exit_code = int(final_exit_re.search(line).group(1))
                current_line += 1
        found_lines = "\n".join(found_lines)
        found_text = "\n".join(found_text)
        # Since HUC logs also have branches, replacing with the final exit code
        # does a better job of showing when a HUC fails as opposed to a branch
        if final_exit_code is not None and (final_exit_code != exit_code):
            exit_code = final_exit_code
        return found_lines, found_text, exit_code

    # @staticmethod
    ### HUC list check


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Look for all errors in HUC folders')
    parser.add_argument('-n', '--hand-dir', help='REQUIRED: folder path where the HUC folder exists.', required=True)
    parser.add_argument('-o', '--output-csv-path', help='REQUIRED: path of the csv report to be saved', required=True)
    
    args = vars(parser.parse_args())
    hand = HandDir(args['hand_dir'])
    hand.check_logs(args['output_csv_path'])

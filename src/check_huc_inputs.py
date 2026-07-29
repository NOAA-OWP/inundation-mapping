#!/usr/bin/env python3
import argparse
import os
import sys
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Validate input HUCs against a master reference HUC list.")
    parser.add_argument(
        "-u",
        "--hucInputs",
        required=True,
        help="Space or comma-delimited string of HUCs OR path to a file containing HUCs.",
    )
    parser.add_argument(
        "-i",
        "--masterHucList",
        required=True,
        help="Path to master/acceptable HUC list file (e.g., full_huc_list.lst).",
    )
    parser.add_argument(
        "-o",
        "--outputFile",
        required=True,
        help="Path to destination file where parsed valid HUCs will be written.",
    )
    return parser.parse_args()


def load_master_huc_set(master_list_path: Path) -> set:
    """Loads acceptable HUCs from the master list file into a set for fast lookup."""
    if not master_list_path.is_file():
        sys.stderr.write(f"Error: Master HUC list file '{master_list_path}' does not exist.\n")
        sys.exit(1)

    master_set = set()
    with open(master_list_path, "r") as f:
        for line in f:
            huc = line.strip().strip('"').strip("'")
            if huc and not huc.startswith("#"):
                # Zero-pad to 8 digits if numeric to handle lost leading zeroes
                if huc.isdigit() and len(huc) < 8:
                    huc = huc.zfill(8)
                master_set.add(huc)

    if not master_set:
        sys.stderr.write(f"Error: Master HUC list file '{master_list_path}' is empty.\n")
        sys.exit(1)

    return master_set


def parse_raw_huc_inputs(huc_input_arg: str) -> list:
    """
    Parses raw HUC input argument which can be either a file path
    or a space/comma/newline-delimited string of HUCs.
    """
    input_path = Path(huc_input_arg.strip())

    # Case 1: Input is a file path
    if input_path.is_file():
        raw_tokens = []
        with open(input_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    # Split comma/space separated lines inside the file
                    tokens = line.replace(",", " ").split()
                    raw_tokens.extend(tokens)
        return raw_tokens

    # Case 2: Input is a delimited string
    cleaned_str = huc_input_arg.strip().strip('"').strip("'")
    tokens = cleaned_str.replace(",", " ").split()
    return tokens


def check_hucs(huc_inputs: str, master_huc_list_path: str, output_file_path: str) -> int:
    """
    Core validation function.
    Validates input HUCs, writes output file, and returns valid count.
    """
    master_path = Path(master_huc_list_path)
    output_path = Path(output_file_path)

    # 1. Load acceptable master set
    master_set = load_master_huc_set(master_path)

    # 2. Parse raw inputs
    raw_hucs = parse_raw_huc_inputs(huc_inputs)
    if not raw_hucs:
        sys.stderr.write("Error: No HUC inputs provided.\n")
        sys.exit(1)

    # 3. Process and validate
    valid_hucs = []
    invalid_hucs = []

    for h in raw_hucs:
        h_clean = h.strip().strip('"').strip("'")
        if not h_clean:
            continue

        # Zero-pad leading zero if lost during shell expansion (e.g. 5030104 -> 05030104)
        if h_clean.isdigit() and len(h_clean) < 8:
            h_clean = h_clean.zfill(8)

        if h_clean in master_set:
            if h_clean not in valid_hucs:
                valid_hucs.append(h_clean)
        else:
            invalid_hucs.append(h_clean)

    # 4. Handle validation errors
    if invalid_hucs:
        sys.stderr.write(
            f"Error: The following HUC(s) were not found in master list '{master_path}':\n"
            f"  {', '.join(invalid_hucs)}\n"
        )
        sys.exit(1)

    if not valid_hucs:
        sys.stderr.write("Error: Zero valid HUCs matched the master list.\n")
        sys.exit(1)

    # 5. Write validated HUC list to output destination
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        for huc in valid_hucs:
            f.write(f"{huc}\n")

    return len(valid_hucs)


def main():
    args = parse_args()
    count = check_hucs(args.hucInputs, args.masterHucList, args.outputFile)
    # Print integer count to stdout so parent scripts can capture it
    print(count)


if __name__ == "__main__":
    main()

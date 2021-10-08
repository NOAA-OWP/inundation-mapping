#!/usr/bin/env python3
import argparse
import os


def archive_cleanup(archive_cleanup_path, additional_whitelist):
    """
    Processes all archived job outputs from a given path to keep only necessary files

    Parameters
    ----------
    archive_cleanup_path : STR
        Path to the archived outputs
    additional_whitelist : STR
        Additional list of files to keep
    """

    # List of files that will be saved by default
    whitelist = [
        "rem_zeroed_masked.tif",
        "rem_clipped_zeroed_masked.tif",
        "gw_catchments_reaches_filtered_addedAttributes.tif",
        "gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg",
        "gw_catchments_reaches_clipped_addedAttributes.tif",
        "gw_catchments_reaches_clipped_addedAttributes_crosswalked.gpkg",
        "hydroTable.csv",
        "gw_catchments_pixels.tif",
        "dem_burned_filled.tif",
        "demDerived_reaches.dbf",
        "demDerived_reaches.prj",
        "demDerived_reaches.shp",
        "demDerived_reaches.shx",
    ]

    # Add any additional files to the whitelist that the user wanted to keep
    if additional_whitelist:
        whitelist = whitelist + [
            filename for filename in additional_whitelist.split(",")
        ]

    # Delete any non-whitelisted files
    directory = os.fsencode(archive_cleanup_path)
    for subdir in os.listdir(directory):
        subdirname = os.fsdecode(subdir)
        if subdirname != "logs" and subdirname != "aggregate_fim_outputs":
            for file in os.listdir(os.path.join(archive_cleanup_path, subdirname)):
                filename = os.fsdecode(file)
                if filename not in whitelist:
                    os.remove(os.path.join(archive_cleanup_path, subdirname, filename))


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(description="Cleanup archived output files")
    parser.add_argument(
        "archive_cleanup_path", type=str, help="Path to the archived job outputs"
    )
    parser.add_argument(
        "-w",
        "--additional_whitelist",
        type=str,
        help="List of additional files to keep",
    )

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    # Rename variable inputs
    archive_cleanup_path = args["archive_cleanup_path"]
    additional_whitelist = args["additional_whitelist"]

    # Run archive_cleanup
    archive_cleanup(archive_cleanup_path, additional_whitelist)

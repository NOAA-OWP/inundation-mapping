#!/bin/bash -e


# Note: This is pretty rough with alot of hardcoding
# and is used for rtx FIM_30 at this time


# Remember to run EXPORT lastest 3 AWS key variables in the same window
# before running this.

# As well as the export TRG_ROOT='/efs drive name/>>>>>>(some path)/20241115'
# at command line prior to running the script


# NOTE: This solution is not complete. This script loads just one S3 folder at a time.
# I will build a wrapper script that can feed in a whole list and jsut iterate through these
# The key is trapping errors which has proven to be many and getting stats on it as well.

# This also could be change to python scripts... TBD
# We also could jsut pulll down everyting in the remote S3 bucket in one large pull
# but it is prone to timing out and erroring with such a large pull

# Besides.. this filters out just folders and files we need, dropping the total download
# size by appx 80%.

# YES, YES.... s3 buckets don't really have folders, just prefixes
# but for simplicity here, we will call them folders

:
usage_msg()
{
    echo "Sample Usage:  sh get_s3_folder.sh -n 'mip_03170004'

    Usage : get_s3_folder.sh -n <key_

    All arguments to this script are passed to 'fim_pre_processing.sh'.
    REQUIRED:
      -b/--bucket_name  : S3 bucket name
      -r/--root_prefix  : Folder name after bucket before folder list
                          ie)  /ripple/30_pcnt_domain/collections
                          as in s3://(some bucket)/ripple/30_pcnt_domain/collections
      -f/--folder_list  : list of folders at a given level, each item is just a single s3 folder (key)
                        : name. ie) mip_03170
      -t/--trg_folder   : Root local folder path for downloads
      -n/--runName      : A name to tag the output directories and log files (only alphanumeric).

    "
}
#!/bin/bash -e


# Note: This is pretty rough with a lot of hardcoding
# and is used for rtx FIM_30 at this time, but can easily be upgraded later if required.

# Why bash?  Easier than having to load a FIM docker image or create a conda enviro in order
# to use python and other packages.

# Remember to run EXPORT lastest 3 AWS key variables in the same window
# before running this.

# This file takes in a single folder name or list of folder and iterates through them to pass into
# get_s3_folder.sh. This tool is just a simple wrapper to get_s3_folder which only does one folder(key)

# Why does it not just do one big s3 sync for the entire s3 folder(key)?
# We want to manage it a bit more carefully. We also want stats such as:
#   - Size of the downloaded the model (key folder name)
#   - Duration of download per model (key)
#   - A count of how many library extent folder exist. Each library extent folder is one
#     Ripple HECRAS feature.

# We also do not know the total size or duration of the source directory so we want to manage it
# a bit more than usual

# Also.. if we get an error, abort the entire process

# YES, YES.... s3 buckets don't really have folders, just prefixes
# but for simplicity here, we will call them folders

# At this time we do not aim for parellization as aws cli sync has some built in
# and when I watch the network traffic, it is pretty much maxed out even without parallelization.

:
usage_msg()
{
    echo "This takes a single S3 folder key name (not full s3 folder path), then use
    the incoming single, multiple or txt file with list of the s3 folder key names.
    
    Sample Usage:  sh get_s3_folder_from_list.sh -m 'mip_03170004'

    Sample Usage:  sh get_s3_folder_from_list.sh
                -s 's3://(somebucket)/ripple/30_pcnt_domain/collections'
                -list 'mip_03170004' (or multiple or file. See notes below)
                -t '/home/rdp-user/output/ripple/fim_30'
  

    # NOTE: for now.. Leave off all starting and trailing slashes.

    All arguments to this script are passed to 'fim_pre_processing.sh'.
    REQUIRED:
      -s/--s3_source_path      : Full s3 bucket and common prefix.
                                 Parent folder where all child key folders live
                                   ie) s3://(somebucket)/ripple/30_pcnt_domain/collections
      -list/--list_of_keys     : This is a simple text file list of all collection (model folders)
                                 to be downloaded. You can submit a list file, a single collection
                                 (folder name) or multiple folder names:
                                   ie:  '/home/rdp-user/ripple/fim_30_collection_names.txt'
                                   or   'mip_03170004'
                                   or   'mip_03170004 ble_03170004' (space delimited)

                                 If you are submitting a file, each line in the file is a key folder
                                 name.

                                 The list of keys will be the subfolder at just the one level below
                                 the s3_source_path.
                                   ie) s3://(somebucket)/ripple/30_pcnt_domain/collections/mip_03170004

      -t/--trg_path            : Root local folder path for downloads.
                                   ie) ~/fim_30

    OPTIONS:
      -h/--help                 : Print usage statement.
    "
}

set -e

while [ "$1" != "" ]; do
    case $1
    in
    -s|--s3_source_path)
        shift
        s3_source_path=$1
        ;;
    -list/--list_of_keys)
        shift
        list_of_keys=$1
        ;;
    -t|--trg_path)
        shift
        trg_path=$1
        ;;
    -h|--help)
        shift
        usage_msg
        exit
        ;;
    *) ;;
    esac
    shift
done

file_date=$(date +"%Y_%m_%d")
log_file="${trg_path}/s3_download_log_${file_date}.txt"
csv_stats_file="${trg_path}/ripple_download_stats.csv"

# chmod 777 ./get_s3_folder.sh

echo
echo "======================= Start of loading model folders ========================="
echo "---- Started: `date -u`"

script_path="$(echo $(pwd) | tr -d ' ')/get_s3_folder.sh"
echo $script_path

source_args="-s '$s3_source_path' -n 'mip_03170004' -t '$trg_path' -log '$log_file' -c '$csv_stats_file'"
echo "$source_args"
./get_s3_folder.sh -s "$s3_source_path" -n "mip_03170004" -t "$trg_path" -log "$log_file" -c "$csv_stats_file"
#./test.sh "sup"


#!/bin/bash -e

# ***  FC means Ripple Feature Collection (a folder for inside the Ripple root data dirs) ***

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
#   - Size of the downloaded the FC (key folder name)
#   - Duration of download per FC (key)
#   - A count of how many library extent folder exist. Each library extent folder is one
#     Ripple HECRAS feature.

# We also do not know the total size or duration of the source directory so we want to manage it
# a bit more than usual

# Also.. if we get an error, abort the entire process

# YES, YES.... s3 buckets don't really have folders, just prefixes
# but for simplicity here, we will call them folders

# At this time we do not aim for parallization as aws cli sync has some built in
# and when I watch the network traffic, it is pretty much maxed out even without parallelization.

# *****************************************
# PREPARING AND RUNNING DATA
# Mar 2025:
# While not overly elegant at this point, we took some shortcuts in the name of effort/reward.
#
# STEPS FOR PREP:
#   - Did a simple aws ls at the level where all of the FC folders were at

#   - Copied / Pasted from screen into a doc, then saved it in files of 50.  Becuase the export
#     can time out, sets of 50 worked ok, but a good handful woudl time out before even finishing
#     50 collections. When a set of 50 timed out, I manually just split that set of "50" names
#     into a smaller subset with the remaining models in that set of 50 that did not complete.

#   - Ran this script passing in which set of 50 script (ie.. ripple_download_set_20.txt)
#     I was able to get multiple machines pulling at the same time.

# Runtime notes:
#   - If the tool failed on a record, the system by design shuts down. That way we could manually adjust
#     files and folders before conitnuing on. When it failed on a FC, it would start the
#     the folder, and start filling it.  If it did not finish that FC, it has no record in the
#     the stats file. AWS time outs happened about 5 times over the first set of 485 FCs for
#     FIM_30 (split into sets of 50). One set of 50 had to be split twice as it was soo big / long.

#   - When it failed, I deleted the last WIP feature collection folder which at least started downloading.
#     But restarting that folder entirely, I picked up correct stats versus "sync" which would have
#     shortened the download time if partially there aleady. I would also ensure the stats file was
#     happy.
#
#   - One HUC may have more than one FC folder. A FC folder is a source plus a HUC. ie) mip_12090301
#     and/or ble_12090301. A small amount of HUCs have more than one source as of FIM_30.
#
# Overall Metrics:
#     I don't have a tool for this, but I manually copied/pasted all of the contents of each stats
#     file into a master google sheets. Then I could sort out things like:
#        - Which HUCs has an FC folder but no features, library extent folders, in it.
#        - Number of features in each FC folder.
#        - The source type for that FC folder. ie) mip  versus  ble  (we only have two at this point)

# Data for the HECRAS Boundary Service plus usage for forecast processing.
#     A seperate simple JupyterLab was created to quickly make up a csv dataset for the
#     HECRAS Boundary service. It previously was called ras2fim Boundary Service but now has a new name.
#     For more details, see the "hecras_boundaries.ipynb" file which creates the final csv (db).
#
# *****************************************

# *** Remember:  You can always daisy change bash commands together with a semi-colon (one big line).
#    ie) sh get_s3_folder_from_list.sh {your args} -list '/home/rdp-user/ripple/names_set_1.txt' ; sh
#          get_s3_folder_from_list.sh {your args} -list '/home/rdp-user/ripple/names_set_2.txt' ; etc
#

:
usage_msg()
{
    echo "This takes a single S3 folder key name (not full s3 folder path), then use
    the incoming single, multiple or txt file with list of the s3 folder key names.
 
    Sample Usage:  sh get_s3_folder_from_list.sh
                -s 's3://(somebucket)/ripple/30_pcnt_domain/collections'
                -list 'mip_03170004' (or multiple or file. See notes below)
                -t '/home/rdp-user/output/ripple/fim_30'

             or -list '/home/rdp-user/ripple/fim_30_collection_names.txt'


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

    NOTE: You can always daisy change bash commands together with a semi-colon, one big line.
    ie) sh get_s3_folder_from_list.sh {your args} -list '/home/rdp-user/ripple/names_set_1.txt' ; sh
     get_s3_folder_from_list.sh {your args} -list '/home/rdp-user/ripple/names_set_2.txt' ;  etc
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
    -list|--list_of_keys)
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

# Yes.. this is duplicated for now (couldnt' figure how to read function from other script)
calc_Duration( ) {
    local start_time=$1
    local end_time=`date +%s`

    local total_sec=$(( $end_time - $start_time ))
    local dur_min=$((total_sec / 60))
    local dur_remainder_sec=$((total_sec %60))
    local dur_sec_percent=$((100*$dur_remainder_sec/60))

    echo -e "${dur_min}.${dur_sec_percent}"
}

file_date=$(date '+%Y%m%d_%H%M%S')
log_file="${trg_path}/s3_download_log_${file_date}.txt"
csv_stats_file="${trg_path}/ripple_download_stats.csv"

# let's split the incoming list_of_keys into an array we can iterate through
# I don't want to use parallization at this point as even with just one aws sync running
# it hits the network near max. aws sync has it own form of paralliation in it via chunking

# echo "++ $list_of_keys ++"
echo "Logs going to ${log_file}"

# First. let's see if it is file path or string with spaces.
if [ ! -e "$list_of_keys" ]
then
    # Not a file, let check if it single collection folder name or multiple
    # echo "list is now $list_of_keys"
    # Split the string by space
    arr_key_names=(${list_of_keys//' '/ })
else
    # load the file and turn into an array
    msg="loading the file list of model names from $list_of_keys"
    echo -e $msg ; echo "$msg" >> ${log_file}
    readarray -t arr_key_names < "${list_of_keys}"
fi

# for key in "${arr_key_names[@]}"; do
#     #echo "inside the loop"
#     if [ -n "$key" ]; then
#         echo ".. ${key}.."
#     else
#         echo "empty"
#     fi
# done

t_overall_start=`date +%s`
echo
echo "======================= Start of loading feature collection folders ========================="
msg="---- Started: `date -u`"
echo -e $msg ; echo "$msg" >> ${log_file}

for key in "${arr_key_names[@]}"; do
    if [ -n "$key" ]; then
        # echo ".. ${key}.."
        ./get_s3_folder.sh -s "$s3_source_path" -n "$key" -t "$trg_path" -log "$log_file" -c "$csv_stats_file"
    fi
done

echo
msg="---- Ended: `date -u`"
echo -e $msg ; echo "$msg" >> ${log_file}
dur="$(calc_Duration $t_overall_start)"
msg="Overall processing duration (in percent minutes) : $dur mins"
echo -e $msg ; echo "$msg" >> ${log_file}
echo "======================= End of loading feature collection folders ========================="
echo
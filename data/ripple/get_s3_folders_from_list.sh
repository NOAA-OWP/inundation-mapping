#!/bin/bash -e

# ### DO NOT RUN IN A DOCKER CONTAINER

# PS.. bash get_s3_folders_from_list.sh" instead of   ./get_s3_folder.sh  (dot)

# ***  MC means Ripple Model Collection (a folder for inside the Ripple root data dirs) ***

# Note: This is pretty rough with a lot of hardcoding
# and is used for rtx FIM_100 at this time, but can easily be upgraded later if required.

# Why bash?  Easier than having to load a FIM docker image or create a conda enviro in order
# to use python and other packages.

# THIS SHOULD NOT BE RUN IN A DOCKER IMAGE, just regular terminal window
#    If you get an error sayign "parallel command not found", use
#    sudo apt install parallel

# AWS Permissions
# You will need two AWS sets of keys (profiles), one for RTX and one for FIM in order to 
# download and upload in one terminal window. Details of how to set this up and what
# what values can not be provided here. See Rob H in the interium until we get it documented
# somewhere
# As of Jul 2025, we are looking into non expiring aws keys and session
# tokens, or possible upgraded permissions to help go from S3 to S3. If we get to update
# from S3 to S3, we will create new tools to scan S3 and pull download each metrics file.

# This file takes in a single folder name or list of folder and iterates through them to pass into
# get_s3_folder.sh. This tool is just a simple wrapper to get_s3_folder which only does one folder(key)

# Why does it not just do one big s3 sync for the entire s3 folder(key)?
# We want to manage it a bit more carefully. We also want stats such as:
#   - Size of the downloaded the FC (key folder name)
#   - Duration of download per FC (key)
#   - A count of how many library extent folder exist. Each library extent folder is one
#     Ripple HECRAS feature.

# We also do not know the total size or duration of the source directory so we want to manage it
# a bit more than usual. Each MC is logged in its own csf for download meta data.

# Also.. if we get an error, abort the entire process

# YES, YES.... s3 buckets don't really have folders, just prefixes
# but for simplicity here, we will call them folders

# At this time we do not aim for parallization as aws cli sync has some built in
# and when I watch the network traffic, it is pretty much maxed out even without parallelization.

# *****************************************
# PREPARING AND RUNNING DATA
# July 2025:
# While not overly elegant at this point, we took some shortcuts in the name of effort/reward.
#
# STEPS FOR PREP:
#   - Did a simple aws ls at the level where all of the MC folders were at

#   - Copied / Pasted from screen into a doc, then saved it in files of 50.  Becuase the export
#     can time out, sets of 50 worked ok, but a good handful woudl time out before even finishing
#     50 collections. When a set of 50 timed out, I manually just split that set of "50" names
#     into a smaller subset with the remaining models in that set of 50 that did not complete.

#   - Ran this script passing in which set of 50 script (ie.. ripple_download_set_20.txt)
#     I was able to get multiple machines pulling at the same time.
#   - I found it best that if it did timeout, just make a newer version of the downloading feed
#     file so it does not attempt to re-download / upload all MC's from the original list.
#     It is not pretty but it has to be good enough for now until we get time for something more
#     robust.

# Runtime notes:
#   - If the tool failed on a record, the system by design shuts down. Each MC downloaded and 
#     uploaded use aws s3 sync commands to help pick up where it left off. With each MC having it's
#     one download meta csv, when sync is used it might slightly skew the download times which is fine
#     the folder, and start filling it. The download log and stats files are overwritting for each
#     download attempt.  If it did not finish that FC, it has no record in the
#     the stats file. AWS time outs happened about 5 times over the first set of 485 FCs for
#     FIM_30 (split into sets of 50). One set of 50 had to be split twice as it was soo big / long.

#   - One HUC may have more than one MC folder. A MC folder is a source plus a HUC. ie) mip_12090301
#     and/or ble_12090301. A small amount of HUCs have both ble and mip for FIM_100.
# 
#
# Overall Metrics:
#     I will make a jupyter tool that can find and concat all MC stats file.
#     The tool will have the ability to make master csv's that can be copied / pasted into our
#     master MC tracking page
# Data for the HECRAS Boundary Service plus usage for forecast processing.
#     A seperate simple JupyterLab was created to quickly make up a csv dataset for the
#     HECRAS Boundary service. It previously was called ras2fim Boundary Service but now has a new name.
#     For more details, see the "hecras_boundaries.ipynb" file which creates the final csv (db).
#
# *****************************************

# *** Remember:  You can always change bash commands together with a semi-colon (one big line).
#    ie) bash get_s3_folder_from_list.sh {your args} -list '/home/your-user/ripple/names_set_1.txt' ; ./
#          get_s3_folder_from_list.sh {your args} -list '/home/your-user/ripple/names_set_2.txt' ; etc

# PS.. bash get_s3_folders_from_list.sh" instead of   ./get_s3_folder.sh  (dot)

# abort on any fails
set -e

:
usage_msg()
{
    echo "This takes a single S3 folder key name (not full s3 folder path), then use
    the incoming single, multiple or txt file with list of the s3 folder key names.
  
    ### DO NOT RUN IN A DOCKER CONTAINER

    Sample Usage:  bash get_s3_folders_from_list.sh
                -src 's3://(somebucket)/ripple/fim_100_domain/collections'
                -sap 'rtx-profile-name'                
                -list 'mip_03170004' (or a file with a list of MC names. See notes below)
                -ltrg '/home/your/output/ripple/fim_30'
                -stats '/efs.../ripple/fim_100_prod_data/download_stats'
                -log '/efs.../ripple/fim_100_prod_data/download_logs'               
                -targ 's3://(somebucket)/fim/ripple_100/collections'
                -tap 'ti-temp'
                -j 10

             or -list '/home/your-user/ripple/fim_30_collection_names.txt'

    Notes:
       - Almost all args are passed to each get_s3_folder. The only differnces is 
         the list arg which can take one MC folder name or a list of MC folder names
         including in a .txt file.
       - This script does not make a master log file, but each MC makes it's own via
         the get_s3_folders.sh file.
       - Also, pathing does not like tilde's in them. 
             ie) ~/temp won't work but /home/some-user/temp will

    All arguments to this script are passed to 'get_s3_folders.sh'.

    REQUIRED:
      -src/--s3_source_path    : Source Full s3 bucket and common prefix.
                                 Parent folder where all child key folders live
                                   ie) s3://(somebucket)/ripple/30_pcnt_domain/collections
      -sap/--src_aws_profile_name: Name of the cli aws profile name for the src download.                                   
      -ltrg/--local_trg_path    : Root local folder path for downloads.
                                   ie) /home/some-user/temp_ripple_downloads
      -stats/--stats_folder     : For each collection folder (key_name) downloaded, a unique stats file
                                 will be created with meta data about the download.
                                 This saves a header line of:
                                 (folder_name, download_size, num_models, date_downloaded)
                                 The file name pattern will be download_stats_{key_name}.csv
      -log/--log_folder        : Location where the log files for stored                                 
      -j/--num_jobs            : This can process multiple MC's at one. Enter the number of 
                                 concurrent processes you want. Default is 1.

      -list/--list_of_keys     : This is a simple text file list of all collection (model folders)
                                 to be downloaded. You can submit a list file, a single collection
                                 (folder name) or multiple folder names:
                                   ie:  '/home/your-user/ripple/fim_30_collection_names.txt'
                                   or   'mip_03170004'
                                   or   'mip_03170004 ble_03170004' (space delimited)

                                 If you are submitting a file, each line in the file is a key folder
                                 name.

                                 The list of keys will be the subfolder at just the one level below
                                 the s3_source_path.
                                   ie) s3://(somebucket)/ripple/30_pcnt_domain/collections/mip_03170004
      -targ/--s3_target_root      : Optional: s3://(some bucket)/fim/ripple_100/collections
      -tap/--trg_aws_profile_name: Optional: Name of the cli aws profile name for the target upload.                                      

    OPTIONS:
      -h/--help                 : Print usage statement.
    "
}

set -e

while [ "$1" != "" ]; do
    case $1
    in
    -src|--s3_source_path)
        shift
        s3_source_path=$1
        ;;
    -sap|--src_aws_profile_name)
        shift
        src_aws_profile_name=$1
        ;;
    -list|--list_of_keys)
        shift
        list_of_keys=$1
        ;;
    -ltrg|--local_trg_path)
        shift
        local_trg_path=$1
        ;;
    -stats|--stats_folder)
        shift
        stats_folder=$1
        ;;
    -log|--log_folder)
        shift
        log_folder=$1
        ;;        
    -targ|--s3_target_root)
        shift
        s3_target_root=$1
        ;;
    -tap|--trg_aws_profile_name)
        shift
        trg_aws_profile_name=$1
        ;;
    -j|--num_jobs)
        shift
        num_jobs=$1
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

echo
echo "======================= Start of loading model collection folders ========================="
echo "---- Started: `date -u`"
echo "............................................"

# ==========================================================
# VALIDATION
# print usage if arguments empty
if [ "$s3_source_path" = "" ]; then
    echo "ERROR: Missing -src (s3 source path)"
    usage_msg
    exit 22
fi

if [ "$src_aws_profile_name" = "" ]; then
    echo "ERROR: Missing -sap (source aws profile name)"
    usage_msg
    exit 22
fi

if [ "$list_of_keys" = "" ]; then
    echo "ERROR: Missing -list (list of MC folder names or file with a list of them)"
    usage_msg
    exit 22
fi

if [ "$local_trg_path" = "" ]; then
    echo "ERROR: Missing -ltrg (local temp target path)"
    usage_msg
    exit 22
fi

if [ "$stats_folder" = "" ]; then
    echo "ERROR: Missing -stats (stats folder path name)"
    usage_msg
    exit 22
fi

if [ "$log_folder" = "" ]; then
    echo "ERROR: Missing -log (log file folder path)"
    usage_msg
    exit 22
fi

upload_to_trg_s3="true"
if [ "$s3_target_root" = "" ]; then
#     echo "ERROR: Missing -targ (S3 target path)"
#     usage_msg
#     exit 22
    upload_to_trg_s3="false"
fi

if [ "$trg_aws_profile_name" = "" ]; then
#     echo "ERROR: Missing -tap (target aws profile name)"
#     usage_msg
#     exit 22
    upload_to_trg_s3="false"
fi

# TODO: Lots more validation such as extensions, valid s3 paths, etc

# take off last slash if there is one.
src_s3_uri="${src_s3_uri%/}"
local_trg_path="${local_trg_path%/}"
stats_folder="${stats_folder%/}"
log_folder="${log_folder%/}"
s3_target_root="${s3_target_root%/}"

# ===========================================
# let's split the incoming list_of_keys into an array we can iterate through
# I don't want to use parallization at this point as even with just one aws sync running
# it hits the network near max. aws sync has it own form of paralliation in it via chunking

echo "++ $list_of_keys ++"
# First. let's see if it is file path or string with spaces.
if [ ! -e "$list_of_keys" ]
then
    # Not a file, let check if it single collection folder name or multiple
    # echo "list is now $list_of_keys"
    # Split the string by space
    arr_key_names=(${list_of_keys//' '/ })
else
    # load the file and turn into an array
    msg="loading the file list of model collection (folder) names from $list_of_keys"
    echo -e $msg
    readarray -t arr_key_names < "${list_of_keys}"
fi

if [ "$num_jobs" = "" ]; then num_jobs=1; fi

source ./ripple_shared_tools.sh

# ===========================================
# setup error and warning log folders
mkdir -p $log_folder
error_folder_name="$log_folder/error_scans"
mkdir -p $error_folder_name

# ===========================================
t_overall_list_start=`date +%s`

run_script_with_args() {
    local input_cur_key="$1"

    # trim spaces of the end if any
    cur_key="${input_cur_key%%*([[:space:]])}"    
    #cmd="./get_s3_folder.sh"
    # cmd="get_s3_folder.sh"

    # Sometimes we get a bad key such as a key with a blank string
    # this happens the list being loaded might have extra blank lines
    if [ "$cur_key" != "" ]; then
        cmd=" -src $s3_source_path -n $cur_key -ltrg $local_trg_path"
        cmd+=" -stats $stats_folder -log $log_folder"
        cmd+=" -sap $src_aws_profile_name"
        if [[ "${upload_to_trg_s3}" == "true" ]]; then
            cmd+=" -tap $trg_aws_profile_name"
            cmd+=" -targ $s3_target_root"
        else
            echo "-- Target S3 arguments not provided, skipping upload"
        fi
        echo "$cmd"
        bash get_s3_folder.sh $cmd
        sleep 1
    fi
}

# so the parallel can see the args
export -f run_script_with_args
export s3_source_path=$s3_source_path
export src_aws_profile_name=$src_aws_profile_name
export local_trg_path=$local_trg_path
export stats_folder=$stats_folder
export log_folder=$log_folder
export upload_to_trg_s3=$upload_to_trg_s3

# Send these anyways even though they might be empty
export s3_target_root=$s3_target_root
export trg_aws_profile_name=$trg_aws_profile_name

echo "............................................"
echo "---- Starting: `date -u`"

echo "Starting iterator"
echo "Stand by... All s3 command is in quiet mode and may take a while (5 to 60 mins depending on size)"

parallel -u -j $num_jobs run_script_with_args ::: "${arr_key_names[@]}"

echo
echo "............................................"
echo "---- Ended: `date -u`"
overall_list_download_dur="$(calc_Duration $t_overall_list_start)"
echo "Overall processing duration (in percent minutes) : $overall_list_download_dur mins"

# ===========================================
echo "-- scanning for files for warnings and errors"
file_name_date=$(date +"%Y%m%d_%H%M")
error_file_path="$error_folder_name/errors_${file_name_date}.log"
find $log_folder -maxdepth 1 -type f -exec grep -iHn "error" {} +  > $error_file_path &

warning_file_path="$error_folder_name/warnings_${file_name_date}.log"
find $log_folder -maxdepth 1 -type f -exec grep -iHn "warning" {} +  > $warning_file_path &

echo "======================= End of loading model collection folders ========================="
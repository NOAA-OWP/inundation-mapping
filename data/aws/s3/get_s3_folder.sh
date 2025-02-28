#!/bin/bash -e

# Note: This is pretty rough with a lot of hardcoding
# and is used for rtx FIM_30 at this time, but can easily be upgraded later if required.

# Why bash?  Easier than having to load a FIM docker image or create a conda enviro in order
# to use python and other packages.


# Remember to run EXPORT lastest 3 AWS key variables in the same window
# before running this.

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
    echo "This tool downloads just one folder path {-s/-n}

    Sample Usage:  sh get_s3_folder.sh
                -s 's3://(somebucket)/ripple/30_pcnt_domain/collections'
                -n 'mip_03170004'
                -t '/home/rdp-user/output/ripple/fim_30'
                -log 'logs_20250216_0545.txt'
                -c 'ripple_download_stats.csv'

                # NOTE: for now.. Leave off all starting and trailing slashes.

    REQUIRED:
      -s/--s3_source_path      : Full s3 bucket and common prefix.
                                 Parent folder where all child key folders live
                                   ie) s3://(somebucket)/ripple/30_pcnt_domain/collections
      -n/--key_name            : Folder name (key) to be downloaded.
                                   ie) mip_03170004
                                   from s3://(somebucket)/ripple/30_pcnt_domain/collections/mip_03170004
      -t/--trg_path            : Root local folder path for downloads.
                                   ie) ~/fim_30
      -log/--log_file          : log file name and path, suggest adding a date stamp to it.
      -c/--stats_file          : The csv file name and path, where the download data per folder
                                 will be saved.
                                 This saves a header line of:
                                 (folder_name, download_size, num_models, date_downloaded)
                                 if not already there. Can just keep concating from previous 
                                 runs if you like.
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
    -n|--key_name)
        shift
        key_name=$1
        ;;
    -t|--trg_path)
        shift
        trg_path=$1
        ;;
    -log|--log_file)
        shift
        log_file=$1
        ;;
    -c|--stats_file)
        shift
        stats_file=$1
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

# ==========================================================
# VALIDATION
# print usage if arguments empty
if [ "$s3_source_path" = "" ]; then
    echo "ERROR: Missing -s (s3 source path)"
    usage_msg
    exit 22
fi

if [ "$key_name" = "" ]; then
    echo "ERROR: Missing -n (s3 Key name aka: folder name)"
    usage_msg
    exit 22
fi

if [ "$trg_path" = "" ]; then
    echo "ERROR: Missing -t (target path)"
    usage_msg
    exit 22
fi

if [ "$log_file" = "" ]; then
    echo "ERROR: Missing -log (log file name)"
    usage_msg
    exit 22
fi

if [ "$stats_file" = "" ]; then
    echo "ERROR: Missing -c (stats csv file name)"
    usage_msg
    exit 22
fi

echo $log_file

# TODO: Lots more validation such as extensions, valid s3 paths, etc

# ==========================================================
# function to calc duration  (really doesn't need to keep being reloaded and should be a sep file)
Calc_Duration( ) {
    local start_time=$1
    local end_time=`date +%s`

    local total_sec=$(( $end_time - $start_time ))
    local dur_min=$((total_sec / 60))
    local dur_remainder_sec=$((total_sec %60))
    local dur_sec_percent=$((100*$dur_remainder_sec/60))

    echo -e "${dur_min}.${dur_sec_percent}"
}

Formatted_Date( ){
    dt_string=$(date +"%Y%m%d-%H:%M:%S")
    echo "$dt_string"
}

s3_uri="$s3_source_path/$key_name"
trg="$trg_path/$key_name"

# Does folder exist? create it if not
mkdir -p $trg

# Check if the file exists and is empty
# Add header only if empty
stats_header="model_collection_name,source,huc,download_size_in_mib,download_time_in_mins_perc,num_models,date_downloaded"
stats_download_date=$(date +"%Y%m%d_%H%M%S")
if [ ! -e "$stats_file" ]; then
    echo "$stats_header" >> $stats_file
fi

model_name_split=(${key_name//_/ })
collection_source="${model_name_split[0]}"
huc="${model_name_split[1]}"

msg="++++++++++++++++++++"
echo -e $msg ; echo "$msg" >> $log_file
line_lead="$(Formatted_Date): ${key_name} --"
msg="${line_lead} Processing started for $key_name"
echo -e $msg ; echo "$msg" >> $log_file
msg="${line_lead} From $s3_uri to $trg"
echo -e $msg ; echo "$msg" >> $log_file

t_start=`date +%s`

# sleep 20
cli_args="--exclude '*' --include 'library_extent/*' --include 'qc/*' --include '*.xlsx'"
cli_args="${cli_args} --include 'ripple.gpkg' --include 'start_reaches.csv'"
cmd_str="aws s3 cp  ${s3_source_path}/${key_name}/ ${trg_path}/${key_name}/ $cli_args --recursive"
# echo "$cmd_str"
eval "$cmd_str"

dur="$(Calc_Duration $t_start)"
msg="${line_lead} s3 download complete: duration (in percent minutes) = $dur"
echo -e $msg ; echo "$msg" >> $log_file

disk_usage="$(du -shm $trg | cut -f1)"
# msg="$disk_usage"

# f_size=$(printf '%.0f\n' $(echo $disk_usage / 1000 | bc -l))
# f_size=$(printf '%.0f\n' $(echo $disk_usage / 1000 | bc -l))
msg="Folder Size in MiB = $disk_usage"
# echo "$msg"

# find ${trg}/library_extent/ -mindepth 1 -maxdepth 1 -type d | wc -l | xargs echo "Total folders: "
extent_folder="${trg}/library_extent/"
# echo "libray folder is $extent_folder"
if [ -d "$extent_folder" ]; then
    # echo "made it here"
    folder_count="$(find ${extent_folder}/ -mindepth 1 -maxdepth 1 -type d | wc -l)"
else
    # echo "nope.. here"
    folder_count="0"
fi
msg+="; Extent folder count = $folder_count"

# msg="$(Formatted_Date) : ${key_name}: ${msg}; library extent folders = $folder_count"
msg="${line_lead} ${msg}"
echo -e $msg ; echo "$msg" >> $log_file

# Now build up the stats line for the log
# This one is a simple csv
# It does not attempt to look for an entry that may already exist for the key at this time.

# stats_header="model_collection_name,source,huc,download_size_in_mib,download_time_in_mins_perc,num_models,date_downloaded"
dur=$(Calc_Duration $t_start)
stats="$key_name,$collection_source,$huc,$disk_usage,$dur,$folder_count,$stats_download_date"
# echo $stats
echo "$stats" >> $stats_file

msg="${line_lead} Processing complete: Duration (in percent minutes) = $dur mins"
echo -e $msg ; echo "$msg" >> $log_file


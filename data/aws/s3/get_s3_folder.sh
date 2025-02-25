#!/bin/bash -e


# Note: This is pretty rough with alot of hardcoding
# and is used for rtx FIM_30 at this time


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
    echo "Sample Usage:  sh get_s3_folder.sh
                -s 's3://(somebucket)/ripple/30_pcnt_domain/collections'
                -n 'mip_03170004'
                -t '~/fim_30'
                -log 'logs_20250216_0545.txt'

                # NOTE: for now.. Leave off all starting and trailing slashes.

    REQUIRED:
      -s/--s3_source_path      : Full s3 bucket and common prefix.
                                   Parent folder where all child key folders live
                                   ie) s3://(somebucket)/ripple/30_pcnt_domain/collections
      -n/--s3_key_name         : Folder name (key) to be downloaded.
                                   ie) mip_03170004
                                  from s3://(somebucket)/ripple/30_pcnt_domain/collections/mip_03170004
      -t/--trg_path            : Root local folder path for downloads.
                                   ie) ~/fim_30
      -log/--log_file_name     : Just the log file name not path, suggest adding a date stamp to it.
                                   It will be saved in the same root target path.
      -c/--csv_stats_file_name : Just the csv file name not path, where the download data per folder
                                   will be saved.
                                   This saves (folder_name, download_size, num_models, date_downloaded)
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
    -n|--s3_key_name)
        shift
        key_name=$1
        ;;
    -t|--trg_path)
        shift
        trg_path=$1
        ;;
    -log|--log_file_name)
        shift
        log_file_name=$1
        ;;
    -c|--csv_stats_file_name)
        shift
        stats_file_name=$1
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

if [ "$log_file_name" = "" ]; then
    echo "ERROR: Missing -log (log file name)"
    usage_msg
    exit 22
fi

if [ "$stats_file_name" = "" ]; then
    echo "ERROR: Missing -c (stats csv file name)"
    usage_msg
    exit 22
fi

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

    echo $dur_min.$dur_sec_percent
}

Formatted_Date( ){
    dt_string=$(date +"%Y%m%d-%H:%M:%S")
    echo "$dt_string"
}

s3_uri="$s3_source_path/$key_name"
trg="$trg_path/$key_name"
log_file="$trg_path/$log_file_name"
stats_file="$trg_path/$stats_file_name"

# Check if the file exists and is empty
# Add header only if empty
stats_header="folder_name,download_size,num_models,date_downloaded"
if [ -e "$stats_file" ] && [ ! -s "$stats_file" ]; then
    echo "$stats_header" >> ${stats_file}
elif [ ! -e "$FILE" ]; then
    echo "$stats_header" >> ${stats_file}
fi

echo "========================"
msg="$(Formatted_Date) : Downloading from $s3_source_path to $trg_path"
echo $msg ; echo "$msg" >> ${log_file}

msg="----------------- \n"
msg="${msg}model_name: ${key_name} \n"
echo $msg ; echo "$msg" >> ${log_file}

t_start=`date +%s`

# sleep 20
cli_args="--exclude '*' --include 'library_extent/*' --include 'qc/*' --include '*.xlsx'"
cli_args="${cli_args} --include 'ripple.gpkg' --include 'start_reaches.csv'"
cmd_str="aws s3 sync ${s3_source_path}/${key_name} ${trg_path}/ $cli_args"
eval "$cmd_str"
# aws s3 sync "${s3_source_path/${model_name} ${trg}/ --exclude '*' --include 'library_extent/*' --include 'qc/*' --include '*.xlsx' --include 'ripple.gpkg' --include 'start_reaches.csv'
echo "AWS download done"
date

dur=$(Calc_Duration $t_start)
msg="Duration (in percent minutes): $dur min"
echo $msg ; echo "$msg" >> ${log_file}

#du -sh ${trg} | xargs echo "Total size raw: "
#du -sh ${trg} | xargs echo "Total size  --- "
disk_usage="$(du -sh  $trg | cut -f1)"
msg="Total Size: $disk_usage"
echo $msg ; echo "$msg" >> ${log_file}

size_in_kb=`du -b --max-depth=0  $trg | cut -f1`
f_size=$(printf '%.4f\n' $(echo $size_in_kb /100000000 | bc -l))
msg="Folder Size in GiB: $f_size"
echo $msg ; echo "$msg" >> ${log_file}

# find ${trg}/library_extent/ -mindepth 1 -maxdepth 1 -type d | wc -l | xargs echo "Total folders: "
folder_count=$(find ${trg}/library_extent/ -mindepth 1 -maxdepth 1 -type d | wc -l)
msg="Total folders: $folder_count"
echo $msg ; echo "$msg" >> ${log_file}

# Now build up the stats line for the log
# This one is a simple csv, no headers.
download_date=$(date +"%m/%d/%Y")
stats="$model_name,$f_size,$dur,$folder_count,$download_date"
echo $stats
echo "$stats" >> ${stats_file}
echo "Model download complete"
echo

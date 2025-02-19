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


:
usage_msg()
{
    echo "Sample Usage:  sh get_s3_folder.sh -n 'mip_03170004'"
}

set -e

while [ "$1" != "" ]; do
    case $1
    in
    -n|--model_name)
        shift
        model_name=$1
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

# print usage if arguments empty
if [ "$model_name" = "" ]; then
    echo "ERROR: Missing -n collection folder name"
    usage_msg
    exit 22
fi

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

log_file_date=$(date +"%Y_%m_%d")
log_file_path="${TRG_ROOT}/s3_download_log_${log_file_date}.txt"
# echo "$log_file_path"

dt_string=$(date +"%Y-%m-%d %H:%M:%S")
log_msg="======================== \n"
log_msg="${log_msg}${dt_string} \n"

trg="$TRG_ROOT/$model_name"
echo "${log_msg}model_name: ${model_name}"
log_msg="${log_msg}model_name: ${model_name} \n"

t_start=`date +%s`

# sleep 20
cli_args=" --exclude 'library/*' --exclude 'qc/*' --exclude 'submodels/*' --exclude 'source_models/*' "
cli_args="$cli_args --exclude '*-shm' --exclude '*-wal' "

aws s3 sync s3://fimc-data/ripple/30_pcnt_domain/collections/${model_name} ${trg}/ --exclude '*' --include 'library_extent/*' --include 'qc/*' --include '*.xlsx' --include 'ripple.gpkg' --include 'start_reaches.csv'
echo "AWS download done"
date

dur=$(Calc_Duration $t_start)
echo "Duration (in percent minutes): $dur min"
log_msg="${log_msg}${dur} \n"

#du -sh ${trg} | xargs echo "Total size raw: "
#du -sh ${trg} | xargs echo "Total size  --- "
disk_usage="$(du -sh  $trg | cut -f1)"
echo "Total Size: $disk_usage"
log_msg="${log_msg}Total size: $disk_usage \n"

size_in_kb=`du -b --max-depth=0  $trg | cut -f1`
f_size=$(printf '%.4f\n' $(echo $size_in_kb /100000000 | bc -l))
echo "Folder Size in GiB: $f_size"

# find ${trg}/library_extent/ -mindepth 1 -maxdepth 1 -type d | wc -l | xargs echo "Total folders: "
folder_count=$(find ${trg}/library_extent/ -mindepth 1 -maxdepth 1 -type d | wc -l)
echo "Total folders: $folder_count"
log_msg="${log_msg}Total folders: $folder_count"

echo
echo "$log_msg" >> ${log_file_path}

# Now build up the stats line for the log
# This one is a simple csv, no headers.
download_date=$(date +"%m/%d/%Y")
stats="$model_name,$f_size,$dur,$folder_count,$download_date"
echo $stats
csv_file_path="${TRG_ROOT}/s3_download_stats_${log_file_date}.csv"
echo "$stats" >> ${csv_file_path}
echo "Model download complete"
echo

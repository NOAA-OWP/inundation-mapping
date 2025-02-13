#!/bin/bash -e

# Note: This is pretty rough with alot of hardcoding
# and is used for rtx FIM_30 at this time

# Remember to run the lastest 3 AWS key variables in the same window
# before running this.

# As well as the export TRG_ROOT='/our efs drive name/>>>>>>(some path)/20241115'
# at command line prior to running the script

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
    # local dur_hrs=$((total_sec / 3600 ))
    # local dur_min=$((total_sec %3600 / 60))
    local dur_min=$((total_sec / 60))
    local dur_remainder_sec=$((total_sec %60))
    local dur_sec_percent=$((100*$dur_remainder_sec/60))

    echo $dur_min.$dur_sec_percent

}


dt_string=$(date +"%Y-%m-%d %H:%M:%S")
log_msg="======================== \n"
log_msg="${log_msg}${dt_string} \n"

trg="$TRG_ROOT/$model_name"
echo "${log_msg}model_name: ${model_name}"
log_msg="${log_msg}model_name: ${model_name} \n"

t_start=`date +%s`

sleep 20
# aws s3 sync s3://fimc-data/ripple/30_pcnt_domain/collections/${model_name} ${trg}/ ; date
echo "AWS download done"

dur=$(Calc_Duration $t_start)
echo "Duration (in percent minutes): $dur min"
log_msg="${log_msg}${dur} \n"

#du -sh ${trg} | xargs echo "Total size raw: "
#du -sh ${trg} | xargs echo "Total size  --- "
disk_usage="$(du -sh  $trg | cut -f1)"
echo "Total Size: $disk_usage"
log_msg="${log_msg}Total size: $disk_usage \n"

#folder_size_path_removed=$((disk_usage / 10000000))
#folder_size_path_removed=${disk_usage#"$trg"}
# echo "Total size: $folder_size_path_removed"

#disk_usage_in_k=$(du -b ${trg})
#echo "$disk_usage_in_k"
#dec_num=$(printf "'%.2f\n' $disk_usage_in_k / 100000000" | bc)
size=`du -b --max-depth=0  $trg | cut -f1`
echo $size
#printf '%.3f\n' $(echo "35/3600" | bc -l)
# printf '%.2f\n' $(echo $size /100000000 | bc -l)
#framename=$(printf 'frame_%03d' $framenr)
#framename=$(printf 'frame_%03d' $framenr)printf '%.2f\n' $(echo $size /100000000 | bc -l
f_size=$(printf '%.2f\n' $(echo $size /100000000 | bc -l))
echo $f_size
#dec_num=$(echo "scale=2; $disk_usage / 10000000" | bc)
#echo "my dec is $dec_num"

#log_msg="${log_msg}Total size: $disk_usage \n"
# echo "$log_msg"

# find ${trg}/library_extent/ -mindepth 1 -maxdepth 1 -type d | wc -l | xargs echo "Total folders: "
folder_count=$(find ${trg}/library_extent/ -mindepth 1 -maxdepth 1 -type d | wc -l)
echo "Total folders: $folder_count"
log_msg="${log_msg}Total folders: $folder_count"

echo
echo "$log_msg" >> ${TRG_ROOT}/s3_download_log.txt

# Now build up the stats line for the log
#foo=${string#"$prefix"}
#"${Test/Today/$Date}"
#folder_size_path_removed=${disk_usage#""}
stats="$model_name, $f_size, $folder_count, $dur"
echo $stats
echo "$stats" >> ${TRG_ROOT}/s3_download_log.txt
echo "Model download complete"
echo


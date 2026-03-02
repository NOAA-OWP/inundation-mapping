#!/bin/bash -e

# Feb 18, 2026
#    - in July 2025, we received a large dataset from RTX which we loaded into some HV buckets, appx 984 folders
#    - in Oct 2025, we received a second set including a giant "Ohio" folder. It has 211 folders,
#      and other than the new Ohio, the other 210 were replacements for some in the July bucket.
#    - With those 210 updated folders, we can not do a simple sync overwrite as there will be critical residue
#      that could be left behind even with the sync delete feature. A decision was made to manually remove the
#      original 210 folders and replace them with the new incoming Oct set.
#    - We intended on building a py tool that could take care of this easily and reliably, but ran of out time.
#      We will do it all by manual AWS S3 cli commands for this release.

#    - We are making a brand new "merged" s3 bucket folder at: s3://{hv deploy bucket name}/fim/ripple/20260211_merged/
#        - Steps to building it up correctly and safely
#            - Copied the original July ripple folder into the new merged.
#            - Use this tool to search out and remove the folders from the s3 merged folder.
#            - Copy in the Ripple Oct dataset into the new merge folder, skipping only the Ohio folder.

# This tool just focuses on using the provided list of folder names under the "s" argument (s3 path),
# and removes those folders.

# ====================================

:
usage_msg()
{
    echo "See notes in code.

    ### DO NOT RUN IN A DOCKER CONTAINER, just a terminal window
    
    Sample Usage:   (yes.. .bash  and not just ./)
         bash ripple-s3-bucket-remove-updated-folders.sh
            -s 's3://{some bucket or path}/fim/ripple/20260211_merged/collections'
            -f '/efs-drives/{some path}/ripple/ripple_20260211_merged/s3_folder_remove_list.lst''

    REQUIRED:
      -s/--s3-path           : The root s3 path that will have the folders to be removed
      -f/--folder-list-file  : The file name and path that has the list of folders to be removed in s3

    OPTIONS:
      -h/--help               : Print usage statement.

    "
}

# ====================================
# ARGUMENT PARSING

while [ "$1" != "" ]; do
    case $1
    in
    -s|--s3-path)
        shift
        s3_path=$1
        ;;
    -f|--folder-list-file)
        shift
        folder_list_file=$1
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


echo ""
echo "**** Starting to remove the s3 folders based on the folder list provided ****"
echo ""

# ====================================
# VALIDATION

source ./ripple_shared_tools.sh

if [ "$s3_path" = "" ]; then
    echo "ERROR: Missing -s (s3 path)"
    usage_msg
    exit 22
fi

if [ "$folder_list_file" = "" ]; then
    echo "ERROR: Missing -f (File path to the list of S3 folders to be removed)"
    usage_msg
    exit 22
fi

if [ ! -f "$folder_list_file" ]; then
    echo "ERROR: (-f arg) : file does not exist"
    usage_msg
    exit 22
fi

# take off last slash if there is one.
s3_path="${s3_path%/}"

# ====================================

# Get the directory of the script, resolving symlinks
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
FILE_NAME_DATE=$(date +"%Y%m%d_%H%M")
SCRIPT_FILE_NAME_WITH_EXT="$0"
SCRIPT_FILE_NAME="${SCRIPT_FILE_NAME_WITH_EXT:0:-3}"

dir_path=$(dirname "$folder_list_file")
log_folder="${dir_path}/logs"
log_file="${log_folder}/${SCRIPT_FILE_NAME}_$FILE_NAME_DATE.log"
# becomes something like:
# /efs-drives/...../ripple/ripple_20260211_merged/logs/ripple-s3-bucket-remove-updated-folders_20260220_2228.log

mkdir -p $log_folder
Setup_logging $log_file

# ====================================

echo ""
echo "Starting processing" >> $log_file
msg="Using folder names in the $folder_list_file file to remove them from $s3_path"
l_echo "$msg" "$log_file"
echo ""
echo "log file name is $log_file"
echo ""

overall_t_start=`date +%s`

mapfile -t upd_folder_names < $folder_list_file

list_length=${#upd_folder_names[@]}

echo "Total folder names listed: $list_length"

read -p "Press Enter to continue..."

s3_base_cmd="aws s3 rm ${s3_path}"
# s3_cmd_args=" --recursive --dryrun"
s3_cmd_args=" --recursive"

# Loop through each element of the array
for line in "${upd_folder_names[@]}"; do
    echo "Folder being removed in the S3 merged folder: $line"
    s3_cmd="${s3_base_cmd}/${line}/ ${s3_cmd_args}"
    # echo "$s3_cmd"
    l_echo "$s3_cmd" ${log_file}
    eval $s3_cmd
done

echo ""
echo "**** Done removing the s3 folders based on the folder list provided ****"
echo ""
download_date=$(date +"%Y%m%d_%H%M")
total_dur="$(calc_Duration $overall_t_start)"
msg="Processing complete: Duration (in percent minutes) = $total_dur mins"
l_echo "$msg" "$log_file"
echo ""



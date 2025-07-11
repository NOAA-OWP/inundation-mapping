#!/bin/bash -e

# *** MC means Ripple Model Collection (a folder for inside the Ripple root data dirs) ***

# Note: This is pretty rough with a lot of hardcoding
# and is used for rtx FIM_100 at this time, but can easily be upgraded later if required.

# Why bash?  Easier than having to load a FIM docker image or create a conda enviro in order
# to use python and other packages.

# Processing steps:
# 1) Given a specific S3 folder name at the src S3 folder (prefix) path, it will
#    download specific files / folders needed for HV processing. Why filtered?
#    Without filtering, the total size of each folder is expected to be 10x larger
#    As is, with filtering, it is estimated the total FIM 100 set to be 1 TiB.
#    At this time, it does not copy from S3 to our S3. It goes to an local drive, which
#    is strongly encouraged to be an EBS and not EFS drive. R/W to EFS can cost a major
#    amount of costs. EBS does not have r/w costs.
# 2) After a MC is downloaded, some stats are gathered. A download stats file specifically
#    for this MC is created. It is recommended that this file go on the shared EFS drives.
#    Later, a script will gather up those csv's and compile the data. Yes.. the stats files
#    are not going to S3 at this time.
# 3) The temp MC folder will be copied up to the S3 target bucket/folder. It will sync against
#    the target S3 folder to help with timing for aborted copies.
# 4) The local MC files is removed.

# AWS Permissions
# You will need two AWS sets of keys (profiles), one for RTX and one for FIM in order to 
# download and upload in one terminal window. Details of how to set this up and what
# what values can not be provided here. See Rob H in the interium until we get it documented
# somewhere
# As of Jul 2025, we are looking into non expiring aws keys and session
# tokens, or possible upgraded permissions to help go from S3 to S3. If we get to update
# from S3 to S3, we will create new tools to scan S3 and pull download each metrics file.

# NOTE: This solution is not complete. This script loads just one S3 folder at a time.
# I will build a wrapper script that can feed in a whole list and just iterate through these
# The key is trapping errors which has proven to be many and getting stats on it as well.

# This also could be change to python scripts or aws step functions or something to improve
# performance... TBD

# Besides.. this filters out just folders and files we need, dropping the total download
# size by appx 80%.

# YES, YES.... s3 buckets don't really have folders, just prefixes
# but for simplicity here, we will call them folders


# abort on any fails
set -e

:
usage_msg()
{
    echo "This tool downloads just one folder path from the source S3, calcs some stats
    then pushes it back up to a target s3. 

    Sample Usage: 
         ./get_s3_folder.sh
            -src 's3://(somebucket)/ripple/fim_100_domain/collections'
            -n 'mip_03170004'
            -lt '/home/your/output/ripple/fim_30'
            -st 's3://(somebucket)/fim/ripple_100/collections'
            -c '/efs.../ripple/fim_100_prod_data/download_stats'
            -log '/efs.../ripple/fim_100_prod_data/download_logs            
            -sap 'rtx-profile-name'
            -tap 'ti-temp'

            # NOTE: for now.. Leave off all starting and trailing slashes.
 
            Also.. pathing does not like tilde's in them. 
               ie) ~/temp won't work but /home/some-user/temp will

    REQUIRED:
      -src/--s3_source_path    : Source Full s3 bucket and common prefix.
                                 Parent folder where all child key folders live
                                   ie) s3://(somebucket)/ripple/30_pcnt_domain/collections
      -n/--key_name            : Folder name (key) to be downloaded.
                                   ie) mip_03170004
                                   from s3://(somebucket)/ripple/30_pcnt_domain/collections/mip_03170004
      -lt/--temp_trg_path      : Root local folder path for downloads.
                                   ie) /home/some-user/temp_ripple_downloads
      -st/--s3_target_root     : s3://(some bucket)/fim/ripple_100/collections/
      -c/--stats_folder        : For each collection folder (key_name) downloaded, a unique stats file
                                 will be created with meta data about the download.
                                 This saves a header line of:
                                 (folder_name, download_size, num_models, date_downloaded)
                                 The file name pattern will be download_stats_{key_name}.csv
       -log/--log_folder        : Location where the log files for stored                                 
       -sap/--src_aws_profile_name: Name of the cli aws profile name for the src download.
       -tap/--trg_aws_profile_name: Name of the cli aws profile name for the target upload.

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
    -n|--key_name)
        shift
        key_name=$1
        ;;
    -lt|--temp_trg_path)
        shift
        temp_trg_path=$1
        ;;
    -st|--s3_target_root)
        shift
        s3_target_root=$1
        ;;
    -c|--stats_folder)
        shift
        stats_folder=$1
        ;;
    -log|--log_folder)
        shift
        log_folder=$1
        ;;        
    -sap|--src_aws_profile_name)
        shift
        src_aws_profile_name=$1
        ;;
    -tap|--trg_aws_profile_name)
        shift
        trg_aws_profile_name=$1
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
    echo "ERROR: Missing -src (s3 source path)"
    usage_msg
    exit 22
fi

if [ "$key_name" = "" ]; then
    echo "ERROR: Missing -n (s3 Key name aka: folder name (MC (model collection)))"
    usage_msg
    exit 22
fi

if [ "$temp_trg_path" = "" ]; then
    echo "ERROR: Missing -lt (local temp target path)"
    usage_msg
    exit 22
fi

if [ "$s3_target_root" = "" ]; then
    echo "ERROR: Missing -st (S3 target path)"
    usage_msg
    exit 22
fi

if [ "$stats_folder" = "" ]; then
    echo "ERROR: Missing -c (stats folder path name)"
    usage_msg
    exit 22
fi

if [ "$src_aws_profile_name" = "" ]; then
    echo "ERROR: Missing -sap (source aws profile name)"
    usage_msg
    exit 22
fi

if [ "$trg_aws_profile_name" = "" ]; then
    echo "ERROR: Missing -tap (target aws profile name)"
    usage_msg
    exit 22
fi

if [ "$log_folder" = "" ]; then
    echo "ERROR: Missing -log (log file folder path)"
    usage_msg
    exit 22
fi
  
# trim spaces of the end if any
key_name="${key_name%%*([[:space:]])}"    

# TODO: Lots more validation such as extensions, valid s3 paths, etc

source ./ripple_shared_tools.sh

# ==========================================================
# function to calc duration  (really doesn't need to keep being reloaded and should be a sep file)

src_s3_uri="$s3_source_path/$key_name"
local_trg="$temp_trg_path/$key_name"
log_file="${log_folder}/download_log_${key_name}.txt"
stats_file="${stats_folder}/download_stats_${key_name}.csv"
trg_s3_uri="${s3_target_root}/$key_name"

# Does folder exist? create it if not
mkdir -p $local_trg
mkdir -p $log_folder
mkdir -p $stats_folder
Setup_logging $log_file

# test keyname is a valid
# extract first (TODO later)
# if [[ ${#key_name} -ne 12 ]]; then
#     l_echo "*** WARNING: ${key_name} is expected to be 12 chars in length.\n Skipping download"
#     exit 0
# fi

# Start or remove stats file
if [ ! -f $stats_file ]; then
    touch $stats_file
else
    rm -f $stats_file
fi

# extract the huc from the source type
# ie: key name is mip_12090301, now becomes collection_source of mip
# and huc as 12090301
# It is possible to get some invalid key names which we attempt to filter out above
IFS='_' read -r -a fc_name_split <<< $key_name
collection_source="${fc_name_split[0]}"
huc="${fc_name_split[1]}"

l_echo "++++++++++++++++++++" $log_file
msg="${key_name}: Processing started"
l_echo "$msg" "$log_file"
msg="${key_name}: From ${src_s3_uri} to ${local_trg} to ${trg_s3_uri}"
l_echo "$msg" "$log_file"
msg="${key_name}: For HUC $huc, source $collection_source"
l_echo "$msg" "$log_file"
msg="Stats file:  $stats_file"
l_echo "$msg" "$log_file"
echo "Log file written to ${log_file}"

overall_t_start=`date +%s`

# ==============================
# Download from S3
# l_echo "---------------------" "$log_file"
l_echo "${key_name}: Starting S3 download" "$log_file"
echo "Stand by... the s3 command is in quiet mode and may take a while (7 to 30 mins depending on size)"
t_start=`date +%s`
# sleep 20
cli_args="--exclude '*' --include 'library_extent/*' --include 'qc/*' --include '*.xlsx'"
cli_args="${cli_args} --include 'ripple.gpkg' --include 'start_reaches.csv' --quiet"
cmd_str="aws s3 cp --recursive ${src_s3_uri}/ ${local_trg}/ $cli_args --profile $src_aws_profile_name"
# echo "$cmd_str"
eval "$cmd_str"


download_dur="$(calc_Duration $t_start)"
msg="${key_name}: s3 download complete: duration (in percent minutes) = $download_dur"
l_echo "$msg" "$log_file"

# ==============================
# Calc disk usage, number of features (folder count) and make stats csv
l_echo "${key_name}: Starting calc stats" "$log_file"
# t_start=`date +%s`
disk_usage="$(du -shm $local_trg | cut -f1)"
msg="Folder Size in MiB = $disk_usage"
# echo "$msg"

# find ${local_trg}/library_extent/ -mindepth 1 -maxdepth 1 -type d | wc -l | xargs echo "Total folders: "
extent_folder="${local_trg}/library_extent/"
# echo "libray folder is $extent_folder"
if [ -d "$extent_folder" ]; then
    folder_count="$(find ${extent_folder}/ -mindepth 1 -maxdepth 1 -type d | wc -l)"
else
    folder_count="0"
fi
msg+="; Extent folder count = $folder_count"

# msg="${key_name}: ${msg}; library extent folders = $folder_count"
msg="${key_name}: ${msg}"
l_echo "$msg" "$log_file"

# folder_stats_dur="$(calc_Duration $t_start)"
# msg="${key_name}: Calc stats complete: duration (in percent minutes) = $folder_stats_dur"
#folder_stats_dur="$(calc_Duration $t_start)"
msg="${key_name}: Calc stats complete:"
l_echo "$msg" "$log_file"


# ==============================
# Upload to Target S3

# TEMP: hold on trying to reload to our S3 as we have perms issues using the same term
# window to download from RTX, then upload to our S3.  It does not like having one
# terminal window usign two accounts at one time. I am going to use my temp aws creds
l_echo "${key_name}: Starting S3 Upload" "$log_file"
echo "Stand by... the s3 command is in quiet mode and may take a while (7 to 30 mins depending on size)"

t_start=`date +%s`
cmd_str="aws s3 sync ${local_trg}/ ${trg_s3_uri}/ --quiet --profile $trg_aws_profile_name"
# echo "$cmd_str"
eval "$cmd_str"

upload_dur="$(calc_Duration $t_start)"
msg="${key_name}: upload to s3 complete: duration (in percent minutes) = $upload_dur"
l_echo "$msg" "$log_file"


# ==============================
# Now build up the stats line for the log
# This one is a simple csv
stats_header="model_collection_name,source,huc,num_features,download_size_in_mib,"
stats_header+="download_s3_dur_in_mins_perc,upload_s3_dur_in_mins_perc,total_duration,date_downloaded"
echo "$stats_header" >> ${stats_file}

download_date=$(date +"%Y%m%d_%H%M")
total_dur="$(calc_Duration $overall_t_start)"

stats="$key_name,$collection_source,$huc,$folder_count,$disk_usage,"
stats+="$download_dur,$upload_dur,$total_dur,$download_date"
# echo $stats
echo "$stats" >> ${stats_file}


# ==============================
msg="${key_name}: Processing complete: Duration (in percent minutes) = $total_dur mins"
l_echo "$msg" "$log_file"

# ==============================
echo "${key_name}: Removing local temp folders"
rm -rdf $local_trg
echo "+++++++++++++++++++"


#!/bin/bash

#######################################################################################################################
##                                                                                                                   ##
##      Argument validation for slurm_pipeline.sh                                                                    ##
##                                                                                                                   ##
#######################################################################################################################

# Source bash functions
source "$im_path/src/bash_functions.env"
# Gather parameters from /config/params_template.env
source "$im_path"/config/params_template.env

## Get the absolute path of this script
CURRENT_DIR="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"

## If no huc_list argument, fail fast, and print usage statement
if [ "$huc_list" = "" ] && [ $restart == 0 ]; then
    "$CURRENT_DIR/slurm_pipeline.sh" -h
    printf "\n\t ERROR: Missing -u huc_list argument. \n"
    return 2
fi

## If no run_name argument, fail fast, and print usage statement
if [ "$run_name" = "" ]; then
    "$CURRENT_DIR/slurm_pipeline.sh" -h
    printf "\n\t ERROR: Missing -n run_name argument. \n"
    return 2
fi

if [[ "$overwrite" -eq 1 ]] && [[ "$restart" -ne 0 ]];then
    printf "\n\t ERROR: Overwrite and restart flags provided, used one or the other. \n"
    return 2
fi

## Exit if huc_list argument doesn't have correct pathing 
## (here we need to provide a huc_list that is accessible to the docker container)
if [ "${huc_list:0:12}" != "/data/inputs" ] && [ "$restart" -eq 0 ]; then
    printf "\n\t ERROR: Provide a Huc list in format of /data/inputs/huc_lists/<name of huc list> \n"
    return 2
fi

## Error handling for partitions
##  We need to validate that all of the partitions are available on the cluster - 
##      at a minimum, compute1, pre-processing, post-processing        
COMPUTE1_PARTITION_EXISTS=$(sinfo -h -o "%P" | grep -q "compute1" && echo true || echo false)
PRE_PROCESSING_PARTITION_EXISTS=$(sinfo -h -o "%P" | grep -q "pre-processing" && echo true || echo false)
POST_PROCESSING_PARTITION_EXISTS=$(sinfo -h -o "%P" | grep -q "post-processing" && echo true || echo false)

if [[ "$COMPUTE1_PARTITION_EXISTS" == "true" && "$PRE_PROCESSING_PARTITION_EXISTS" == "true" && "$POST_PROCESSING_PARTITION_EXISTS" == "true" ]]; then
    printf "\n All required partitions (compute1, pre-processing, post-processing) are available on this cluster. \n"
else
    printf "\n\t ERROR: Required partitions are not available."
    printf "\n\t ERROR: Please modify the cluster definition to contain three partitions, with the following names: "
    printf "\n\t\t      pre-processing"
    printf "\n\t\t      compute1"    
    printf "\n\t\t      post-processing"
    return 1
fi

## Ensure huc_list is valid and print how many hucs will execute.
## Print how many hucs will execute on each partition (if -p is passed)
## Exit if -p value is not valid
relativeHucList=${huc_list/data/$parent_inputs_dir}
export relativeHucList
huclistDir="/efs/fim-data/hand_fim/inputs/huc_lists/"
if [ "$restart" -eq 0 ];then    
    if [ -f "$relativeHucList" ]; then
        num_hucs=$(wc -l $relativeHucList | awk '{print $1}')
        printf "\n Huc List is valid, amount of hucs in huc list: $num_hucs. \n"
        if [ "$partitions" -ne 0 ]; then
            remainder=$(( num_hucs % partitions ))
            num_hucs_per_partition=$(( num_hucs / partitions ))
            partitions_plus_one=$(( partitions + 1 ))
            if [ $remainder -gt 0 ]; then
                ENOUGH_PARTITIONS_EXISTS=$(sinfo -h -o "%P" | grep -q "compute$partitions_plus_one" && echo true || echo false)
            else
                ENOUGH_PARTITIONS_EXISTS=$(sinfo -h -o "%P" | grep -q "compute$partitions" && echo true || echo false)
            fi
            if [[ "$ENOUGH_PARTITIONS_EXISTS" == "false" ]];then
                printf "\n\t ERROR: Provided a -p value of ' $partitions ', but 'compute$partitions_plus_one' "
                printf "(or compute$partitions if remainder is zero) does not exist."
                printf "\n\t ERROR: Adjust -p value to be one less than the total amount of compute partitions."
                printf "\n\t ERROR: Only if the remainder is zero (remainder is: ' $remainder '), "
                printf "can the partition value be equal to the total amount of compute partitions. \n"
                return 2
            fi
            if [ $remainder -gt 0 ]; then
                for ((i=1; i<=partitions+1; i++)); do
                    if (( i == partitions + 1 )); then
                        printf "\n Will execute remainder: $remainder hucs on compute$i."
                        continue
                    fi
                    printf "\n Will execute $num_hucs_per_partition hucs on compute$i."
                done
            else
                for ((i=1; i<partitions+1; i++)); do
                    printf "\n Will execute $num_hucs_per_partition hucs on compute$i."
                done
            fi
        fi
    else
        printf "\n\t ERROR: Provided Huc List does not exist. \n"
        printf "\n\t ERROR: Please adjust -u argument and try again. \n"
        return 2
    fi
fi

## Error handling for overwrite
outputDestDir="$outputsDir"/"$run_name"
if [ -d "$outputDestDir" ] && [[ "$restart" -eq 0 ]]; then
    if [[ "$overwrite" -ne 1 ]];then
        printf "\n\t ERROR: Output directory exists and no overwrite flag provided."
        printf "\n\t ERROR: If you'd like to overwrite the output directory, please provide -o argument and try again."
        printf "\n\t ERROR: If you'd like to keep the output directory, please use a unique run name (-n) argument. \n"
        return 2
    else
        printf "\n\n Overwrite flag provided, will overwrite data in '$outputDestDir'"
        printf "\n\t & slurm logs in slurm_outputs/$run_name. \n"
        rm -rf slurm_outputs/$run_name
    fi
fi

## Fail fast if restart arg provided, but does not meet criteria for restart capability.
if [ $restart != 0 ]; then
    if [ -d "$outputDestDir" ];then
        files=("$outputDestDir"/unit_errors/*)
        file_count=${#files[@]}
        if [ $file_count -gt 0 ];then
            printf "\n Will issue restart run with the ' $run_name ' outputs directory.\n"
        else
            printf "\n\t ERROR: Restart flag provided, but there are no errored hucs in previous outputs directory: "
            printf " '$outputDestDir/unit_errors' \n"
            return 2
        fi
    else
        printf "\n\t ERROR: Restart flag provided, but there is no ' $outputDestDir ' outputs directory.\n"
        return 2
    fi
fi

# Print path to inundation mapping repo to be used and exit if non-valid
if [ "$im_path" = "$(realpath ../)" ]; then
    printf "\n The inundation-mapping default location (relative to this script) will be used: \n"
    im_path="$(realpath ../)"
    printf "\t im_path:  ${im_path} \n"
else
    if [ ! -f "$im_path/fim_pipeline.sh" ]; then
        printf "\n Path to inundation-mapping repository does not exist or is not valid. \n"
        printf "\n Please adjust -i argument to be an absolute path and try again."
        return 2
    else
        printf "\n The following path to inundation-mapping will be used: "
        printf "\n\t im_path:  ${im_path} \n"
    fi
fi


## Error handling for using jobBranchLimit value thats too high
compute_cpu=$(sinfo -p compute1 -h -o "%c")
if [ $jobBranchLimit -gt $compute_cpu ];then
    printf "\n Job branch limit provided (-jb): ' $jobBranchLimit ' is more than what is available"
    printf " on compute1 partition: ' $compute_cpu '."
    printf "\n Right sizing jobBranchLimit to $compute_cpu. \n"
    jobBranchLimit=$compute_cpu
fi
if [ "$jobBranchLimit" -eq 1 ]; then
    printf "\n\n jobBranchLimit argument not provided, will use the default value of 1. \n"
fi

## Notification for skipcal 
if [ "$skipcal" -eq 1 ]; then
    printf "\n\n skipcal argument provided, will skip calibration steps in post processing. \n"
fi

## Notification for skip post 
if [ "$skippost" -eq 1 ]; then
    printf "\n\n skippost argument provided, skipping post processing. \n"
fi

## Create slurm_outputs log directory if it doesn't exist
if [ ! -d slurm_outputs ]; then
    mkdir slurm_outputs
    chmod 777 slurm_outputs
fi

#######################################################################################################################
# Call the set_huc_list function from src/bash_functions.env if available
if declare -F set_huc_list >/dev/null; then
    set_huc_list
else
    printf "\n\n\t ERROR: set_huc_list is not available in src/bash_functions.env. \n"
    printf "\t ERROR Please use an updated version of inundation-mapping. \n\n"
    exit 1
fi


# Get exit status from set_huc_list in src/bash_functions.env
EXIT_STATUS=$?

# Handle exit status from set_huc_list
if [ "$EXIT_STATUS" -eq 0 ]; then
    printf "\n\t SUCCESS: set_huc_list in src/bash_functions.env completed. \n"
else
    printf "\n\n\t ERROR: set_huc_list in src/bash_functions.env failed with exit code $EXIT_STATUS. \n"
    exit 2
fi

#######################################################################################################################
# Call the enforce_instance_type function from src/bash_functions.env
enforce_instance_type

# Get exit status from enforce_instance_type in src/bash_functions.env
EXIT_STATUS=$?

# Handle exit status from enforce_instance_type
if [ "$EXIT_STATUS" -eq 0 ]; then
    printf "\n\t SUCCESS: enforce_instance_type in src/bash_functions.env completed. \n"
    printf "\n\t Compute node configuration is appropriate for this run. \n"
else
    printf "\n\n\t ERROR: enforce_instance_type in src/bash_functions.env failed with exit code $EXIT_STATUS. \n"
    exit 2
fi
#######################################################################################################################

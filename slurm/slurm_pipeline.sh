#!/bin/bash

#######################################################################################################################
##                                                                                                                   ##
##  Slurm implementation of fim_pipeline.sh                                                                          ##
##                                                                                                                   ##
##  This top level script submits slurm_pre_processing.sh, slurm_process_unit_wb.sh or                               ##
##      slurm_process_unit_wb_restart.sh or slurm_partition_process_unit.sh, & slurm_post_processing.sh as           ##
##      sbatch jobs.                                                                                                 ##
##                                                                                                                   ##
##  Execution example:                                                                                               ##
##      ./slurm_pipeline.sh -u /data/inputs/huc_lists/dev_small_test_4_hucs.lst -n test_slurm_pipeline -o            ##
##                                                                                                                   ##
##  This script relies upon Slurm Job dependencies for execution flow. For reference see:                            ##
##      https://slurm.schedmd.com/sbatch.html#OPT_dependency                                                         ##
##                                                                                                                   ##
#######################################################################################################################

:
usage()
{
    echo "
    slurm_pipeline.sh splits processing up into three steps, sequentially calling the following scripts: 
        1.) 'slurm_pre_processing.sh'
        2.) 'slurm_process_unit_wb.sh' or 'slurm_process_unit_wb_restart.sh' or 'slurm_partition_process_unit.sh'
        3.) 'slurm_post_processing.sh'.
        
    The above are wrappers of : 'fim_pre_processing.sh', 'fim_process_unit_wb.sh' & 'fim_post_processing.sh'.

    Usage : slurm_pipeline.sh -u <huc list> -n <name_of_your_run>

        Partition:
            slurm_pipeline.sh -u <huc list> -n <name_of_your_run> -p 5
        
        Skip post processing:

            slurm_pipeline.sh -u <huc list> -n <name_of_your_run> -s 

    All arguments to this script are passed to 'fim_pre_processing.sh'.
    REQUIRED:
      -u/--huc_list      : HUC8s to run; more than one HUC8 should be passed in quotes (space delimited).
                            A line delimited file, with a .lst extension, is also acceptable. 
                            ** MAKE SURE TO INCLUDE THE .LST AS COMPELTE FILEPATH RELATIVE TO THE DOCKER CONTAINER,
                                i.e. : /data/inputs/huc_lists/dev_small_test_10.lst
      -n/--run_name      : A name to tag the output directories and log files (only alphanumeric).

    OPTIONS:
      -h/--help         : Print usage statement.
      -jb/--jobBranchLimit
                        : Amount of branches to run in parallel.
                        :     Note: Make sure that jb plus 2 (jb + 2) does not exceed the total number of cores available.
      -p/--partitions   : The amount of partitions available. Used to 'chunk' the huc list, into a subset of arrays to
                            submit them into different partitions.
                            Before the -p argument is supplied, we need to do a little bit of math. First, it is necessary
                            to know how many HUCs are in the file you're submitting (wc -l <huc_list>.lst). 
                            Based off of that number, ideally you should provide the --partition that is evenly divisible 
                            (or as close as possible) by the amount of HUCs. If there is a remainder, there will be another
                            chunked huc array containing the remaining hucs. Be advised that there will be an additional 
                            partition that is needed (+1 of whatever argument provided) to run the remainder. 
                            If there is a remainder, provide a value to the --partition argument which is one less than the 
                            available partitions in the cluster. Take the following exmaples:
                              list_of_10.lst has 10 HUCs, if you provide a -p of 2, there will be five hucs in each array, 
                                    submitted to 2 compute partitions. :)
                              list_of_10.lst has 10 HUCs, if you provide a -p of 3, there will be 4 huc arrays submitted to
                                    4 compute partitions. 
                                   3 arrays with 3 HUCS, and one array comprising the remaining HUC, totalling 4 arrays and 4 partitions.
      -o/ --overwrite   : Overwrite outputs if they already exist.
      -sc/--skipcal     : If this param is included, the calibration steps will be skipped.
      -s/--skippost     : If this param is included, the post processing step will be skipped.
      -r/--restart      : Restart. Provide an integer value for the retry attempt. (e.g. -r 1 for first retry)
                            This will delete the failed HUC Directories, and re-issue run_unit_wb.sh on HUCs
                            that had processing errors (identified by a log file in <run_name>/unit_errors/ directory)
      -i/--impath       : Absolute path to inundation-mapping repository. e.g. /efs/projects/inundation-mapping

    Running 'slurm_pipeline.sh' is a quicker process than running all three scripts independently; however,
        you can run each slurm wrapper script independently if desired. 
    "
    exit
}

# print usage if agrument is '-h' or '--help'
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    usage
fi

while [ "$1" != "" ]; do
    case $1 in
        -u|--huc_list)
            shift
            if [ "$1" == "" ] || [ "${1:0:1}" == "-" ]; then
                printf "\n\t ERROR: No value provided for --huc_list flag \n\n" >&2
                exit 1
            fi
            huc_list=$1
            ;;
        -n|--run_name)
            shift
            # Check if there's actually a value provided
            if [ "$1" == "" ] || [ "${1:0:1}" == "-" ]; then
                printf "\n\t ERROR: No value provided for --run_name flag \n\n" >&2
                exit 1
            fi
            run_name=$1
            ;;
        -jb|--jobBranchLimit)
            shift
            # Check if there's actually a value provided
            if [ "$1" == "" ] || [ "${1:0:1}" == "-" ]; then
                printf "\n\t ERROR: No value provided for --jobBranchLimit flag \n\n" >&2
                exit 1
            fi
            jobBranchLimit=$1
            ;;
        -p|--partitions)
            shift
            if [ "$1" == "" ] || [ "${1:0:1}" == "-" ]; then
                printf "\n\t ERROR: No value provided for --partitions flag \n\n" >&2
                exit 1
            fi
            partitions=$1
            ;;
        -i|--impath)
            shift
            if [ "$1" == "" ] || [ "${1:0:1}" == "-" ]; then
                printf "\n\t ERROR: No value provided for --impath flag \n\n" >&2
                exit 1
            fi
            im_path=$1
            ;;
        -h|--help)
            shift
            usage
            ;;
        -r|--restart)
            shift
            if [ "$1" == "" ] || [ "${1:0:1}" == "-" ]; then
                printf "\n\t ERROR: No value provided for --restart flag \n\n" >&2
                exit 1
            fi
            restart=$1
            ;;
        -o|--overwrite)
            overwrite=1
            ;;
        -sc|--skipcal)
            skipcal=1
            ;;
        -s|--skippost)
            skippost=1
            ;;
        *) ;;
    esac
    shift
done

# Set Default values before checking arguments
if [ "$jobBranchLimit" = "" ]; then jobBranchLimit=1; fi
if [ -z "$partitions" ]; then partitions=0; fi
if [ -z "$restart" ]; then restart=0; fi
if [ -z "$overwrite" ]; then overwrite=0; fi
if [ -z "$skipcal" ]; then skipcal=0; fi
if [ -z "$skippost" ]; then skippost=0; fi
if [ "$im_path" = "" ];then im_path="$(realpath ../)"; fi

# Export variables to make available to child scripts. 
export huc_list
export run_name
export jobBranchLimit
export partitions
export im_path
export restart
export overwrite
export skipcal
export skippost

## Set the parent_inputs_dir for string replacement in check_arguments.sh
parent_inputs_dir="efs/fim-data/hand_fim"
## Set the outputsDir for string replacement in check_arguments.sh
outputsDir="/efs/fim-data/hand_fim/outputs"

# Get the absolute path of this script
CURRENT_DIR="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"

#######################################################################################################################
# Execute check_arguments.sh
source "$CURRENT_DIR/check_arguments.sh"

# Get exit status from check_arguments.sh
EXIT_STATUS=$?

# Handle exit status from check_arguments.sh
if [ "$EXIT_STATUS" -eq 0 ]; then
    printf "\n\t SUCCESS: check_arguments.sh completed. \n"
    printf "\n\t >>>> Please be patient with slurm job submissions, as nodes may take up to 10 minutes   <<<< \n"
    printf "\n\t >>>> \t to spin up and begin processing at each step (pre/process/post). \t         <<<< \n\n"
else
    printf "\n\t ERROR: check_arguments.sh failed with exit code $EXIT_STATUS. \n"
    exit 2
fi

#######################################################################################################################
## Split up the processing into 3 seperate logical processing steps (pre, compute, post)
## Each step correlates to a batch job/job array sent to the scheduler to ensure they are processed in the correct order
## We are making use of the sbatch --dependency option to wait for each step to finish before moving on to the next one 

## The SLURM_PRE_PROCESSING job sets up the folder structure and environment variables
## The PROCESS_UNIT_WB_ARRAY job is an array job, which parallelizes the HUC8 level processing
## The post processing job runs the post processing steps (modifies rating curves, etc)
#######################################################################################################################


if [ "$restart" -eq 0 ]; then
    printf "\n Initiating slurm_pre_processing job with a run_name of: $run_name \n\t & huc_list: $huc_list \n"
    SLURM_PRE_PROCESSING=$(sbatch --parsable --partition=pre-processing --output=slurm_outputs/$run_name/%x.out \
                            slurm_pre_processing.sh)
    status=$?
    if [ $status -ne 0 ]; then
        echo "ERROR submitting slurm_pre_processing job, exiting."
        printf "\n\t $SLURM_PRE_PROCESSING"
        exit 1
    fi
else
    printf "\n Initiating restart of slurm_pre_processing job with a run_name of: $run_name \n"
    huc_list="NONE"
    SLURM_PRE_PROCESSING=$(sbatch --parsable --partition=pre-processing --output=slurm_outputs/$run_name/%x_r$restart.out \
                            slurm_pre_processing.sh)
    status=$?
    if [ $status -ne 0 ]; then
        echo "ERROR submitting slurm_pre_processing job, exiting."
        printf "\n\t $SLURM_PRE_PROCESSING"
        exit 1
    fi
fi

printf "\n SLURM_PRE_PROCESSING Submitted, Job ID is: $SLURM_PRE_PROCESSING \n"


#######################################################################################################################
## Parallelization of HUC Processing

## Depending on if the partition argument is provided, issue the appropriate script
if [ "$partitions" -eq 0 ]; then
    ## Call slurm_process_unit_wb.sh, and assign its slurm job id to PROCESS_UNIT_WB_ARRAY
    if [ "$restart" -eq 0 ]; then
        PROCESS_UNIT_WB_ARRAY=$(sbatch --dependency=afterok:$SLURM_PRE_PROCESSING --parsable \
                                    --partition=pre-processing --output=slurm_outputs/$run_name/%x.out \
                                    slurm_process_unit_wb.sh ${relativeHucList})
    else
        PROCESS_UNIT_WB_ARRAY=$(sbatch --dependency=afterok:$SLURM_PRE_PROCESSING --parsable \
                                    --job-name=slurm_process_unit_wb_restart --partition=pre-processing \
                                    --output=slurm_outputs/$run_name/%x_r$restart.out \
                                    slurm_process_unit_wb_restart.sh ${hucList_restart})
    fi
    printf "\n PROCESS_UNIT_WB_ARRAY Submitted, Job ID is: $PROCESS_UNIT_WB_ARRAY \n"
    ## The Slurm Job Id associated with $PROCESS_UNIT_WB_ARRAY is not the Array Job id (slurm_process_unit_wb.sh is a 
    ## wrapper script which submits the array job). Therefore the  Slurm Array Job ID is the following Slurm Job ID.
    SLURM_PROCESS_UNIT_JOB_ARRAY_ID=$(($PROCESS_UNIT_WB_ARRAY + 1))
    printf "\n SLURM_PROCESS_UNIT_JOB_ARRAY_ID is: $SLURM_PROCESS_UNIT_JOB_ARRAY_ID \n"
else
    ## Set the remainder variable. This is needed in order to appropriately set $slurm_process_unit_job_array_ids
    remainder=$(( num_hucs % partitions ))
    ## Call slurm_partition_process_unit.sh, and assign its slurm job id to PROCESS_UNIT_WB_ARRAY
    PROCESS_UNIT_WB_ARRAY=$(sbatch --partition=pre-processing --dependency=afterok:$SLURM_PRE_PROCESSING --parsable \
                                --output=slurm_outputs/$run_name/slurm_partition_process_unit.out \
                                slurm_partition_process_unit.sh)
    printf "\n PROCESS_UNIT_WB_ARRAY Submitted, Job ID is: $PROCESS_UNIT_WB_ARRAY \n"
    ## Depending on if there is a remainder or not, build the string which will be passed as the
    ## 'dependency of array jobs' to post_processing step.
    slurm_process_unit_job_array_ids=($PROCESS_UNIT_WB_ARRAY:)
    if [ $remainder -ne 0 ]; then
        for ((i=0; i<=partitions; i++)); do
            slurm_process_unit_job_array_ids+=$(($PROCESS_UNIT_WB_ARRAY + $i + 1))
            if [ $i -lt $(( $partitions )) ]; then
                slurm_process_unit_job_array_ids+=":"
            fi
        done
    else
        for ((i=0; i<partitions; i++)); do
            slurm_process_unit_job_array_ids+=$(($PROCESS_UNIT_WB_ARRAY + $i + 1))
            if [ $i -lt $(( $partitions - 1 )) ]; then
                slurm_process_unit_job_array_ids+=":"
            fi
        done
    fi
    printf "\n slurm_process_unit_job_array_ids (job dependency string that will be passed to post_processing): "
    printf "\n\t ${slurm_process_unit_job_array_ids} \n"
fi

#######################################################################################################################
# Wait for 30 seconds for the inital jobs (SLURM_PRE_PROCESSING & PROCESS_UNIT_WB_ARRAY) to be submitted

sleep 30

#######################################################################################################################
## Wait for the Slurm array job submission to complete.
## There are no "futures" in slurm. The job id passed to --dependency must precede the current job id.

## PROCESS_UNIT_WB_ARRAY will be submitted, but it will be in the PD state initially, due to its dependency on
## SLURM_PRE_PROCESSING. Therefore, we are not concerned with SLURM_PRE_PROCESSING, but we do need to wait on 
## PROCESS_UNIT_WB_ARRAY.

while true; do
    job_status=$(squeue -j $PROCESS_UNIT_WB_ARRAY -o %t | tail -n 1)
    if [[ $job_status == "PD" ]] || [[ $job_status == "CF" ]]; then
        echo "Job $PROCESS_UNIT_WB_ARRAY is a pending or configuring state : ($job_status)."
        sleep 60 # Wait for 60 seconds before checking again
    elif [[ $job_status == "R" ]] || [[ $job_status == "CD" ]] || [[ $job_status == "CG" ]] || [[ $job_status == "ST" ]]; then
        echo -e "Job $PROCESS_UNIT_WB_ARRAY is either in a running, completed, or stopped state: ($job_status). \n"
        echo -e "squeue output: \n"
        squeue --format="%.10i %.15P %.32j %.15u %.15t %.10M %.6D %R"
        sleep 60 # Wait for 60 seconds for slurm to associate the job array id/ids
        break
    else
        echo "Job $PROCESS_UNIT_WB_ARRAY is in an unaccounted for state: $job_status"
        echo "Please cancel this job by executing: scancel $PROCESS_UNIT_WB_ARRAY"
        echo "See https://slurm.schedmd.com/squeue.html#SECTION_JOB-STATE-CODES "
        exit 1
    fi

done

#######################################################################################################################
## Post-processing 

if [ $skippost -eq 0 ] && [ $partitions -eq 0 ]; then
    ./slurm_post_processing.sh ${SLURM_PROCESS_UNIT_JOB_ARRAY_ID}
    printf "\n slurm_post_processing.sh submitted, this depends on all of the array jobs completing. \n"
elif [ $skippost -eq 0 ] && [ $partitions -ne 0 ]; then 
    ./slurm_post_processing.sh ${slurm_process_unit_job_array_ids}
    printf "\n slurm_post_processing.sh submitted, this depends on all of the array jobs completing. \n"
else
    printf "\n slurm_post_processing.sh skipped, please remember to run the post processing step after "
    printtf "all HUCs have finished processing. \n"
fi

#######################################################################################################################

printf "\n Jobs submitted, see slurm log files in slurm/slurm_outputs/$run_name/*.out for status and errors."
printf "\n\t You many execute 'squeue' to view the job queue. \n\n"
printf "\n If slurm jobs have 'hung', or you wish to cancel certain jobs, issue 'scancel <job id>' to kill jobs. \n\n"

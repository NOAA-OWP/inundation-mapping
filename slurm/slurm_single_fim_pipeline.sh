#!/bin/bash

#######################################################################################################################
##                                                                                                                   ##
##   Use this script as a template for a single HUC submission, a huc list of less than 10 to fim_pipeline.sh,       ##
##      or to use a single compute node (optimized jb and jh values). The default partition is compute1.             ##
##                                                                                                                   ##
##   This job can be configured by modifying the arguments to the fim_pipeline.sh execution,                         ##
##      and/or SBATCH parameters (e.g. partition)                                                                    ##
##          The following flags can be added (see ../fim_pipeline.sh for more information):                          ##
##                      -o  : overwrite                                                                              ##
##               -skippost  : skip post processing                                                                   ##
##                -skipcal  : skip calibration                                                                       ##
##                      -r  : restart                                                                                ##
##                                                                                                                   ##
##   Example execution:                                                                                              ##
##      bash slurm_single_fim_pipeline.sh -n test_slurm_single -u 12090301                                           ##
##                                                                                                                   ##
#######################################################################################################################

:
usage()
{
    echo "
    Usage : slurm_single_fim_pipeline.sh -u <huc8 or huc list> -n <name_of_your_run>

    All arguments to this script are passed to 'fim_pre_processing.sh'.
    REQUIRED:
      -u/--huc_list      : HUC8s to run; more than one HUC8 should be passed in quotes (space delimited).
                            A line delimited file, with a .lst extension, is also acceptable.
                            ** MAKE SURE TO INCLUDE THE .LST AS COMPELTE FILEPATH RELATIVE TO THE DOCKER CONTAINER,
                                i.e. : /data/inputs/huc_lists/dev_small_test_10.lst
      -n/--run_name      : A name to tag the output directories and log files (only alphanumeric).

    OPTIONS:
      -h/--help         : Print usage statement.
      -jh/--jobHucLimit : Max number of concurrent HUC jobs to run. Default 1 job at time.
                        :   Note: Make sure that jh * jb plus 2 (jh * jb + 2) does not exceed the total number
                        :       of cores available.
      -jb/--jobBranchLimit
                        : Max number of concurrent Branch jobs to run. Default 1 job at time.
                        :   Note: Make sure that jb * jh plus 2 (jb * jh + 2) does not exceed the total number
                        :       of cores available.
    "
    exit
}

if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    usage
fi

while [ "$1" != "" ]; do
    case $1 in
        -u|--huc_list)
            shift
            huc_list=$1
            ;;
        -n|--run_name)
            shift
            run_name=$1
            ;;
        -jb|--jobBranchLimit)
            shift
            jobBranchLimit=$1
            ;;
        -jh|--jobHucLimit)
            shift
            jobHucLimit=$1
            ;;
        -i|--impath)
            shift
            im_path=$1
            ;;
        -h|--help)
            shift
            usage
            ;;
        *) ;;
    esac
    shift
done

# Print usage if arguments empty
if [ "$huc_list" = "" ]; then
    echo "ERROR: Missing -u huc list argument"
    usage
    exit 22
fi
if [ "$run_name" = "" ]; then
    echo "ERROR: Missing -n run_name argument"
    usage
    exit 22
fi

if [ "$im_path" = "" ]; then
    echo "Missing -i argument (path to inundation-mapping repository)"
    echo "The default location (relative to this script) will be used: "
    im_path="$(realpath ../)"
    echo -e "\t ${im_path}"
fi

# Default values
if [ "$jobBranchLimit" = "" ]; then jobBranchLimit=1; fi
if [ "$jobHucLimit" = "" ]; then jobHucLimit=1; fi

# Set variables dependant on compute1 cluster
CPU_COUNT=$(sinfo -p compute1 -h -o "%c")
echo "CPU_COUNT : $CPU_COUNT will be passed to #SBATCH cpus-per-task "

# Get the latest container image that is larger than 3Gb. There should only be one container on the current
# node by default, however a user may have built other docker images locally.  
DOCKER_IMAGE=$(docker images --format '{{.Repository}}:{{.Tag}} {{.Size}} {{.CreatedAt}}' | \
  awk '$2 ~ /GB/ && $2+0 > 3 {print $0}' | sort -k3,5 -r | awk '{print $1; exit}')
echo "Using docker image -> $DOCKER_IMAGE"

# Fail fast if over-requesting resources.
if (( jobHucLimit * jobBranchLimit - 2 > CPU_COUNT )); then
    printf "\n ERROR: jobHucLimit ( $jobHucLimit ) * jobBranchLimit ( $jobBranchLimit ) - 2 is "
    printf " more than available processors on this system."
    printf "\n ERROR: Available CPUs on this system are $CPU_COUNT, please modify -jb and -jh values accordingly. \n"
    exit 22
fi

sbatch <<EOF
#!/bin/bash
## The --job-name is transferred to the run_name (output directory)
#SBATCH --job-name=\${run_name}
#SBATCH --output slurm_outputs/${run_name}/%x.out # %x is the job-name
#SBATCH --partition=compute1
#SBATCH --nodes=1
#SBATCH --cpus-per-task $((CPU_COUNT))
#SBATCH --time=20:00:00

## Reassign variables in HEREDOC to be correctly interpreted by docker run command
IM_PATH=${im_path}
RUN_NAME=${run_name}
HUCLIST=${huc_list}
JOBHUCLIMIT=${jobHucLimit}
JOBBRANCHLIMIT=${jobBranchLimit}
DOCKER_IMAGE=${DOCKER_IMAGE}

echo "Slurm job name: \${SLURM_JOB_NAME}"
echo "IM_PATH: \${IM_PATH}"

# Spin up Docker container with correct mounts, and issue fim_pipeline.sh
docker run --rm --name slurm_single_huc_pipeline \
    -v \${IM_PATH}/:/foss_fim \
    -v /efs/fim-data/hand_fim/inputs/:/data/inputs \
    -v /efs/fim-data/hand_fim/outputs/:/outputs \
    -v /fsx/outputs_temp/:/fim_temp \${DOCKER_IMAGE} \
    ./foss_fim/fim_pipeline.sh -u \${HUCLIST} -n \${RUN_NAME} -jh \${JOBHUCLIMIT} -jb \${JOBBRANCHLIMIT}

EOF

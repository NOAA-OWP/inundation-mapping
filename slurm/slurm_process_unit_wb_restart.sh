#!/bin/bash

#######################################################################################################################
##                                                                                                                   ##
##   Slurm wrapper of fim_process_unit_wb.sh                                                                         ##
##   Passed a huc list, this script will parallelize the submission of sbatch jobs to the scheduler.                 ##
##                                                                                                                   ##
##   run_name and im_path parameters are inherited from slurm_pipeline.sh's parent context.                          ##
##                                                                                                                   ##
##   This script is only meant to be executed from slurm_pipeline.sh.                                                ##
##                                                                                                                   ##
##      DO NOT CALL THIS SCRIPT DIRECTLY.                                                                            ##
##                                                                                                                   ##
#######################################################################################################################

# Assign each argument (HUC) to a bash array (huc list)
for arg in "$@"; do
    # Check if the argument is an 8-digit integer
    if [[ $arg =~ ^[0-9]+$ ]]; then
        # Add the argument to the array
        huclist+=("$arg")
    fi
done

num_lines=${#huclist[@]}

# Set variables dependant on compute1 cluster
CPU_COUNT=$(sinfo -p compute1 -h -o "%c")
echo "CPU_COUNT : $CPU_COUNT will be passed to #SBATCH cpus-per-task "

# Get the latest container image that is larger than 3Gb. There should only be one container on the current
# node by default, however a user may have built other docker images locally. 
DOCKER_IMAGE=$(docker images --format '{{.Repository}}:{{.Tag}} {{.Size}} {{.CreatedAt}}' | \
  awk '$2 ~ /GB/ && $2+0 > 3 {print $0}' | sort -k3,5 -r | awk '{print $1; exit}')

# Set NUM_CONCURRENT_ARRAY_JOBS variable based off of Max Nodes in the compute1 partition
# This variable sets the amount of array jobs to run at once based off the amount of nodes on a given partition
NUM_CONCURRENT_ARRAY_JOBS=$(sinfo -p compute1 -h -o "%D")

echo -e "Amount of HUCS to Process : ${num_lines}"
echo -e "Huc list from slurm_process_unit_wb_restart.sh: ${huclist[@]}"
echo -e "CPU_COUNT : $CPU_COUNT will be passed to #SBATCH cpus-per-task "

## Create the Slurm script ($ used in script need to be escaped: \$)
sbatch <<EOF
#!/bin/bash
#SBATCH --job-name=${run_name}
## %x is the job-name, %A is the Job array's master job allocation number, %a is the Slurm Array Task ID (index) number.
#SBATCH --output slurm_outputs/${run_name}/%x_%A_%a.out 
#SBATCH --partition=compute1
#SBATCH --ntasks-per-node 1 # Use for single-node jobs
#SBATCH --cpus-per-task $((CPU_COUNT))
#SBATCH --nodes=1
#SBATCH --time=15:00:00
#SBATCH --array=0-$(( num_lines - 1 ))%$((NUM_CONCURRENT_ARRAY_JOBS))


## Re-load huc list array from arguments above into a bash array (HUCS) to be used in HEREDOC
HUCS=(${huclist[@]})

echo "SLURM_ARRAY_TASK_ID : \$SLURM_ARRAY_TASK_ID"

## Get each individual HUC
HUC=\${HUCS[\$SLURM_ARRAY_TASK_ID]}
export HUC

## Reassign variables in HEREDOC to be correctly interpreted by docker run command
RUN_NAME=${run_name}
IM_PATH=${im_path}
DOCKER_IMAGE=${DOCKER_IMAGE}

echo "Compute node: \${HOSTNAME}"
echo "Running fim_process_unit_wb.sh on \${HUC}"
echo "RUN_NAME is \${RUN_NAME}"
echo "IM_PATH is \${IM_PATH}"
echo "Using docker image -> \${DOCKER_IMAGE}"
echo "Name of docker container is \${HUC}"

docker run --rm --name \${RUN_NAME}_\${HUC} \
-v \${IM_PATH}/:/foss_fim \
-v /efs/fim-data/hand_fim/inputs/:/data/inputs \
-v /efs/fim-data/hand_fim/outputs/:/outputs \
-v /fsx/outputs_temp/:/fim_temp \${DOCKER_IMAGE} \
./foss_fim/fim_process_unit_wb.sh \${RUN_NAME} \${HUC}

EOF

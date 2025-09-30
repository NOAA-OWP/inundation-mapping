#!/bin/bash

#######################################################################################################################
##                                                                                                                   ##
##   Slurm wrapper of fim_process_unit_wb.sh                                                                         ##
##                                                                                                                   ##
##   run_name and im_path parameters are inherited from slurm_pipeline.sh's parent context.                          ##
##                                                                                                                   ##
##   This script is only meant to be executed from slurm_pipeline.sh.                                                ##
##                                                                                                                   ##
##      DO NOT CALL THIS SCRIPT DIRECTLY.                                                                            ##
##                                                                                                                   ##
#######################################################################################################################

# Set arguments to variables
while [ "$1" != "" ]; do
    case $1 in
        -p|--partition)
            shift
            partition=$1
            ;;
        -u|--huc_list)
            shift
            huc_array=("$@")
            ;;
        *) ;;
    esac
    shift
done

# Get the latest container image that is larger than 3Gb. There should only be one container on the current
# node by default, however a user may have built other docker images locally. 
DOCKER_IMAGE=$(docker images --format '{{.Repository}}:{{.Tag}} {{.Size}} {{.CreatedAt}}' | \
  awk '$2 ~ /GB/ && $2+0 > 3 {print $0}' | sort -k3,5 -r | awk '{print $1; exit}')

# Here we're requiring that in the PW Cluster definition, compute partitions are 1 indexed. 
# For example for 3 partitions they would be named: compute1, compute2, compute3
partitionplusone=$((partition + 1))

# Set variables dependant on current compute environment
CPU_COUNT=$(sinfo -p compute$partitionplusone -h -o "%c")

# Set NUM_CONCURRENT_ARRAY_JOBS variable based off of Max Nodes in the compute${partitionplusone} partition
# This variable sets the amount of array jobs to run at once based off the amount of nodes on a given partition
NUM_CONCURRENT_ARRAY_JOBS=$(sinfo -p compute$partitionplusone -h -o "%D")

echo -e "\npartition to be used: compute$partitionplusone"
echo "Using docker image -> $DOCKER_IMAGE"
echo "CPUs on compute$partitionplusone -> $CPU_COUNT"
echo "CPU_COUNT : $CPU_COUNT will be passed to #SBATCH cpus-per-task "
echo "Amount of concurrent jobs (huc level parallelization) to run on each partition is: $NUM_CONCURRENT_ARRAY_JOBS"
echo "huc_array length: ${#huc_array[@]}"
echo "huc_array: ${huc_array[@]}"


## Create the Slurm script ($ used in script need to be escaped: \$)
sbatch <<EOF
#!/bin/bash

#SBATCH --job-name="${run_name}_${partition}"
## %x is the job-name, %a is the Slurm Array Task ID (index) number.
#SBATCH --output slurm_outputs/${run_name}/%x_%a.out 
#SBATCH --partition="compute${partitionplusone}"
#SBATCH --ntasks-per-node 1 # Use for single-node jobs
#SBATCH --cpus-per-task $((CPU_COUNT))
#SBATCH --nodes=1
#SBATCH --time=70:00:00
#SBATCH --array=0-$(( ${#huc_array[@]} - 1 ))%$((NUM_CONCURRENT_ARRAY_JOBS))

## Reassign variables in HEREDOC to be correctly interpreted by docker run command
HUCS=(${huc_array[@]})
RUN_NAME=${run_name}
IM_PATH=${im_path}
DOCKER_IMAGE=${DOCKER_IMAGE}

HUC=\${HUCS[\$SLURM_ARRAY_TASK_ID]}

echo "Array job number: \${SLURM_ARRAY_TASK_ID}"
echo "Running fim_process_unit_wb.sh on: \${HUC}"
echo "RUN_NAME is \${RUN_NAME}"
echo "Using docker image -> \${DOCKER_IMAGE}"
echo "Name of current container is \${HUC}"

docker run --rm --name \${HUC} \
-v \${IM_PATH}/:/foss_fim \
-v /efs/fim-data/hand_fim/inputs/:/data/inputs \
-v /efs/fim-data/hand_fim/outputs/:/outputs \
-v /fsx/outputs_temp/:/fim_temp \${DOCKER_IMAGE} \
./foss_fim/fim_process_unit_wb.sh \${RUN_NAME} \${HUC}

EOF

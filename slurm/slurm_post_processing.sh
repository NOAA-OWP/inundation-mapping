#!/bin/bash

#######################################################################################################################
##                                                                                                                   ##
##   Slurm wrapper of fim_post_processing.sh                                                                         ##
##   Parameters are inherited from slurm_pipeline.sh's parent context.                                               ##
##                                                                                                                   ##
##   This script is only meant to be executed from slurm_pipeline.sh.                                                ##
##                                                                                                                   ##
##      DO NOT CALL THIS SCRIPT DIRECTLY.                                                                            ##
##                                                                                                                   ##
#######################################################################################################################

job_array_id=$1

# Get the latest container image that is larger than 3Gb. There should only be one container on the current
# node by default, however a user may have built other docker images locally. 
DOCKER_IMAGE=$(docker images --format '{{.Repository}}:{{.Tag}} {{.Size}} {{.CreatedAt}}' | \
  awk '$2 ~ /GB/ && $2+0 > 3 {print $0}' | sort -k3,5 -r | awk '{print $1; exit}')

# Implement the use of CPU_COUNT_minus_two instead of job_limit, and remove argument.
CPU_COUNT=$(sinfo -p post-processing -h -o "%c")
CPU_COUNT_minus_two=$((CPU_COUNT - 2))

printf "\n\n Job_array_id passed to slurm_post_processing.sh is $job_array_id \n"

## Create a secondary Slurm script. This is required in order pass the $job_array_id variable, after it is available, 
## once slurm_process_unit_wb.sh has been submitted
sbatch <<EOF
#!/bin/bash
#SBATCH --job-name slurm_post_processing
#SBATCH --output slurm_outputs/${run_name}/slurm_post_processing.out
#SBATCH --dependency=afterany:$job_array_id
#SBATCH --partition=post-processing # This is set in PW Cluster Definition
#SBATCH --nodes=1
#SBATCH --cpus-per-task $((CPU_COUNT_minus_two))
#SBATCH --time=20:00:00

## Allow ability to run docker as non-root user 
sudo chmod 666 /var/run/docker.sock

echo "Waited on slurm job array, argument passed to sbatch --dependency: \${SLURM_JOB_DEPENDENCY}"
echo "CPU_COUNT_minus_two : ${CPU_COUNT_minus_two} will be passed to #SBATCH cpus-per-task, "
echo "and used as -j argument to fim_post_processing.sh. \n"

RUN_NAME=${run_name}
JOB_LIMIT=${CPU_COUNT_minus_two}
IM_PATH=${im_path}
DOCKER_IMAGE=${DOCKER_IMAGE}

echo "slurm_post_processing will be run on \${RUN_NAME} (output directory)"
echo "Compute node: \${HOSTNAME}"

docker run --rm --name fim_post_processing  \
-v \${IM_PATH}/:/foss_fim \
-v /efs/fim-data/hand_fim/inputs/:/data/inputs \
-v /efs/fim-data/hand_fim/outputs/:/outputs \
-v /fsx/outputs_temp/:/fim_temp \${DOCKER_IMAGE} \
./foss_fim/fim_post_processing.sh -n \${RUN_NAME} -j \${JOB_LIMIT}

EOF

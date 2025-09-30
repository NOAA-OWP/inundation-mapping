#!/bin/bash
#SBATCH --job-name=slurm_pre_processing
#SBATCH --nodes=1
#SBATCH --cpus-per-task 1 # Use this for threads/cores in single-node jobs.
#SBATCH --time=00:10:00

#######################################################################################################################
##                                                                                                                   ##   
##   Slurm wrapper of fim_pre_processing.sh                                                                          ##
##   All parameters are inherited from slurm_pipeline.sh's parent context.                                           ##
##                                                                                                                   ##
##   This script is only meant to be executed from slurm_pipeline.sh.                                                ##
##                                                                                                                   ##
##      DO NOT CALL THIS SCRIPT DIRECTLY.                                                                            ##
##                                                                                                                   ##
#######################################################################################################################

echo "Compute node: $(hostname)"
echo "Running fim_pre_processing.sh for ${run_name}"

echo "Args for pre-processing -> "
printf "\n\t im_path: ${im_path} "
printf "\n\t huc_list: ${huc_list}"
printf "\n\t run_name: ${run_name}"
printf "\n\t jobBranchLimit: ${jobBranchLimit}"
printf "\n\t overwrite: ${overwrite}"
printf "\n\t skipcal: ${skipcal}"
printf "\n\t restart: ${restart} \n\n"

## Allow ability to run docker as non-root user 
sudo chmod 666 /var/run/docker.sock

# Lots of conditionals depending on whether restart, overwrite, and/or skipcal are provided
if [[ "$restart" -ne 0 ]];then 
    docker run --rm --name fim_pre_processing_${run_name}_${restart} \
    -v ${im_path}/:/foss_fim \
    -v /efs/fim-data/hand_fim/inputs/:/data/inputs \
    -v /efs/fim-data/hand_fim/outputs/:/outputs \
    -v /fsx/outputs_temp/:/fim_temp fim:latest_hlp \
    ./foss_fim/fim_pre_processing.sh -n "${run_name}" -jb "${jobBranchLimit}" -r "${restart}"
elif [[ "$overwrite" -eq 0 ]] && [[ "$skipcal" -eq 0 ]] ;then
    docker run --rm --name fim_pre_processing_${run_name} \
    -v ${im_path}/:/foss_fim \
    -v /efs/fim-data/hand_fim/inputs/:/data/inputs \
    -v /efs/fim-data/hand_fim/outputs/:/outputs \
    -v /fsx/outputs_temp/:/fim_temp fim:latest_hlp \
    ./foss_fim/fim_pre_processing.sh -u "${huc_list}" -n "${run_name}" -jb "${jobBranchLimit}"
elif [[ "$overwrite" -eq 0 ]] && [[ "$skipcal" -ne 0 ]];then 
    docker run --rm --name fim_pre_processing_${run_name} \
    -v ${im_path}/:/foss_fim \
    -v /efs/fim-data/hand_fim/inputs/:/data/inputs \
    -v /efs/fim-data/hand_fim/outputs/:/outputs \
    -v /fsx/outputs_temp/:/fim_temp fim:latest_hlp \
    ./foss_fim/fim_pre_processing.sh -u "${huc_list}" -n "${run_name}" -jb "${jobBranchLimit}" -skipcal
elif [[ "$overwrite" -ne 0 ]] && [[ "$skipcal" -eq 0 ]];then 
    docker run --rm --name fim_pre_processing_${run_name}_${overwrite} \
    -v ${im_path}/:/foss_fim \
    -v /efs/fim-data/hand_fim/inputs/:/data/inputs \
    -v /efs/fim-data/hand_fim/outputs/:/outputs \
    -v /fsx/outputs_temp/:/fim_temp fim:latest_hlp \
    ./foss_fim/fim_pre_processing.sh -u "${huc_list}" -n "${run_name}" -jb "${jobBranchLimit}" -o
else
    docker run --rm --name fim_pre_processing_${run_name} \
    -v ${im_path}/:/foss_fim \
    -v /efs/fim-data/hand_fim/inputs/:/data/inputs \
    -v /efs/fim-data/hand_fim/outputs/:/outputs \
    -v /fsx/outputs_temp/:/fim_temp fim:latest_hlp \
    ./foss_fim/fim_pre_processing.sh -u "${huc_list}" -n "${run_name}" -jb "${jobBranchLimit}" -o -skipcal
fi

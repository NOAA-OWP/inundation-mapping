#!/bin/bash

#######################################################################################################################
##                                                                                                                   ##
##   Use this script as a template to issue sierra test for a certain run.                                           ##
##                                                                                                                   ##
##   This job is meant to be configured within this script to meet needs of a particular use case                    ##
##      Please modify:                                                                                               ##
##          SBATCH parameters                                                                                        ##
##   Example:                                                                                                        ##
##      bash slurm_sierra_test.sh <RUN_NAME>                                                                         ##
##                                                                                                                   ##
#######################################################################################################################

if [[ $# != 1 ]]; then
   echo -n "ERROR: Please provide a run name for the batch script: "
   echo "${0} <RUN_NAME>"
   exit 22
fi

## SBATCH parameters
RUN_NAME="${1}"
EFS_MOUNT="/efs"
TMP_MOUNT="/fsx"
EFSFIM_DIR="${EFS_MOUNT}/fim-data/hand_fim"
IM_DIR="/contrib/Rob.G.Pita/home/projects/dev"
SLURM_PARTITION="post-processing"
JOBLIMIT=20

sbatch <<EOF
#!/bin/bash
## The --job-name is transferred to the run_name (output directory)
#SBATCH --job-name=${RUN_NAME}
#SBATCH --output slurm_outputs/${RUN_NAME}/%x.out # %x is the job-name
#SBATCH --partition=${SLURM_PARTITION}
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --cpus-per-task 32 # Use this for threads/cores in single-node jobs.
#SBATCH --time=08:00:00

echo "Slurm job name: \${SLURM_JOB_NAME}"

# Spin up Docker container with correct mounts, and issue fim_pipeline.sh
docker run --rm --name ${RUN_NAME} \
-v ${IM_DIR}/:/foss_fim \
-v ${EFSFIM_DIR}/inputs/:/data/inputs \
-v ${EFSFIM_DIR}/outputs/:/outputs \
-v ${TMP_MOUNT}/outputs_temp/:/fim_temp fim:latest_hlp \
python3 foss_fim/tools/rating_curve_comparison.py -fim_dir /outputs/${RUN_NAME} \
-o /outputs/${RUN_NAME}/rating_curve_compare -gages /data/inputs/usgs_gages/usgs_rating_curves.csv \
-catfim /data/inputs/usgs_gages/catfim_flows_cms.csv -flows /data/inputs/rating_curve/nwm_recur_flows \
-j ${JOBLIMIT}

EOF

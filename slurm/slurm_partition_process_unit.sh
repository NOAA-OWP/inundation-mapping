#!/bin/bash

#######################################################################################################################
##                                                                                                                   ##
##  This script will split up the huc list into chunks, each of which is passed to                                   ##
##      ./slurm_process_unit_wb_partitions.sh. Partitions mitigate the concern of compute availability and           ##
##       enables to provision nodes across "availability zones", and spread the huc level processing across them.    ##
##                                                                                                                   ##
##  Parameters are inherited from slurm_pipeline.sh's parent context.                                                ##
##                                                                                                                   ##
##  This script is only meant to be executed from slurm_pipeline.sh.                                                 ##
##                                                                                                                   ##
##      DO NOT CALL THIS SCRIPT DIRECTLY.                                                                            ##
##                                                                                                                   ##
#######################################################################################################################

huc_list=$relativeHucList

## Get all HUCS into one array
readarray -t HUCS < $huc_list

# Calculate the size of chunks
chunkSize=$(( ${#HUCS[@]} / partitions ))
remainder=$(( ${#HUCS[@]} % partitions ))

printf "chunkSize (hucs per chunk) -> ${chunkSize} \n"
printf "remainder -> ${remainder} \n" 

## Create the subsets arrays of hucs based on amount of partitions
for ((i=0; i<partitions; i++)); do
    start=$((i * chunkSize))
    end=$((start + chunkSize))
    eval "chunked_array_of_hucs_$i=('${HUCS[@]:${start}:${chunkSize}}')"    
done

## Handle the remainder
if [ $remainder -gt 0 ]; then
    start=$((chunkSize * partitions))
    eval "chunked_array_of_hucs_$partitions=('${HUCS[@]:${start}:${remainder}}')"
fi

## Depending on the remainder, iterate over all chunked arrays
if [ $remainder -gt 0 ]; then
    for ((i=0; i<=partitions; i++)); do
        eval "./slurm_process_unit_wb_partitions.sh -p \$i -u \${chunked_array_of_hucs_$i[*]}"
    done
else 
    for ((i=0; i<partitions; i++)); do
        eval "./slurm_process_unit_wb_partitions.sh -p \$i -u \${chunked_array_of_hucs_$i[*]}"
    done
fi

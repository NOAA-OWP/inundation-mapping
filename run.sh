#!/bin/bash

# $1 = environment file location
# $2 = log fileName

## RENAME VARIABLES ##x
envFile=$1
logFile=$2

## SOURCE ENV FILE AND FUNCTIONS ##
source $envFile
source $libDir/bash_functions.env

## ECHO ENV AND RUN FILES ##
echo -e "\n" | tee $dataDir/$logFile
echo "Running "$envFile | tee -a $dataDir/$logFile
echo -e "\n" | tee -a $dataDir/$logFile
cat $envFile | tee -a $dataDir/$logFile
echo -e "\n" | tee -a $dataDir/$logFile
cat $libDir/run_by_unit_mr_data_PRE_MASKING.sh | tee -a $dataDir/$logFile
echo -e "\n" | tee -a $dataDir/$logFile

## RUN ##
source $libDir/run_by_unit_hr_data_cross_walking.sh | tee -a $dataDir/$logFile

#!/bin/bash

# $1 = environment file location
# $2 = log fileName

## RENAME VARIABLES ##x
envFile=$1
logFile=$2

## SOURCE ENV FILE AND FUNCTIONS ##
source $envFile
source $libDir/bash_functions.env

logFile=$logDir/$2

## ECHO ENV AND RUN FILES ##
echo -e "\n" | tee $logFile
echo "Running "$envFile | tee -a $logFile
echo -e "\n" | tee -a $logFile
cat $envFile | tee -a $logFile
echo -e "\n" | tee -a $logFile
cat $libDir/run_by_unit.sh | tee -a $logFile
echo -e "\n" | tee -a $logFile

## RUN ##
source $libDir/run_by_unit.sh | tee -a $logFile

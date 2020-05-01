#!/bin/bash

# $1 = environment file location
# $2 = log fileName

## RENAME VARIABLES ##x
envFile=$1
logFile=$2
if [ -z "$3" ]
  then
      user_id=0
  else
      user_id=$3
fi

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

## CHANGE PERMISSIONS OF OUTPUTS FOR DOCKER ##
chown -R $user_id:$group_id $outputDataDir
find $outputDataDir -type d -exec chmod 775 {} +
find $outputDataDir -type f -exec chmod 664 {} +

chown $user_id:$group_id $logFile
chmod 664 $logFile

#!/bin/bash

# $1 = environment file location
# $2 = log fileName

## RENAME VARIABLES ##x
hucFile=$1
envFile=$2
logFile=$3
if [ -z "$4" ]
  then
      user_id=0
  else
      user_id=$4
fi

## SOURCE ENV FILE AND FUNCTIONS ##
source $envFile
source $libDir/bash_functions.env

## Make output and data directories ##
if [ ! -d "$outputDataDir" ]; then mkdir $outputDataDir ;fi
if [ ! -d "$logDir" ]; then mkdir $logDir ;fi

logFile=$logDir/$logFile

## ECHO ENV AND RUN FILES ##
echo -e "\n" | tee $logFile
echo "Running "$envFile | tee -a $logFile
echo -e "\n" | tee -a $logFile
cat $envFile | tee -a $logFile
echo -e "\n" | tee -a $logFile
cat $libDir/run_by_unit.sh | tee -a $logFile
echo -e "\n" | tee -a $logFile

## RUN ##
parallel --verbose -a $hucFile -j $maxJobs --progress --joblog $logFile $libDir/run_by_unit.sh 

## CHANGE PERMISSIONS OF OUTPUTS FOR DOCKER ##
chgrp -R $group_id $outputDataDir
find $outputDataDir -type f -exec chmod 664 {} +

chgrp $group_id $logDir
chmod 775 $logDir

chgrp $group_id $logFile
chmod 664 $logFile

#!/bin/bash -e
:
usage ()
{
    echo 'Parameter Adjustment Tool for FIM 3'
    echo 'Usage : param_adj_wrapper.sh [REQ: -u <hucname> -p <paramfile> -n <runName>  -b <blesitename> -r <branchname> ] [OPT: -h -j <job limit> -o]'
    echo ''
    echo 'REQUIRED:'
    echo '  -u/--hucname    : HUC 4,6,or 8 to run (multiples not implemented) passed in quotes. Line delimited file'
    echo '                     also accepted. HUCs must present in inputs directory.'
    echo '  -l/--paramfolder     : folder containing param adjustment files'
    echo '  -p/--paramfile     : file containing mannings n parameter sets'
    echo '  -n/--runName    : a name to tag the output directories and log files as. could be a version tag.'
    echo '  -b/--blesitename    : a name to tag the output directories and log files as. could be a version tag.'
    echo '  -r/--branchname    : name of the feature branch'
    echo ''
    echo 'OPTIONS:'
    echo '  -h/--help       : help file'
    echo '  -j/--jobLimit   : max number of concurrent jobs to run. Default 1 job at time. 1 outputs'
    echo '                    stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest'
    echo '  -o/--overwrite  : overwrite outputs if already exist'
    exit
}


if [ "$#" -lt 6 ]
then
  usage
fi

while [ "$1" != "" ]; do
case $1
in
    -u|--hucname)
        shift
        hucname="$1"
        ;;
    -l|--paramfolder )
        shift
        paramfolder=$1
        ;;
    -p|--paramfile )
        shift
        paramfile=$1
        ;;
    -n|--runName)
        shift
        runName=$1
        ;;
    -b|--blesitename)
        shift
        blesitename=$1
        ;;
    -r|--branchname)
        shift
        branchname=$1
        ;;
    -j|--jobLimit)
        shift
        jobLimit=$1
        ;;
    -h|--help)
        shift
        usage
        ;;
    -o|--overwrite)
        overwrite=1
        ;;
    *) ;;
    esac
    shift
done

# print usage if arguments empty
if [ "$hucname" = "" ]
then
    usage
fi
if [ "$paramfile" = "" ]
then
    usage
fi
if [ "$runName" = "" ]
then
    usage
fi
if [ "$blesitename" = "" ]
then
    usage
fi
if [ "$branchname" = "" ]
then
    usage
fi

## SOURCE ENV FILE AND FUNCTIONS ##
source $envFile
source $libDir/bash_functions.env

# default values
if [ "$jobLimit" = "" ] ; then
    jobLimit=$defaultMaxJobs
fi

## Define Outputs Data Dir & Log File##
export outputRunDataDir=$paramfolder
logFile=$outputRunDataDir/param_logs/summary.log
mkdir -p $outputRunDataDir/param_logs
## RUN ##
if [ -f "$hucname" ]; then
    if [ "$jobLimit" -eq 1 ]; then
        parallel --verbose --lb  -j $jobLimit --joblog $logFile -- $libDir/time_and_tee_param_adj.sh :::: $hucList
    else
        parallel --eta -j $jobLimit --joblog $logFile -- $libDir/time_and_tee_param_adj.sh :::: $hucList
    fi
else
    if [ "$jobLimit" -eq 1 ]; then
        parallel --verbose --lb -j $jobLimit --joblog $logFile -- $libDir/time_and_tee_param_adj.sh ::: $hucList
    else
        parallel --eta -j $jobLimit --joblog $logFile -- $libDir/time_and_tee_param_adj.sh ::: $hucList
    fi
fi


while read p; do

    # Generate mannings_table.json
    python3 foss_fim/config/create_mannings_table.py -d "$p" -f $paramfolder/mannings_table.json
    # Run FIM
    fim_run.sh -u $hucname -c foss_fim/config/test1.env -n $runName
    # Run Eval
    python3 foss_fim/tests/run_test_case.py -r $runName -t $blesitename -b $branchname -c
    # Calculate Objective Function
    metrics_path=test_cases/$blesitename/performance_archive/development_versions/$branchname/

done <$paramfolder/param_adj_mannings.csv

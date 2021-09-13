#!/bin/bash -e
:
usage ()
{
    echo 'Produce FIM datasets'
    echo 'Usage : fim_run.sh [REQ: -u <hucs> -c <config file> -n <run name> ] [OPT: -h -j <job limit>]'
    echo ''
    echo 'REQUIRED:'
    echo '  -u/--hucList    : HUC 4,6,or 8 to run or multiple passed in quotes. Line delimited file'
    echo '                     also accepted. HUCs must present in inputs directory.'
    echo '  -e/--extent     : full resolution or mainstem method; options are MS or FR'
    echo '  -c/--config     : configuration file with bash environment variables to export'
    echo '  -n/--runName    : a name to tag the output directories and log files as. could be a version tag.'
    echo ''
    echo 'OPTIONS:'
    echo '  -h/--help       : help file'
    echo '  -j/--jobLimit   : max number of concurrent jobs to run. Default 1 job at time. 1 outputs'
    echo '                    stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest'
    echo '  -o/--overwrite  : overwrite outputs if already exist'
    echo '  -p/--production : only save final inundation outputs'
    echo '  -w/--whitelist  : list of files to save in a production run in addition to final inundation outputs'
    echo '                     ex: file1.tif,file2.json,file3.csv'
    echo '  -v/--viz        : compute post-processing on outputs to be used in viz'
    echo '  -m/--mem        : enable memory profiling'
    exit
}

if [ "$#" -lt 7 ]
then
  usage
fi

while [ "$1" != "" ]; do
case $1
in
    -u|--hucList)
        shift
        hucList="$1"
        ;;
    -c|--configFile )
        shift
        envFile=$1
        ;;
    -e|--extent)
        shift
        extent=$1
        ;;
    -n|--runName)
        shift
        runName=$1
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
    -p|--production)
        production=1
        ;;
    -w|--whitelist)
        shift
        whitelist="$1"
        ;;
    -v|--viz)
        viz=1
        ;;
    -m|--mem)
        mem=1
        ;;
    *) ;;
    esac
    shift
done

# print usage if arguments empty
if [ "$hucList" = "" ]
then
    usage
fi
if [ "$extent" = "" ]
then
    usage
fi
if [ "$envFile" = "" ]
then
    usage
fi
if [ "$runName" = "" ]
then
    usage
fi

## SOURCE ENV FILE AND FUNCTIONS ##
source $envFile
source $srcDir/bash_functions.env

# default values
if [ "$jobLimit" = "" ] ; then
    jobLimit=$default_max_jobs
fi

## Define Outputs Data Dir & Log File##
export outputRunDataDir=$outputDataDir/$runName
export extent=$extent
export production=$production
export whitelist=$whitelist
export viz=$viz
export mem=$mem
logFile=$outputRunDataDir/logs/summary.log

## Define inputs
export input_WBD_gdb=$inputDataDir/wbd/WBD_National.gpkg
export input_nwm_lakes=$inputDataDir/nwm_hydrofabric/nwm_lakes.gpkg
export input_nwm_catchments=$inputDataDir/nwm_hydrofabric/nwm_catchments.gpkg
export input_nwm_flows=$inputDataDir/nwm_hydrofabric/nwm_flows.gpkg
export input_nhd_flowlines=$inputDataDir/nhdplus_vectors_aggregate/agg_nhd_streams_adj.gpkg
export input_nhd_headwaters=$inputDataDir/nhdplus_vectors_aggregate/agg_nhd_headwaters_adj.gpkg
export input_GL_boundaries=$inputDataDir/landsea/gl_water_polygons.gpkg
## Input handling ##
$srcDir/check_huc_inputs.py -u "$hucList"

## Make output and data directories ##
if [ -d "$outputRunDataDir" ] && [  "$overwrite" -eq 1 ]; then
    rm -rf "$outputRunDataDir"
elif [ -d "$outputRunDataDir" ] && [ -z "$overwrite" ] ; then
    echo "$runName data directories already exist. Use -o/--overwrite to continue"
    exit 1
fi
mkdir -p $outputRunDataDir/logs

## RUN ##
if [ -f "$hucList" ]; then
    if [ "$jobLimit" -eq 1 ]; then
        parallel --verbose --lb  -j $jobLimit --joblog $logFile -- $srcDir/time_and_tee_run_by_unit.sh :::: $hucList
    else
        parallel --eta -j $jobLimit --joblog $logFile -- $srcDir/time_and_tee_run_by_unit.sh :::: $hucList
    fi
else
    if [ "$jobLimit" -eq 1 ]; then
        parallel --verbose --lb -j $jobLimit --joblog $logFile -- $srcDir/time_and_tee_run_by_unit.sh ::: $hucList
    else
        parallel --eta -j $jobLimit --joblog $logFile -- $srcDir/time_and_tee_run_by_unit.sh ::: $hucList
    fi
fi

# identify missing HUCs
# time python3 /foss_fim/tools/fim_completion_check.py -i $hucList -o $outputRunDataDir

echo "$viz"
if [[ "$viz" -eq 1 ]]; then
    # aggregate outputs
    time python3 /foss_fim/src/aggregate_fim_outputs.py -d $outputRunDataDir -j 6
fi

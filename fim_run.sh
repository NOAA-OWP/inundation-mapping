#!/bin/bash -e
:
usageMessage ()
{
	echo
    echo 'Produce FIM datasets'
    echo 'Usage : fim_run.sh [REQ: -u <hucs> -c <config file> -n <run name> ] [OPT: -h -j <job limit>]'
    echo ''
    echo 'REQUIRED:'
    echo '  -u/--hucList    : HUC 4,6,or 8 to run or multiple passed in quotes. Line delimited file also accepted.'
	echo '                      HUCs must present in inputs directory.'
    echo '  -e/--extent     : Full resolution or mainstem method; options are MS or FR.'
    echo '  -c/--config     : Configuration file with bash environment variables to export.'
    echo '  -n/--runName    : A name to tag the output directories and log files as.'
	echo '                      Can be a version tag. AlphaNumeric and underscore only.'
    echo ''
    echo 'OPTIONS:'
    echo '  -h/--help       : Help file.'
    echo '  -j/--jobLimit   : Max number of concurrent jobs to run. Default one job at time.'
    echo '                      One outputs stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest.'
    echo '  -o/--overwrite  : Overwrite outputs if already exist.'
    echo '  -p/--production : Only save final inundation outputs.'
    echo '  -w/--whitelist  : List of files to save in a production run in addition to final inundation outputs.'
    echo '                       ex: file1.tif,file2.json,file3.csv'
    echo '  -v/--viz        : Compute post-processing on outputs to be used in viz.'
    echo '  -m/--mem        : Enable memory profiling'
    echo '  -ssn            : Step number to start at (defaulted to 1).'
    echo '                        ex: -ssn 2'
    echo '  -sen            : Step number to end after (defaulted to 99).'
    echo '                        ex: -sen 3  (if ssn is 2, this means start at 2 and end after at 3)'
	echo 
	echo '   ***** NOTE: If you use the step start and end numbers, remember that it may '
	echo '               leave orphaned files in output which needs to be cleaned up by hand.'
	echo '               And, if you start at step number, you may need to have some files already in place.'
	echo 
    exit
}

set -e

process_error(){

	echo "An error has occurred. Please recheck your input parameters and try again."
	echo "Error Details: $1 .  On Line: $2"
	echo
	usageMessage

}

trap "process_error $1 $LINENO" ERR

while [ "$1" != "" ]; do
case $1
in
    -u|--hucList)
        shift
        hucList="$1"
		;;
    -c|--configFile)
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
        usageMessage
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
    -ssn)
        shift
		step_start_number="$1"
		;;
	-sen)
        shift
		step_end_number="$1"
		;;
    *) ;;
    esac
    shift
done


# Common used tools
source $srcDir/bash_functions.env

# ---------------------------------------
## Define Outputs Data Dir & Log File and input validations##

# ---------------------------------------
## Check command line arguments for errors and setup variables if required
#source $srcDir/validate_fim_run_args.sh 

input_validation_output=$(python3 $srcDir/validate_frm_run_args.py -u $hucList \
																   -c $envFile \
																   -e $extent \																   
																   -n $runName \
																   -j $jobLimit \
																   -w $whitelist \
																   -ssn $step_start_number \
																   -sen $step_end_number )

if [ "$input_validation_output" != "" ] 
then
	Show_Error "$input_validation_output"
	#usageMessage
fi

## SOURCE ENV FILE AND FUNCTIONS ##
source $envFile


# validate_fim_run_args can change some values which Bash will need.
# We wil change step_start_number and step_end_number as global values

export outputRunDataDir=$outputDataDir/$runName
export extent=$extent
export production=$production
export whitelist=$whitelist
export viz=$viz
export mem=$mem



echo "All is well at this point - b"
exit 0



logFile=$outputRunDataDir/logs/summary.log

## Define inputs
export input_WBD_gdb=$inputDataDir/wbd/WBD_National.gpkg
export input_nwm_lakes=$inputDataDir/nwm_hydrofabric/nwm_lakes.gpkg
export input_nwm_catchments=$inputDataDir/nwm_hydrofabric/nwm_catchments.gpkg
export input_nwm_flows=$inputDataDir/nwm_hydrofabric/nwm_flows.gpkg
export input_nhd_flowlines=$inputDataDir/nhdplus_vectors_aggregate/agg_nhd_streams_adj.gpkg
export input_nhd_headwaters=$inputDataDir/nhdplus_vectors_aggregate/agg_nhd_headwaters_adj.gpkg
export input_GL_boundaries=$inputDataDir/landsea/gl_water_polygons.gpkg



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

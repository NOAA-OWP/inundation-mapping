#!/bin/bash -e
:

# Note: There is an extra param that can come in. It is the -api command which is generally only set if an external tool
# such as the API (FIM Job Scheduler) is calling it. Setting that switch will change output messages.
usageMessage ()
{
	echo
    echo 'Produce FIM datasets'
    echo 'Usage : fim_run.sh [REQ: -u <hucs> -e <extent (MS or FR) -c <config file> -n <run name> ] '
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

	if [ -z is_API ] ; then
		echo "An error has occurred. Please recheck your input parameters and try again."
		echo "Error Details: $1 .  On Line: $2"
		echo
		usageMessage
	else
		echo "An error has occured. Details: $1 .  On Line: $2"
	fi
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
        envFile="$1"
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
    -ssn)
        shift
		step_start_number=$1
		;;
	-sen)
        shift
		step_end_number=$1
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

	-api)
		is_API=1
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
#  October 2021: the python version below, but was temp put on hold in favour of the 
# bash version which can update Bash variables. Python can not update the parent Bash variables
## Check command line arguments for errors and setup variables if required

## SOURCE ENV FILE AND FUNCTIONS ##
source $envFile

# Bash Version
# make all input values as global so validate can use them

#export hucList=$hucList
export envFile=$envFile
export extent=$extent
export runName=$runName
export jobLimit=$jobLimit
export whitelist=$whitelist
export production=$production
export viz=$viz
export mem=$mem
export step_start_number=$step_start_number
export step_end_number=$step_end_number

# we don't want to delete the output directory if they are using steps
if [ "$step_start_number" = "" ] && [ "$step_end_number" = "" ]
then
	has_Step_Numbers=0
else
	has_Step_Numbers=1
fi


# a new variable called hucCodes is created in validate_fim_run which is an array
# of huc codes that can be passed to the time_and_tee_run_by_unit
export hucCodes

# We need to create this variable at this point as the API needs this variable right away,
# so it can log errors, including errors from the validation tools. runName might be error
# at this point or have extra spaces or similar, but we have to look at this closer later.
export outputRunDataDir=$outputDataDir/$runName

logFile=$outputRunDataDir/logs/summary.log

# validation for most input variables is done here. Some cleanup of variables are done as well
# such as trimming, splitting hucs into pure numeric arrays, etc.
source $srcDir/validate_fim_run_args.sh 


#Python version
# Careful not to add more than one space after each \.
# input_validation_output=$(python3 $srcDir/validate_frm_run_args.py -u "$hucList" \
																   # -c "$envFile" \
																   # -e "$extent" \
																   # -n "$runName" \
																   # -j "$jobLimit" \
																   # -w "$whitelist" \
																   # -s "$step_start_number" \
																   # -d "$step_end_number" )

# if [ "$input_validation_output" != "" ] 
# then
	# Show_Error "$input_validation_output"
	# #usageMessage
# fi

# At this point there is no clean way to have python change and export variables back to Bash
# short of prints and bash parsing it. We will leave it for now until we change bash to python

# validate_fim_run_args can change some values which Bash will need.
# We wil change step_start_number and step_end_number as global values.
# This is not perfect as during validation it stripped some strings 
# but was not easy get those values back to Bash. We can fix this
# when we get it all in python


## Define inputs
export input_WBD_gdb=$inputDataDir/wbd/WBD_National.gpkg
export input_nwm_lakes=$inputDataDir/nwm_hydrofabric/nwm_lakes.gpkg
export input_nwm_catchments=$inputDataDir/nwm_hydrofabric/nwm_catchments.gpkg
export input_nwm_flows=$inputDataDir/nwm_hydrofabric/nwm_flows.gpkg
export input_nhd_flowlines=$inputDataDir/nhdplus_vectors_aggregate/agg_nhd_streams_adj.gpkg
export input_nhd_headwaters=$inputDataDir/nhdplus_vectors_aggregate/agg_nhd_headwaters_adj.gpkg
export input_GL_boundaries=$inputDataDir/landsea/gl_water_polygons.gpkg


## Make output and data directories ##
# if using steps, don't delete the directory
if [ "$has_Step_Numbers" -eq 0 ]
then
	if [ -d "$outputRunDataDir" ] && [ "$overwrite" == "1" ] ; then
		rm -rf "$outputRunDataDir"
	fi
elif [ "$overwrite" == "1" ]
then
	echo "Output directory not overwrite as steps numbers are being used"
fi

mkdir -p $outputRunDataDir/logs	


## RUN ##
if [ "$jobLimit" -eq 1 ]; then
	parallel --verbose --lb -j $jobLimit --joblog $logFile -- $srcDir/time_and_tee_run_by_unit.sh ::: "${hucCodes[@]}"
else
	parallel --eta -j $jobLimit --joblog $logFile -- $srcDir/time_and_tee_run_by_unit.sh ::: "${hucCodes[@]}"
fi

# identify missing HUCs
# time python3 /foss_fim/tools/fim_completion_check.py -i $hucList -o $outputRunDataDir

echo "$viz"
if [[ "$viz" -eq 1 ]]; then
    # aggregate outputs
    time python3 /foss_fim/src/aggregate_fim_outputs.py -d $outputRunDataDir -j 6
fi

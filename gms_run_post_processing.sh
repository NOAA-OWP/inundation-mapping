#!/bin/bash -e
:
usage ()
{
    echo 'Produce GMS hydrofabric at levelpath/branch scale. Execute gms_run_unit.sh and gms_run_branch prior to.'
    echo 'Usage : gms_run_post_processing.sh [REQ: -n <run name> ]'
    echo '  	 				             [OPT: -h -c -j <job limit>]'
    echo ''
    echo 'REQUIRED:'
    echo '  -n/--runName    : A name to tag the output directories and log files as. could be a version tag.'
    echo ''
    echo 'OPTIONS:'
    echo '  -h/--help       : help file'
    echo '  -c/--config     : configuration file with bash environment variables to export'    
    echo '                    default (if arg not added) : /foss_fim/config/params_template.env'            
    echo '  -j/--jobLimit   : max number of concurrent jobs to run. Default 1 job at time. 1 outputs'
    echo '                    stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest'
    echo
    exit
}

while [ "$1" != "" ]; do
case $1
in
    -c|--configFile )
        shift
        envFile=$1
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
    *) ;;
    esac
    shift
done

# print usage if arguments empty
if [ "$runName" = "" ]
then
    echo "ERROR: Missing -n run time name argument"
    usage
fi

if [ "$envFile" = "" ]
then
    envFile=/foss_fim/config/params_template.env
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
export extent=GMS

## Check for run data directory ##
if [ ! -d "$outputRunDataDir" ]; then 
    echo "Depends on output from gms_run_unit.sh. Please produce data with gms_run_unit.sh first."
    exit 1
fi

## Check to ensure gms_run_branch completed ##
if [ ! -f "$outputRunDataDir/branch_errors/non_zero_exit_codes.log" ]; then
    echo "Depends on output from gms_run_branch.sh. Please run gms_run_branch.sh or check if it failed."
    exit 1
fi

# Clean out the other post processing files before starting
rm -rdf $outputRunDataDir/logs/src_optimization
rm -f $outputRunDataDir/logs/log_bankfull_indentify.log
rm -f $outputRunDataDir/logs/subdiv_src_.log

gms_inputs=$outputRunDataDir/gms_inputs.csv

echo
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "---- Start of gms_run_post_processing"
echo "---- Started: `date -u`" 
T_total_start
post_proc_start_time=`date +%s`

## RUN AGGREGATE BRANCH ELEV TABLES ##
# TODO: How do we skip aggregation if there is a branch error
# maybe against the non_zero logs above
echo 
echo "Processing usgs gage aggregation"
python3 $srcDir/usgs_gage_aggregate.py -fim $outputRunDataDir -gms $gms_inputs

## RUN SYNTHETIC RATING CURVE BANKFULL ESTIMATION ROUTINE ##
if [ "$src_bankfull_toggle" = "True" ]; then
    echo -e $startDiv"Estimating bankfull stage in SRCs"$stopDiv
    # Run SRC bankfull estimation routine routine
    Tstart
    time python3 /foss_fim/src/identify_src_bankfull.py -fim_dir $outputRunDataDir -flows $bankfull_flows_file -j $jobLimit
    Tcount
fi

## RUN SYNTHETIC RATING SUBDIVISION ROUTINE ##
if [ "$src_subdiv_toggle" = "True" ]; then
    echo -e $startDiv"Performing SRC channel/overbank subdivision routine"$stopDiv
    # Run SRC Subdivision & Variable Roughness routine
    Tstart
    time python3 /foss_fim/src/subdiv_chan_obank_src.py -fim_dir $outputRunDataDir -mann $vmann_input_file -j $jobLimit
    Tcount
fi

## CONNECT TO CALIBRATION POSTGRESQL DATABASE (OPTIONAL) ##
if [ "$src_adjust_spatial" = "True" ]; then
    if [ ! -f $CALB_DB_KEYS_FILE ]; then
        echo "ERROR! - the src_adjust_spatial parameter in the params_template.env (or equiv) is set to "True" (see parameter file), but the provided calibration database access keys file does not exist: $CALB_DB_KEYS_FILE"
        exit 1
    else
        source $CALB_DB_KEYS_FILE
        : '
        This makes the local variables from the calb_db_keys files
        into global variables that can be used in other files, including python.

        Why not just leave the word export in front of each of the keys in the
        calb_db_keys.env? Becuase that file is used against docker-compose
        when we start up that part of the sytem and it does not like the word
        export.
        '
        export CALIBRATION_DB_HOST=$CALIBRATION_DB_HOST
        export CALIBRATION_DB_NAME=$CALIBRATION_DB_NAME
        export CALIBRATION_DB_USER_NAME=$CALIBRATION_DB_USER_NAME
        export CALIBRATION_DB_PASS=$CALIBRATION_DB_PASS
        echo "Populate PostgrSQL database with benchmark FIM extent points and HUC attributes (the calibration database)"
        echo "Loading HUC Data"
        time ogr2ogr -overwrite -nln hucs -t_srs EPSG:5070 -f PostgreSQL PG:"host=$CALIBRATION_DB_HOST dbname=$CALIBRATION_DB_NAME user=$CALIBRATION_DB_USER_NAME password=$CALIBRATION_DB_PASS" $inputDataDir/wbd/WBD_National.gpkg WBDHU8
        echo "Loading Point Data"
        time ogr2ogr -overwrite -t_srs EPSG:5070 -f PostgreSQL PG:"host=$CALIBRATION_DB_HOST dbname=$CALIBRATION_DB_NAME user=$CALIBRATION_DB_USER_NAME password=$CALIBRATION_DB_PASS" $fim_obs_pnt_data usgs_nws_benchmark_points -nln points
    fi
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ USGS GAGE RATING CURVES ##
if [ "$src_adjust_usgs" = "True" ] && [ "$src_subdiv_toggle" = "True" ]; then
    Tstart
    echo -e $startDiv"Performing SRC adjustments using USGS rating curve database"$stopDiv
    # Run SRC Optimization routine using USGS rating curve data (WSE and flow @ NWM recur flow thresholds)
    python3 $srcDir/src_adjust_usgs_rating.py -run_dir $outputRunDataDir -usgs_rc $inputDataDir/usgs_gages/usgs_rating_curves.csv -nwm_recur $nwm_recur_file -j $jobLimit
    Tcount
    date -u
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ BENCHMARK POINT DATABASE (POSTGRESQL) ##
if [ "$src_adjust_spatial" = "True" ] && [ "$src_subdiv_toggle" = "True" ]; then
    Tstart
    echo -e $startDiv"Performing SRC adjustments using benchmark point database"$stopDiv
    python3 $srcDir/src_adjust_spatial_obs.py -fim_dir $outputRunDataDir -j $jobLimit
    Tcount
    date -u
fi

echo
echo -e $startDiv"Combining crosswalk tables"$stopDiv
# aggregate outputs
Tstart
python3 /foss_fim/tools/gms_tools/combine_crosswalk_tables.py -d $outputRunDataDir -o $outputRunDataDir/crosswalk_table.csv
Tcount
date -u

echo
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "---- gms_run_post_processing complete"
echo "---- Ended: `date -u`"
Calc_Duration $post_proc_start_time
echo

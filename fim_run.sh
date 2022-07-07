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
if [ $SENSITIVE_ENV_PATH = "" ]
then
    echo 'WARNING! - .env file with sensitive paths not provided'
else
    source $SENSITIVE_ENV_PATH
fi

# default values
if [ "$jobLimit" = "" ] ; then
    jobLimit=$default_max_jobs
fi
if [ "$viz" = "" ] ; then
    viz=0
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
if [ "$bathy_src_toggle" = "True" ]; then
    # Run BARC routine
    echo -e $startDiv"Performing Bathy Adjusted Rating Curve routine"$stopDiv
    time python3 /foss_fim/src/bathy_src_adjust_topwidth.py -fim_dir $outputRunDataDir -bfull_geom $bankfull_input_table -j $jobLimit -plots $src_plot_option
else
    echo -e $startDiv"SKIPPING Bathy Adjusted Rating Curve routine"$stopDiv
fi

echo -e $startDiv"Estimating bankfull stage in SRCs"$stopDiv
if [ "$src_bankfull_toggle" = "True" ]; then
    # Run SRC bankfull estimation routine routine
    time python3 /foss_fim/src/identify_src_bankfull.py -fim_dir $outputRunDataDir -flows $bankfull_flows_file -j $jobLimit -plots $src_bankfull_plot_option
fi

echo -e $startDiv"Applying variable roughness in SRCs"$stopDiv
if [ "$src_vrough_toggle" = "True" ]; then
    # Run SRC Variable Roughness routine
    [[ ! -z "$whitelist" ]] && args+=( "-w$whitelist" )
    (( production == 1 )) && args+=( '-p' )
    (( viz == 1 )) && args+=( '-v' )
    time python3 /foss_fim/src/vary_mannings_n_composite.py -fim_dir $outputRunDataDir -mann $vmann_input_file -bc $bankfull_attribute -suff $vrough_suffix -j $jobLimit -plots $src_vrough_plot_option "${args[@]}"
fi

echo -e $startDiv"Performing SRC adjustments using USGS rating curve database"$stopDiv
if [ "$src_adjust_usgs" = "True" ]; then
    # Run SRC Optimization routine using USGS rating curve data (WSE and flow @ NWM recur flow thresholds)
    time python3 foss_fim/src/src_adjust_usgs_rating.py -fim_dir $outputRunDataDir -usgs_rc $inputDataDir/usgs_gages/usgs_rating_curves.csv -nwm_recur $nwm_recur_file -j $jobLimit
fi

echo "Loading HUC Data"
time ogr2ogr -overwrite -nln hucs -a_srs ESRI:102039 -f PostgreSQL PG:"host=$CALIBRATION_DB_HOST dbname=calibration user=$CALIBRATION_DB_USER_NAME password=$CALIBRATION_DB_PASS" $input_WBD_gdb WBDHU8

echo "Loading Point Data"
time ogr2ogr -overwrite -f PostgreSQL PG:"host=$CALIBRATION_DB_HOST dbname=calibration user=$CALIBRATION_DB_USER_NAME password=$CALIBRATION_DB_PASS" /data/inputs/rating_curve/water_edge_database/usgs_nws_benchmark_points_cleaned.gpkg usgs_nws_benchmark_points -nln points

echo -e $startDiv"Performing SRC adjustments using obs FIM/flow point database"$stopDiv
if [ "$src_adjust_spatial" = "True" ]; then
    # Run SRC Optimization routine using USGS rating curve data (WSE and flow @ NWM recur flow thresholds)
    time python3 foss_fim/src/src_adjust_spatial_obs.py -fim_dir $outputRunDataDir -j 1
fi

echo -e $startDiv"$viz"
if [[ "$viz" -eq 1 ]]; then
    # aggregate outputs
    time python3 /foss_fim/src/aggregate_fim_outputs.py -d $outputRunDataDir -j 6
fi

#!/bin/bash -e

:
usage ()
{
    echo 'Post processing for creating FIM hydrofabric.'
    echo 'Usage : fim_post_processing.sh [REQ: -n <run name> ]'
    echo '  	 				         [OPT: -h -j <job limit>]'
    echo ''
    echo 'REQUIRED:'
    echo '  -n/--runName    : A name to tag the output directories and log files.'
    echo ''
    echo 'OPTIONS:'
    echo '  -h/--help       : help file'
    echo '  -j/--jobLimit   : max number of concurrent jobs to run. Default 1 job at time. 1 outputs'
    echo '                    stdout and stderr to terminal and logs. With >1 outputs progress and logs the rest'
    echo '                    Note: Not the same variable name as fim_pipeline or fim_pre_processing'
    echo '                    and can be the multiplication of jobHucLimit and jobBranchLimit'
    echo
    exit
}

while [ "$1" != "" ]; do
case $1
in
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

outputDestDir=$outputsDir/$runName

## Check for output destination directory ##
if [ ! -d "$outputDestDir" ]; then 
    echo "Depends on output from units and branches. Please provide an output folder name that has hucs/branches run."
    exit 1
fi

#########################################################################################
#                                                                                       #
# PLEASE DO NOT USE the job limits coming in from the runtime_args.env                  #
# Most of the time, post processing will not be run on the same servers                 #
# that is running fim_process_unit_wb.sh and the processing power                       #
# used to run fim_post_processing.sh will be different (hence.. different job limit)    #
#                                                                                       #
#########################################################################################

if [ "$jobLimit" = "" ]; then jobLimit=1; fi

# Clean out the other post processing files before starting
rm -rdf $outputDestDir/logs/src_optimization
rm -f $outputDestDir/logs/log_bankfull_indentify.log
rm -f $outputDestDir/logs/subdiv_src_.log

# load up enviromental information
args_file=$outputDestDir/runtime_args.env
fim_inputs=$outputDestDir/fim_inputs.csv

source $args_file
source $outputDestDir/params.env
source $srcDir/bash_functions.env
source $srcDir/bash_variables.env

echo
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "---- Start of fim_post_processing"
echo "---- Started: `date -u`" 
T_total_start
post_proc_start_time=`date +%s`


## AGGREGATE BRANCH LISTS INTO ONE ##
echo -e $startDiv"Start branch aggregation"
python3 $srcDir/aggregate_branch_lists.py -d $outputDestDir -f "branch_ids.csv" -o $fim_inputs

## GET NON ZERO EXIT CODES FOR BRANCHES ##
echo -e $startDiv"Start non-zero exit code checking"
find $outputDestDir/logs/branch -name "*_branch_*.log" -type f | xargs grep -E "Exit status: ([1-9][0-9]{0,2})" > "$outputDestDir/branch_errors/non_zero_exit_codes.log" &

## RUN AGGREGATE BRANCH ELEV TABLES ##
echo "Processing usgs gage aggregation"   
python3 $srcDir/aggregate_by_huc.py -fim $outputDestDir -i $fim_inputs -elev -j $jobLimit

## RUN BATHYMETRY ADJUSTMENT ROUTINE ##
if [ "$bathymetry_adjust" = "True" ]; then
    echo -e $startDiv"Performing Bathymetry Adjustment routine"
    # Run bathymetry adjustment routine
    Tstart
    python3 $srcDir/bathymetric_adjustment.py -fim_dir $outputDestDir -bathy $inputsDir/bathymetry/bathymetry_adjustment_data.gpkg -buffer $wbd_buffer -wbd $inputsDir/wbd/WBD_National_EPSG_5070_WBDHU8_clip_dem_domain.gpkg -j $jobLimit
    Tcount
fi

## RUN SYNTHETIC RATING CURVE BANKFULL ESTIMATION ROUTINE ##
if [ "$src_bankfull_toggle" = "True" ]; then
    echo -e $startDiv"Estimating bankfull stage in SRCs"
    # Run SRC bankfull estimation routine routine
    Tstart
    python3 $srcDir/identify_src_bankfull.py -fim_dir $outputDestDir -flows $bankfull_flows_file -j $jobLimit
    Tcount
fi

## RUN SYNTHETIC RATING SUBDIVISION ROUTINE ##
if [ "$src_subdiv_toggle" = "True" ]; then
    echo -e $startDiv"Performing SRC channel/overbank subdivision routine"
    # Run SRC Subdivision & Variable Roughness routine
    Tstart
    python3 $srcDir/subdiv_chan_obank_src.py -fim_dir $outputDestDir -mann $vmann_input_file -j $jobLimit
    Tcount
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ USGS GAGE RATING CURVES ##
if [ "$src_adjust_usgs" = "True" ] && [ "$src_subdiv_toggle" = "True" ] && [ "$skipcal" = "0" ]; then
    Tstart
    echo    
    echo -e $startDiv"Performing SRC adjustments using USGS rating curve database"
    # Run SRC Optimization routine using USGS rating curve data (WSE and flow @ NWM recur flow thresholds)
    python3 $srcDir/src_adjust_usgs_rating.py -run_dir $outputDestDir -usgs_rc $inputsDir/usgs_gages/usgs_rating_curves.csv -nwm_recur $nwm_recur_file -j $jobLimit
    Tcount
    date -u
fi

## RUN SYNTHETIC RATING CURVE CALIBRATION W/ BENCHMARK POINTS (.parquet files) ##
if [ "$src_adjust_spatial" = "True" ] && [ "$src_subdiv_toggle" = "True" ]  && [ "$skipcal" = "0" ]; then
    Tstart
    echo
    echo -e $startDiv"Performing SRC adjustments using benchmark point .parquet files"
    python3 $srcDir/src_adjust_spatial_obs.py -fim_dir $outputDestDir -j $jobLimit
    Tcount
    date -u
fi

## AGGREGATE BRANCH TABLES ##
echo 
echo -e $startDiv"Aggregating branch hydrotables"
Tstart
python3 $srcDir/aggregate_by_huc.py -fim $outputDestDir -i $fim_inputs -htable -j $jobLimit
Tcount
date -u

echo
echo -e $startDiv"Combining crosswalk tables"
# aggregate outputs
Tstart
python3 /foss_fim/tools/combine_crosswalk_tables.py -d $outputDestDir -o $outputDestDir/crosswalk_table.csv
Tcount
date -u

echo
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo "---- End of fim_post_processing"
echo "---- Ended: `date -u`"
Calc_Duration $post_proc_start_time
echo

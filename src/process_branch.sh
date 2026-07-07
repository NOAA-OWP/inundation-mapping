#!/bin/bash
### We DO NOT want -e (exit on fail)
### All output and errors going to screen and will be caught and rolled up via the "tee"
### command in fim_process_huc.sh (through run_huc.sh -> fim_process_huc.sh)
### Yes.. not all of our .sh files are the same with the -e flag, be design.

# It is strongly recommended that you do not call src/run_by_branch.sh directly.
# Call this file instead, and let it call run_by_branch.sh as it will trap all 
# and any exceptions from run_by_branch.sh.
# This is a key part to handling .sh exceptions.

# This script does not need its own trap if there are errors on this page
# because, as mentioned, it auto goes to screen, then log rollups and usign l_echo has no value.

runName=$1
hucNumber=$2
branchId=$3

source $srcDir/bash_functions.env


#########################
# We really do not use the branch log file as it is all auto rolled up in the huc level log.
# We leave it there to make it easier to quicky jump to get details, even though it is fully
# duplicated in the huc level log.
branch_log_file_name=$tempHucDataDir/logs/branch/"$hucNumber"_branch_"$branchId".log

# Note: By design, there no scanning tools using the branch logs.
# And we do not want any error scaning, just echos so it can be picked up by fim_process_hucs.sh "tee" command
#########################

echo "++++++++++++++++++++++++++++++++++++"
echo -e $startDiv"Processing HUC: $hucNumber - branch_id: $branchId"

branch_start_time=`date +%s`
date -u

/usr/bin/time -v $srcDir/run_by_branch.sh $hucNumber $branchId 2>&1 | tee -a $branch_log_file_name
# /usr/bin/time -f "$time_cmd_format" $srcDir/run_by_branch.sh $hucNumber $branchId 2>&1 | tee $branch_log_file_name

# See note in fim_process_huc.sh talking about PIPESTATUS info
return_codes=( "${PIPESTATUS[@]}" )

# By simply using echo, it goes to screen which is then rolled up to run_huc.sh
# and fim_process_huc.sh which catches it via its "tee" command
# Most of these can end up in the huc log twice and that is ok.
for code in "${return_codes[@]}"
do
    # Note: It was tricky to load in the fim_enum into bash, so we will just
    # go with the exit code for now
    if [ $code -eq 0 ]; then
        echo
        # do nothing
    elif [ $code -eq 61 ]; then
        echo
        echo "***** ERROR (well.. warning): Branch has no valid flowlines *****"
        # rm -rf $tempHucDataDir/branches/$branchId/  # keep for debugging
    elif [ $code -eq 64 ]; then
        echo
        echo "***** ERROR (well.. warning): Branch has no crosswalks *****"
        # rm -rf $tempHucDataDir/branches/$branchId/  # keep for debugging
    elif [ $code -eq 65 ]; then
        echo
        err_exists=1
        echo "***** ERROR (well.. warning): Too many HydroIDs or a HydroID with more" \
        " than 8 digits in gw catchments to convert to Int16 *****"
        # rm -rf $tempHucDataDir/branches/$branchId/   # keep for debugging
    elif [ $code -ne 0 ]; then
        echo
        err_exists=1
        echo "***** ERROR - Unknown Exit status of $code detected for branch $branchId *****"
    fi
done

echo -e $startDiv"End Branch Processing $hucNumber $branchId ..."
date -u
Calc_Duration "Duration : " $branch_start_time ""
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"

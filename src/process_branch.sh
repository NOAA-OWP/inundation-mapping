#!/bin/bash -e

# It is strongly recommended that you do not call src/run_by_branch.sh directly.
# Call this file instead, and let it call run_by_branch.sh.
# This file will trap any exceptions from run_by_branch.sh.
# This is a key part to handling .sh exceptions.

# Also.. remember.. that this file can be called explicitly, but will rarely need to be,
# as it is usually called through a parallelizing iterator in run_unit_wb.sh

# This file also has no named command line arguments, only positional args.

runName=$1

source $srcDir/bash_functions.env

# tempHucDataDir come from fim_process_unit_wb.sh
branchLogFileName=$tempHucDataDir/logs/branch/"$hucNumber"_branch_"$branchId".log
# Tell the system the name and location of the log file
# l_echo is echo to screen and log at the same time.
Set_log_file_path $branchLogFileName

/usr/bin/time -v $srcDir/run_by_branch.sh $hucNumber $branchId 2>&1 | tee $branchLogFileName

# See note in fim_process_huc.sh talking about PIPESTATUS info
return_codes=( "${PIPESTATUS[@]}" )

# we do this way instead of working directly with stderr and stdout
# as they were messing with output logs which we always want.
err_exists=0
for code in "${return_codes[@]}"
do
    # Make an extra copy of the branch log in a new folder
    # Note: It was tricky to load in the fim_enum into bash, so we will just
    # go with the code for now
    if [ $code -eq 0 ]; then
        echo
        # do nothing
    elif [ $code -eq 61 ]; then
        echo
        echo "***** Branch has no valid flowlines *****"

        # Later, we will change this to the "debug" system down the road to keep or rm this
        # folder based on the debug flag being true or false.
        # rm -rf $tempHucDataDir/branches/$branchId/

    elif [ $code -eq 64 ]; then
        echo
        echo "***** Branch has no crosswalks *****"

        # Later, we will change this to the "debug" system down the road to keep or rm this
        # folder based on the debug flag being true or false.
        # rm -rf $tempHucDataDir/branches/$branchId/

    elif [ $code -eq 65 ]; then
        echo
        echo "***** Too many HydroIDs or a HydroID with more than 8 digits in gw catchments to convert to Int16 *****"

        # Later, we will change this to the "debug" system down the road to keep or rm this
        # folder based on the debug flag being true or false. 
        # rm -rf $tempHucDataDir/branches/$branchId/

    elif [ $code -eq 1 ]; then
        # If it is a 1, then it would already have been added to the parent huc log automatically  
        # so just copy it to the branch_errors to help with visiblity
        echo "****** Exit status code of 1 detected *****"
        cp $branchLogFileName $tempHucDataDir/logs/branch_errors/
        
    else
        # could it be anything else? Yes.. might be a null/none, or any other
        # exit code like 2, 4, 5, etc and it has happened.
        echo
        msg="***** Invalid status code returned while processing branch ${hucNumber} : ${branchId}; \
            Exit status is $code *****"
        # add it to the log file
        echo $msg
        echo $msg >> $branchLogFileName
        cp $branchLogFileName $tempHucDataDir/logs/branch_errors
    fi
done

# We can not write to the huc level or run time files/folders as it can and has created multi-proc errors
# We can concat any message to its' own log file an later, the huc level will search for the word "error"
exit 0  # Always return a zero and let logging scan for the error.
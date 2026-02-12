#!/bin/bash -e
umask 000

# It is strongly recommended that you do not call src/run_by_branch.sh directly.
# Call this file instead, and let it call run_by_branch.sh as it will trap all 
# and any exceptions from run_by_branch.sh.
# This is a key part to handling .sh exceptions.

# NOTE: Do not use l_echo here, just echo as it auto bubbles up to run_huc.sh 
# This script does not need a "trap" or handle_errors as it is caught in upstream scripts

# Any actual script errors here including from process_branch.sh
# will bubble up to run_huc.sh and fim_process_huc.sh

runName=$1
hucNumber=$2
branchId=$3

source $srcDir/bash_functions.env

# tempHucDataDir come from fim_process_unit_wb.sh
branchLogFileName=$tempHucDataDir/logs/branch/"$hucNumber"_branch_"$branchId".log
#/usr/bin/time -v $srcDir/run_by_branch.sh $hucNumber $branchId 2>&1 | tee $branchLogFileName
/usr/bin/time -f "$time_cmd_format" $srcDir/run_by_branch.sh $hucNumber $branchId 2>&1 | tee $branchLogFileName
echo ""

# See note in fim_process_huc.sh talking about PIPESTATUS info
return_codes=( "${PIPESTATUS[@]}" )

# we do this way instead of working directly with stderr and stdout
# as they were messing with output logs which we always want.

does_error_exist="false"
# Some exit codes are demeeded as acceptanble errors such as 61, 64 and 65 where we just want to log them
# and continue. 
# The return_codes array can result in more than one loop below.
# Let each "code" print its own messages. We can get more than one exit code of 0 but we only want to
# honor it and use it if no other code has appeared

# Most of these can end up in the huc log twice and that is ok.
for code in "${return_codes[@]}"
do
    # Note: It was tricky to load in the fim_enum into bash, so we will just
    # go with the exit code for now
    if [ $code -eq 0 ]; then
        echo ""
        # do nothing
        
    elif [ $code -eq 61 ]; then
        msg="***** (${hucNumber} : ${branchId}) : Exit status: $code : Branch has no valid flowlines *****"
        echo -e "$msg" ; echo -e "$msg" >> $branchLogFileName
        does_error_exist="true"
        
        # Later, we will change this to the "debug" system down the road to keep or rm this
        # folder based on the debug flag being true or false.
        # rm -rf $tempHucDataDir/branches/$branchId/

    elif [ $code -eq 64 ]; then
        msg="***** (${hucNumber} : ${branchId}) : Exit status: $code : Branch has no crosswalks *****"
        echo -e "$msg" ; echo -e "$msg" >> $branchLogFileName
        does_error_exist="true"

        # Later, we will change this to the "debug" system down the road to keep or rm this
        # folder based on the debug flag being true or false.
        # rm -rf $tempHucDataDir/branches/$branchId/

    elif [ $code -eq 65 ]; then
        msg="***** (${hucNumber} : ${branchId}) : Exit status: $code : Too many HydroIDs or a HydroID with more than 8 digits \
        in gw catchments to convert to Int16 *****"
        echo -e "$msg" ; echo -e "$msg" >> $branchLogFileName
        does_error_exist="true"

        # Later, we will change this to the "debug" system down the road to keep or rm this
        # folder based on the debug flag being true or false. 
        # rm -rf $tempHucDataDir/branches/$branchId/

    elif [ $code -eq 1 ]; then
        # If it is a 1, then it would already have been added to the parent huc log automatically  
        # so just copy it to the branch_errors to help with visiblity
        msg="****** (${hucNumber} : ${branchId}) : Exit status: $code detected *****"
        echo -e "$msg" ; echo -e "$msg" >> $branchLogFileName
        does_error_exist="true"
    else
        # could it be anything else? Yes.. might be a null/none, or any other
        # exit code like 2, 4, 5, etc and it has happened.
        echo
        msg="***** (${hucNumber} : ${branchId}) : Exit status: $code : Unknown status code returned while processing branch *****"
        # add it to the log file
        echo -e "$msg" ; echo -e "$msg" >> $branchLogFileName
        does_error_exist="true"
    fi
done

# robbb-pb broke it again


# # This is here are it is possible to have more than one code in the for loop above
# # This way.. we only ever get one success message if even applicable.
if [[ "$does_error_exist" == "false" ]]; then
    msg="***** Exit status: 0 - Success *****" $hucLogFile
    echo -e "$msg" ; echo -e "$msg" >> $branchLogFileName
    echo "Note: A temp bug may show this as success but python bugs often show up in the logs as " \
        "Command exited with non-zero status 1. Good enough for now as post processing logs are catching it."    
fi


# We can not write to the huc level or run time files/folders as it can and has created multi-proc errors
# We can concat any message to its' own log file an later, the huc level will search for the word "error"
exit 0  # Always return a zero and let logging scan for the error.
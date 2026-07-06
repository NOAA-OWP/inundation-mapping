#!/bin/bash -e
### We must have the -e above which is exit on error
### Yes.. not all of our .sh files are the same with the -e flag, be design.

#####################################
## Note:
##    This is a wrapper for calibrate_rating_curves.sh which is run for only one HUC.
##    This tool is called by rerun_calibation.py which has the HUC iterator in it.
##    rerun_calibration.py has the abililty to catch all errors for this and child pages

##    This this having a "tee" command, like fim_process_hucs.sh, the logs will
##    auto catch all outputs including errors and put it in the log file
#####################################


# calibrate_rating_curve.sh is used in two contexts:
# 1: As part of run_huc.sh -> fim_process_huc.sh.  fim_process_huc.sh has three critcal elements we need here
#      a) It catches StdErr and StdOut and passes it to "tee" which makes sure it gets logged.
#      b) It also catch the exit status in case the something catestropihic fails anywhere under the .sh and .py
#        scripts below it, such as ruc_huc.sh, calibrate_rating_curve.sh, process_branch.sh and run_branch.sh
#    All of those scripts roll up their StnErr and StdOut to fim_process_huc.sh. This system works well
#    for both processing in EC2 pipeline mode, and AWS Step Function mode.

# 2: As part of this process_rerun_calibration_huc.sh which is never called anywhere in the fim_process_huc.sh chain
#    Instead this does the same basic job as fim_process_huc.sh but only at a smaller scale and only when it is being
#    called as part of rerun_calibration.py. This also catches StdErr, StdOut and passes it to "tee". It also
#    can catch exit codes and covers any catestrophic errors (such as .sh script file errors)

# Either way calibrate_rating_curve.sh only uses echos, versus l_echo, which are caught in the "tee" commands.

# Note: While rerun_calibration.py also can use its subprocess to catch StdErr, StdOut and exit codes, is th
# ability for the wrapper script to manage most of its own bash command error without compromising
# log. This is a critical feature needed especially for AWS. Not so for rerun_calibration.py when it
# is run on an EC2 but ....

## otherwise we have two full seperate logging techniques and error trapping. Moreso, without this file, it would force
# rerun_calibration.py to have so sort out exit codes, StdErr and StdOut BEFORE it reviews and builds
# error logs when applicable.

# 'tempHucDataDir' is created as an enviro variable in rerun_calibration.py when in the rerun mode,
# but by fim_process_huc.sh in the pipeline mode.
# When in pipeline mode, 'tempHucDataDir' actually points to the 'fim-temp' directory and calibrate_rating_curve.sh.
# When in rerun mode, it sets that same variable name in the rerun_calibration.py code but it actually points
# to the true huc folder in the "outputs", "previous_fim" or equiv pathing. We also do not want calibrate_rating_curve.sh
# ever talkin the docker enviro to any variables of outputDestDir because AWS works with the "outputs" directory
# in a different way then EC2 pipeline mode.

# However, process_rerun_calibration_huc.sh is never called anywhere via pipeline process, EC2 or AWS Step functions.
# This means process_rerun_calibration_huc.sh can talk to anyone it wants.

# As always in multi-proc, it is ok to read files from here, but do not write to files or folders to a shared file.
# It can and already has happened in BED large scale runs. This is a rule that we have to makes sure does not happen
# somehow via rerun_calibraion.py in its MP mode.

# We can and want to use l_echo here, just not in child  .sh scripts.

# Store arguments in local variables (not exported, will be passed explicitly to child scripts)
calibration_rerun=$1
jobBranchLimit=$2
export hucNumber=$3  # hucNumber is still exported as it's used by sourced files

if [ "$hucNumber" = "" ] ; then
    # putting the echo to &2 (stdError) which rerun_calibration.py as StdErr
    echo "ERROR: Missing hucNumber argument (2nd argument)" >&2; exit 1
fi

re='^[0-9]+$'
if ! [[ $hucNumber =~ $re ]] ; then
   echo "Error: hucNumber is not a number" >&2; exit 1
fi

if [ "$tempHucDataDir" = "" ] ; then
    echo "Error: tempHucDataDir is an empty" >&2; exit 1
fi

# ROB.... We can likely drop in Carsons scan, with an args saying look
# for recal records and not the reg log file


scan_logs_for_errors(){

    echo "++++++++++++++++++++++++++"
    l_echo "Scanning for issues in the logs" $rerunErrorLogFilename
    # No.. the line above is not a mistype.
    # Can't put the word "error" as as a header in the log file as it finds itself in the log files

    # Scan for the word error in the log file. Exit codes were already managed above.
    # We may end up with dup entries but that is ok.
    # Everything else including errors in calibrate_rating_curves.sh and its children
    # are already rolled up in the calib log file and calib error file.

    grep -Hine "Command exited with non-zero status" $rerunlogFilename >> $rerunErrorLogFilename
    grep -Hine "error" $rerunlogFilename >> $rerunErrorLogFilename
    grep -Hine "parallel" $rerunlogFilename >> $rerunErrorLogFilename
    grep -Hine "Exception" {} + >> $all_errors_log

    # we need to also check the files in the src_calibration files for errors and exceptions values
    # Some of the py files using src_calibration log folder may have incomplete logs and not
    # re-raising exception if applicable and most won't actualy complete the write of an error to
    # a log file so it never really gets logged. Look at some of the src...py files, then look for
    # the word "except", the watch what is happening on logs or log variables. 
    # Let's scan that dir to see if we cand find anythign but could be lots missing.
    echo "Scanning issues in the src_calibration folder."

    # Yes... there will be some duplication of errors in logs but good enough for now until we can make it smarter
    find $tempHucDataDir -path "*/*/logs/src_calibrations/*.log" -type f -exec grep -Hni "error" {} + >> $rerunErrorLogFilename  || true
    find $tempHucDataDir -path "*/*/logs/src_calibrations/*.log" -type f -exec grep -Hni "exception" {} + >> $rerunErrorLogFilename  || true
    find $tempHucDataDir -path "*/*/logs/src_calibrations/*.log" -type f -exec grep -Hni "parallel" {} + >> $rerunErrorLogFilename  || true
    find $tempHucDataDir -path "*/*/logs/src_calibrations/*.log" -type f -exec grep -Hni "Command exited with non-zero status" {} + >> $rerunErrorLogFilename  || true


    # Look for warnings in the calibration folder
    find $tempHucDataDir -path "*/*/logs/src_calibrations/*.log" -type f -exec grep -Hni "warning" {} + >> $rerunErrorLogFilename  || true    

    wait # wait for all background grep jobs to complete
    
    echo "++++++++++++++++++++++++++"
}

# As originally designed, it seems much better to keep its own logging seperate from the
# original logs.
rerunlogFilename=$tempHucDataDir/logs/huc_${hucNumber}_calib_rerun.log
rerunErrorLogFilename=$tempHucDataDir/logs/huc_${hucNumber}_calib_rerun_errors.log
rerunWarningLogFilename=$tempHucDataDir/logs/huc_${hucNumber}_calib_rerun_warnings.log

# We need remove earlier versions from previous recalibration runs.
rm -f $rerunlogFilename
rm -f $rerunErrorLogFilename
rm -f $rerunWarningLogFilename  # do we want a warning system here?
rm -rdf $tempHucDataDir/logs/src_calibrations

source $outputDestDir/params_rerun.env  # copied in from rerun_calibration.py
source $srcDir/bash_functions.env
source $srcDir/bash_variables.env

# Tell the system the name and location of the log file
# But don't allow calibrate_rating_curves.sh to do l_echos, only echos and prints.
# Echos and prints are caught here via the "tee" command
# Set_log_file_path $rerunlogFilename


echo "=========================================================================="
l_echo "---- Start of recalibration for $hucNumber" $rerunlogFilename

# Clean out previous src_calibration logs.

# run the actual calibration script (passing arguments explicitly since source commands may overwrite them)
/usr/bin/time -v $srcDir/calibrate_rating_curves.sh "$calibration_rerun" "$jobBranchLimit" "$hucNumber" 2>&1 | tee $rerunlogFilename

# We will check the actual exit status codes. If we find a non-zero, we will
# log it in the error file, then reraise the exit code and let rerun_calibration.py
# figure out what it wants to do with it (log it or abort it's full huc iterator)
return_codes=( "${PIPESTATUS[@]}" )

# turn trapping on for just here down. We can not use a trap above the "tee"
# line unless we detected and build variables for logging files / folders.
# Maybe for another day. Yes.. for now this has to be an acceptable hole.
# trap 'handle_error $LINENO' ERR

# Yes... we can get more than one returned code, it is possible but very rare
for code in "${return_codes[@]}"
do
    if [ $code -eq 0 ]; then
        echo
        # do nothing
    else
        err_msg="***** An error has occurred - Code (${code}) *****"
        l_echo "$err_msg" $rerunlogFilename

        # We are re-raising the error and let rerun_calibration.py manage.
        # in the process_huc mode for both standard pipeline and AWS mode, we must
        # have process_huc alwasy return a zero, as if it was alway successfull

        # Scan logs before exiting so errors are captured
        scan_logs_for_errors

        exit $code
        fi
done

scan_logs_for_errors
l_echo "---- End of recalibration for $hucNumber" $rerunlogFilename

# Rob.... add scan for warnings as well


# TODO... ROB:  can we add Carson scan tool with an arg switch?


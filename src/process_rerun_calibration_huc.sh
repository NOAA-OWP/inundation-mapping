#!/bin/bash -e

#####################################
## Note:
##   This error handling might seem like overkill but it very helpful with rerun_calibration.py
##   and it is critial to how this can work in both AWS mode, EC2 re-run mode and EC2 pipeline mode.
##   These parent scripts themselves can have errors and while rerun_calibration.py can and does
##   catch all exit the details to rerun_calibration.py.
#####################################

# This is a wrapper for calibrate_rating_curves.sh which is run for only one HUC.
# This tool is called by rerun_calibation.py which has the HUC iterator in it.

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

# We need to finish getting rerun_calibration.py to pass these variables in. Likely just needs single
# quotes all on variables otherwise datatypes can create problems (maybe ???)
# but we do need them here for processing flow reasons.
export calibration_rerun=$1
export jobBranchLimit=$2 # should allow new values for rerun_calibrate_rating_curves.py
export hucNumber=$3

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


# Some simple error handling
# We add it to the log file then scan for the word "error" later down.
# This will also handle errors in this script and not just calibrate_rating_curves.sh
handle_error(){

    echo "++++++++++++++++++++++++++++"
    msg="Critical error in process_rerun_calibration_huc.sh itself, line number:$1"
    l_echo "$msg" $rerunErrorLogFilename
    msg="Error Command Submitted: $BASH_COMMAND"
    l_echo "$msg" $rerunErrorLogFilename
    echo "++++++++++++++++++++++++++++"
    scan_logs_for_errors
    # logFileScanComplete="True"
    echo ""
    exit 1  # we DO want to return a exit status of 1 to rerun_calibration.py if in failure.
}

scan_logs_for_errors(){

    echo "++++++++++++++++++++++++++"
    l_echo "Scanning for err..ors and excep..tions in the logs"
    # No.. the line above is not a mistype.
    # Can't put the word "error" as as a header in the log file as it finds itself in the log files

    # Note sure why, but grep seems to like a & at the end when using find and grep together.
    # We have seen this for years but no idea why. Sometimes, as these three
    # do, without the &, the script themselves fail. Worse yet, it can fail and we don't
    # even know it. Hence the "process" wrappers and "traps". Maybe we can find better
    # ways to do it. A critical part is that grep itself can fail and can fail saying
    # it is trying to search a file that is part of its own search. Even though we had
    # code in there to make currently existing log or error log files, grep would fail
    # and we didn't know it and we lost our critcal error log file or errog log files additions.
    # The only safe way appears to be using the "find" and the grep together as we have found
    # countless times over the years, but again, no idea why.

    # Note !!!!
    # The problem above was the exact problem that was exposed in the AWS step function run
    # were we lost some critical error log information, which is why we change calibrate_rating_curve.sh
    # to the "process_rerun_calibraion_huc.sh" pattern same as process_huc and process_branch.sh

    # Scan for the word error in the log file. Exit codes were already managed above.
    # We may end up with dup entries but that is ok.
    # Everything else including errors in calibrate_rating_curves.sh and its children
    # are already rolled up in the calib log file and calib error file.

    # Grep Tech Tip.. use the -e flag when you are not using any wildcards or patterns
    # just a word in a line. If you need a regex type pattern, use -E instead.
    # This helps with errors in this fim_process_huc.sh script.
    # Grep is happy here becuase it is writing to a completely differnt file.
    # like we did when we recently use the "tmp" file system but was a tad confusing and a tad
    # unreliable.
    grep -H -i -n -e "Command exited with non-zero status" $rerunlogFilename >> $rerunErrorLogFilename
    grep -H -i -n -e "error" $rerunlogFilename >> $rerunErrorLogFilename
    grep -H -i -n -e "parallel" $rerunlogFilename >> $rerunErrorLogFilename

    # we need to also check the files in the src_calibration files for errors and exceptions values
    # Some of the py files using src_calibration log folder may have incomplete logs and not
    # re-raising exception if applicable and most won't actualy complete the write of an error to
    # a log file so it never really gets logged. Look at some of the src...py files, then look for
    # the word "except", the watch what is happening on logs or log variables. 
    # Let's scan that dir to see if we cand find anythign but could be lots missing.
    echo "Scanning for err..ors and issues in the src_calibration folder."

    find $tempHucDataDir -path "*/logs/src_calibrations/*.log" -type f | \
        xargs grep -H -n -i -e "error" >> $rerunErrorLogFilename &
    find $tempHucDataDir -path "*/logs/src_calibrations/*.log" -type f | \
        xargs grep -H -n -i -e "exception" >> $rerunErrorLogFilename &
    find $tempHucDataDir -path "*/logs/src_calibrations/*.log" -type f | \
        xargs grep -H -n -i -e "parallel" >> $rerunErrorLogFilename &            

    # Look for warenings in the calibration folder
    find $tempHucDataDir -path "*/logs/src_calibrations/*.log" -type f | \
        xargs grep -H -n -i -e "warning" >> $rerunWarningLogFilename &

    wait # wait for all background grep jobs to complete
    
    echo "++++++++++++++++++++++++++"
}

# As originally designed, it seems much better to keep its own logging seperate from the
# original logs.
rerunlogFilename="$tempHucDataDir/logs/huc_${hucNumber}_calib_rerun.log"
rerunErrorLogFilename="$tempHucDataDir/logs/huc_${hucNumber}_calib_rerun_errors.log"
rerunWarningLogFilename="$tempHucDataDir/logs/huc_${hucNumber}_calib_rerun_warnings.log"

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
Set_log_file_path $rerunlogFilename


echo "=========================================================================="
l_echo "---- Start of recalibration for $hucNumber" $rerunlogFilename

# Clean out previous src_calibration logs.

# run the actual calibration script
/usr/bin/time -v $srcDir/calibrate_rating_curves.sh 2>&1 | tee $rerunlogFilename

# We will check the actual exit status codes. If we find a non-zero, we will
# log it in the error file, then reraise the exit code and let rerun_calibration.py
# figure out what it wants to do with it (log it or abort it's full huc iterator)
return_codes=( "${PIPESTATUS[@]}" )

# turn trapping on for just here down. We can not use a trap above the "tee"
# line unless we detected and build variables for logging files / folders.
# Maybe for another day. Yes.. for now this has to be an acceptable hole.
trap 'handle_error $LINENO' ERR

# Yes... we can get more than one returned code, it is possible but very rare
for code in "${return_codes[@]}"
do
    if [ $code -eq 0 ]; then
        echo
        # do nothing
    else
        err_msg+="***** An error has occurred - Code ("${code}") *****" + $"\n"
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

exit 0   # between trap and tee, this will only be a zero when it gets here.

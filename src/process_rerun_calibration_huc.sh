#!/bin/bash
# set -e         # Critical: Can not have a -e inplace in order for the error handline
set -o pipefail  # Crucial: Forces the pipe to fail if the subscript fails but only when a pipe is used.
# For this one, we do want to stop, but the error script will always be caught
# by the trap, then always hit the exit_and_copy if the error happens after the
# code is executed. We want this script to ALWAYS return a 0
### Yes.. not all of our .sh files are the same with the -e flag, by design.

# ++++++++++++++++++++++++++++
# TODO: We should be able to adjust the rerun_calibration.py script to handle stdErr and StdOut a bit better,
# make it fully responsible for its own logging and error handle. Once we get that running, we can drop the need
# for this script.
# ++++++++++++++++++++++++++++


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

source $outputDestDir/params_rerun.env  # copied in from rerun_calibration.py
source $srcDir/bash_functions.env
source $srcDir/bash_variables.env

# we have no need for an exit and copy as we are still in the original outputs dir, not the true fim_temp dir

# TODO: July 2026: using setfacl is a power tool to help manage perms settings
# but it is not yet in our Docker build. See notes in Dockerfile.dev
# Set default permissions for the owner, group, and others
# This forces 775 (rwxrwxr-x) on all newly created files and folders
# setfacl -d -m u::rwx $tempHucDataDir
# setfacl -d -m g::rwx $tempHucDataDir
# setfacl -d -m o::rx $tempHucDataDir
# In the meantime, we have a weird combination of inefficient chmod everywhere.

# As originally designed, it seems much better to keep its own logging seperate from the
# original logs.
export rerunlogFilename=$tempHucDataDir/logs/huc_${hucNumber}_calib_rerun.log
export rerunErrorLogFilename=$tempHucDataDir/logs/huc_${hucNumber}_calib_rerun_errors.log  # do we need this anymore?
export rerunWarningLogFilename=$tempHucDataDir/logs/huc_${hucNumber}_calib_rerun_warnings.log
export rerunErrorLogCSVReport=$tempHucDataDir/logs/huc_${hucNumber}_calib_rerun_error_report.csv

# No matter what happens, this will execute and copy the folder from temp to outputs
# starting from the trap 'exit_and_copy' EXIT and down, ie) the arg tests above
exit_and_copy() {

    set +e  # shut off auto exit on error
    ## ===============================
    l_echo $startDiv"Compiling rerun calibration err..or report" $rerunlogFilename
    # Tstart

    # Note: For this file only, and seeing as it has the rerun_calibration.py file, it is ok to let
    # the error go back if the script itself fails.
    python3 $srcDir/utils/huc_process_error_report.py \
    -u $hucNumber -s $rerunlogFilename -o $rerunErrorLogCSVReport 2>&1 | tee -a -i $rerunlogFilename 

    l_echo "---- End of recalibration for $hucNumber" $rerunlogFilename
}


# This safety net catches the exit command and runs your final lines
# but only from this point down. This helps ensure outputs data is copied from temp to outputs
# from here down. Not perfect, but pretty good.
trap 'exit_and_copy' EXIT

# We need remove earlier versions from previous recalibration runs.
rm -f $rerunlogFilename
rm -f $rerunErrorLogFilename
rm -f $rerunWarningLogFilename  # do we want a warning system here?
rm -f $rerunWarningLogFilename  # do we want a warning system here?
rm -rdf $tempHucDataDir/logs/src_calibrations

# Tell the system the name and location of the log file
# But don't allow calibrate_rating_curves.sh to do l_echos, only echos and prints.
# Echos and prints are caught here via the "tee" command
# Set_log_file_path $rerunlogFilename

# Error handling starts from here down.
# Note: While the error log file will capture errors from this page or its "tee" returns,
# it does not include some errors and exceptions recorded inside the child .sh files. 

# We will catch those via error search tools.
set +e  # turns off, yes off, the system no longer auto aborts, as trapping will handle it from from here down.
trap 'handle_error $LINENO $rerunlogFilename' ERR

echo "=========================================================================="
l_echo "---- Start of recalibration for $hucNumber" $rerunlogFilename

# run the actual calibration script (passing arguments explicitly since source commands may overwrite them)
/usr/bin/time -v $srcDir/calibrate_rating_curves.sh "$calibration_rerun" "$jobBranchLimit" "$hucNumber" 2>&1 | tee $rerunlogFilename

# TODO: This only gets called if the pages has completed successfully.
grep -Hin "warning" "${rerunlogFilename}" > "${rerunWarningLogFilename}"

# exit_and_copy will be copied here if not earlier,
# depending on exceptions or errors from the TRAP ... ERR and down.
# in 'exit_and_copy', will also do the error log scanning

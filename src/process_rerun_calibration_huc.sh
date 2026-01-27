#!/bin/bash -e

# This is a wrapper for calibrate_rating_curves.sh which is run for only one HUC.
# This tool is called by rerun_calibation.py which has the HUC iterator in it.

# calibrate_rating_curve.sh is used in two contexts:
# 1: As part of run_huc.sh -> fim_process_huc.sh.  fim_process_huc.sh has three critcal elements we need here
#      a) It catchs StdErr and StdOut and passes it to "tee" which makes sure it gets logged.
#      b) It also catch the exit status in case the something catestropihic fails anywhere under the .sh and .py
#        scripts below it, such as ruc_huc.sh, calibrate_rating_curve.sh, process_branch.sh and run_branch.sh
#    All of those scripts roll up their StnErr and StdOut to fim_process_huc.sh

# 2: As part of this process_rerun_calibration_huc.sh which is never called anywhere in the fim_process_huc.sh chain
#    Instead this does the same basic job as fim_process_huc.sh but only at a smaller scale and only when it is being
#    called as part of rerun_calibration.py. This also catches StdErro, StdOut and passes it to "tee". It also
#    can catch exit codes and covers any catestrophic errors (such as .sh script file errors)

# Either way calibrate_rating_curve.sh only uses echos which are caugt in the "tee" commands.

# Note: While rerun_calibration.py also can use its subprocess to catch StdErr, StdOut and exit codes, one additional
# job that both fim_process_huc.sh and this file does, is scannign for errors via logs and exit codes and add them to error
# files. This is a critical feature needed especially for AWS. Not so for rerun_calibraitn.py when it is run on an EC2 but
# otherwise we have two full seperate logging techniques and error trapping. Moreso, without this file, it woudl force
# rerun_calibration.py to have so sort out exit codes, StdErr and StdOut BEFORE it reviews and builds error logs when applicable.

# outputDestDir and tempHucDataDir are setup as an enviro variable in rerun_calibration.py.
# As always in multi-proc, it is ok to read files from here, but do not write to files or folders to a shared file.

# The variable tempHucDataDir which is used throughout calibrate_rating_curve.sh.
# When calibrate_rating_curves.sh is run as part of the fim_process_huc.sh chain it is
# actualy done in the fim-temp directories. But when we run rerun_calibation.py, it is done in the output directory.

export calibration_rerun=$1
export jobBranchLimit=$2 # should allow new values for rerun_calibrate_rating_curves.py
export hucNumber=$3

rerun_log_filename="$tempHucDataDir/logs/${hucNumber}/huc_${hucNumber}_calib_rerun.log"
rerun_error_log_filename="$tempHucDataDir/logs/${hucNumber}/huc_${hucNumber}_errors_calib_rerun.log"

source $outputDestDir/params_rerun.env  # copied in from rerun_calibration.py
source $srcDir/bash_functions.env
source $srcDir/bash_variables.env


# TODO: Jan 26: ROB !!!! finish this (baesd on fim_process_huc.sh)


# Some simple error handling
# We add it to the log file then scan for the word "error" later down.
# This will also handle errors in this script and not just calibrate_rating_curves.sh
handle_error(){

    orig_fail_line_num=$1
    echo "++++++++++++++++++++++++++++"
    msg="Critical error in process_rerun_calibration_huc.sh, line number:$orig_fail_line_num"
    l_echo $msg $rerun_error_log_filename

    msg="Error Command Submitted: $BASH_COMMAND"
    l_echo "$msg" $rerun_error_log_filename
    echo "++++++++++++++++++++++++++++"
    echo
    exit 1  # we do want to return a exit status of 1 to rerun_calibration.py if in failure
}


# Tell the system the name and location of the log file
# But don't allow calibrate_rating_curves.sh to do anything but echos and prints but not l_echo.
# Echos and prints are caught here via the "tee" command
Set_log_file_path $rerun_log_filename

# In case there is a critical error with logic on this page.
trap 'handle_error $LINENO' ERR

# run the actual calibration script
# Skip using the time command on this as it is very short
/usr/bin/time -f "$time_cmd_format" $srcDir/calibrate_rating_curves.sh 2>&1 | tee $rerun_log_filename
return_code=$?

err_exists=0
if [ $return_code -eq 0 ]; then
    echo
    # do nothing
else
    err_msg+="***** An error has occurred - Code ("${code}") *****" + $"\n"
    l_echo $err_msg $rerun_log_filename
    err_exists=1
fi

# Scan for the word error in the log file. Exit codes were already managed above.
# We may end up with dup entries but that is ok.
# Everything else including branch errors are already rolled up in the huc log file
# and huc error file.
l_echo "Scanning for the phrase 'error' or 'parallel' in the huc log file" $rerun_log_filename
grep -H -i -n -e ".*error.*" -e ".*parallel.*" $rerun_log_filename >> $rerun_error_log_filename

#  We will exit with just a 0 or 1 as rerun_calibration.py is looking for just exit codes
exit $err_exists

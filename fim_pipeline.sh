#!/bin/bash -e

:
usage()
{
    echo "
    Processing of HUC's in FIM4 comes in three sections. You can run 'fim_pipeline.sh' which will run
        the three main scripts: 'fim_pre_processing.sh', 'fim_process_unit_wb.sh' & 'fim_post_processing.sh'.

    Usage : fim_pipeline.sh -u <huc8> -n <name_of_your_run>

    All arguments to this script are passed to 'fim_pre_processing.sh'.
    REQUIRED:
      -u/--hucList      : HUC8s to run; more than one HUC8 should be passed in quotes (space delimited).
                            A line delimited file, with a .lst extension, is also acceptable.
                            HUC8s must be present in inputs directory.
      -n/--runName      : A name to tag the output directories and log files (only alphanumeric).

    OPTIONS:
      -h/--help         : Print usage statement.
      -c/--config       : Configuration file with bash environment variables to export
                        - Default: config/params_template.env
      -ud/--unitDenylist
                        A file with a line delimited list of files in UNIT (HUC) directories to be
                            removed upon completion.
                        - Default: config/deny_unit.lst
                        - Note: if you want to keep all output files (aka.. no files removed),
                            use the word NONE as this value for this parameter.
      -bd/--branchDenylist
                        A file with a line delimited list of files in BRANCHES directories to be
                            removed upon completion of branch processing.
                        - Default: config/deny_branches.lst
                        - Note: if you want to keep all output files (aka.. no files removed),
                            use the word NONE as this value for this parameter.
      -zd/--branchZeroDenylist
                        A file with a line delimited list of files in BRANCH ZERO directories to
                            be removed upon completion of branch zero processing.
                        - Default: config/deny_branch_zero.lst
                        - Note: If you want to keep all output files (aka.. no files removed),
                            use the word NONE as this value for this parameter.
      -jh/--jobLimit    : Max number of concurrent HUC jobs to run. Default 1 job at time.
      -jb/--jobBranchLimit
                        Max number of concurrent Branch jobs to run. Default 1 job at time.
                        - Note: Make sure that the product of jh and jb plus 2 (jh x jb + 2)
                            does not exceed the total number of cores available.
      -o                : Overwrite outputs if they already exist.
      -skipcal          : If this param is included, the S.R.C. will be updated via the calibration points.
                            will be skipped.
      -x                : If this param is included, the crosswalk will be evaluated.


    Running 'fim_pipeline.sh' is a quicker process than running all three scripts independently; however,
        you can run them independently if you like. The three sections are:

            - 'fim_pre_processing.sh' : This section must be run first as it creates the basic output folder
                for the run. Key files and folders for the next two sections are also created.

            - 'fim_process_unit_wb.sh' : This script processes one and exactly one HUC8 plus all of its
                related branches. While it can only process one, you can run this script multiple times,
                each with different HUC (or overwriting a HUC). When you run 'fim_pipeline.sh',
                when more than one HUC is provided, this script is iterated over, and parallelized.
                For each HUC provided, 'fim_pipeline.sh' will call 'fim_process_unit_wb.sh'.
                Using the 'fim_process_unit_wb.sh' script allows for a run / rerun of a HUC, or running other
                HUCs at different times / days or even in different docker containers.

            - 'fim_post_processing.sh' : This section takes all of the HUCs that have been processed,
                aggregates key information from each HUC directory and looks for errors across all HUC
                folders. It also processes the group in sub-steps such as usgs guages processesing,
                rating curve adjustments and more. Naturally, running or re-running this script can only
                be done after running 'fim_pre_processing.sh' and at least one run of 'fim_process_unit_wb.sh'.

    "
    exit
}


set -e

# print usage if agrument is '-h' or '--help'
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    usage
fi


echo
echo "======================= Start of fim_pipeline.sh ========================="
echo "---- Started: `date -u`"

## LOAD AND VALIDATE INCOMING ARGUMENTS
source $srcDir/bash_functions.env
. $projectDir/fim_pre_processing.sh "$@"
jobMaxLimit=$(( $jobHucLimit * $jobBranchLimit ))

logFile=$outputDestDir/logs/unit/pipeline_summary_unit.log
process_wb_file=$projectDir/fim_process_unit_wb.sh

pipeline_start_time=`date +%s`

# PROCESS THE UNITS (And branches)
# Why an if and else? watch the number of colons
if [ -f "$hucList" ]; then
    if [ "$jobHucLimit" = "1" ]; then
        parallel --verbose --lb -j $jobHucLimit --colsep ',' --joblog $logFile -- $process_wb_file $runName :::: $hucList
    else
        parallel -j $jobHucLimit --colsep ',' --joblog $logFile -- $process_wb_file $runName :::: $hucList
    fi
else
    if [ "$jobHucLimit" = "1" ]; then
        parallel --verbose --lb -j $jobHucLimit --colsep ',' --joblog $logFile -- $process_wb_file $runName ::: $hucList
    else
        parallel -j $jobHucLimit --colsep ',' --joblog $logFile -- $process_wb_file  $runName  ::: $hucList
    fi
fi

echo
echo "---- Unit (HUC) processing is complete"
date -u
Calc_Duration "Duration : " $pipeline_start_time
echo "---------------------------------------------------"

## POST PROCESSING

# Remove run from the fim_temp directory
rm -df $workDir/$runName

# Pipe into post processing
. $projectDir/fim_post_processing.sh -n $runName -j $jobMaxLimit

echo

echo "======================== End of fim_pipeline for $runName =========="
date -u
Calc_Duration "Total Duration is ... " $pipeline_start_time
echo

# Exit the script
exit 0

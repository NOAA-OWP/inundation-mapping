#!/bin/bash -e
### We want to keep the -e as it is ok to let errors and codes returned to fim_pipeline.sh or AWS
### for this specific file.
### Yes.. not all of our .sh files are the same with the -e flag, be design.

:
usage()
{
    echo "
    This script collects & validates inputs, creates files & folders,
        as well as loads/sets environment & configuration variables.

    Usage : fim_pre_processing.sh [REQ: -u <hucs> -n <run name> ] [OPT: see below for OPTIONS]

    REQUIRED:
      -u/--hucList      : HUC8s to run; more than one HUC8 should be passed in quotes (space delimited).
                            A line delimited file, with a .lst extension, is also acceptable.
                            HUC8s must be present in inputs directory.
      -n/--runName      : A name to tag the output directories and log files.

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
    "
}

set -e

while [ "$1" != "" ]; do
case $1
in
    -u|--hucList)
        shift
        hucList=$1
        ;;
    -c|--configFile )
        shift
        envFile=$1
        ;;
    -n|--runName)
        shift
        runName=$1
        ;;
    -jh|--jobHucLimit)
        shift
        jobHucLimit=$1
        ;;
    -jb|--jobBranchLimit)
        shift
        jobBranchLimit=$1
        ;;
    -h|--help)
        shift
        usage
        exit
        ;;
    -ud|--unitDenylist)
        shift
        deny_unit_list=$1
        ;;
    -bd|--branchDenylist)
        shift
        deny_branches_list=$1
        ;;
    -zd|--branchZeroDenylist)
        shift
        deny_branch_zero_list=$1
        ;;
    -o)
        overwrite=1
        ;;
    -x)
        evaluateCrosswalk=1
        ;;
    *) ;;
    esac
    shift
done

# exit 22 means bad argument

# print usage if arguments empty
if [ "$hucList" = "" ]
then
    echo "ERROR: Missing -u Huclist argument"
    usage
    exit 22
fi
if [ "$runName" = "" ]
then
    echo "ERROR: Missing -n run time name argument"
    usage
    exit 22
fi

# outputsDir & workDir come from the Dockerfile
outputDestDir=$outputsDir/$runName
tempRunDir=$workDir/$runName

# default values
if [ "$envFile" = "" ]; then envFile=/$projectDir/config/params_template.env; fi
if [ "$jobHucLimit" = "" ]; then jobHucLimit=1; fi
if [ "$jobBranchLimit" = "" ]; then jobBranchLimit=1; fi
if [ -z "$overwrite" ]; then overwrite=0; fi
if [ -z "$evaluateCrosswalk" ]; then evaluateCrosswalk=0; fi

# validate and set defaults for the deny lists
if [ "$deny_unit_list" = "" ]
then
    deny_unit_list=$projectDir/config/deny_unit.lst
elif [ "${deny_unit_list^^}" != "NONE" ] && [ ! -f "$deny_unit_list" ]
then
    # NONE is not case sensitive
    echo "Error: The -ud <unit deny file> does not exist and is not the word NONE"
    usage
    exit 22
fi

# validate and set defaults for the deny lists
if [ "$deny_branches_list" = "" ]
then
    deny_branches_list=$projectDir/config/deny_branches.lst
elif [ "${deny_branches_list^^}" != "NONE" ] && [ ! -f "$deny_branches_list" ]
then
    # NONE is not case sensitive
    echo "Error: The -bd <branch deny file> does not exist and is not the word NONE"
    usage
    exit 22
fi

# We do a 1st cleanup of branch zero using branchZeroDenylist (which might be none).
# Later we do a 2nd cleanup of the branch zero that make the final output of branch zero
# to match what all other branch folders have for remaining files. But.. if we override
# branchZeroDenylist, we don't want it to be cleaned a second time.
has_deny_branch_zero_override=0
if [ "$deny_branch_zero_list" = "" ]
then
    deny_branch_zero_list=$projectDir/config/deny_branch_zero.lst
elif [ "${deny_branch_zero_list^^}" != "NONE" ]   # NONE is not case sensitive
then
    if [ ! -f "$deny_branch_zero_list" ]
    then
        echo "Error: The -zd <branch zero deny file> does not exist and is not the word NONE"
        usage
        exit 22
    else
        # only if the deny branch zero has been overwritten and file exists
        has_deny_branch_zero_override=1
    fi
else
    has_deny_branch_zero_override=1 # it is the value of NONE and is overridden
fi

# Safety feature to avoid accidental overwrites
if [ -d $outputDestDir ] && [ $overwrite -eq 0 ]; then
    echo
    echo "ERROR: Output dir $outputDestDir exists. Use overwrite -o to run."
    echo
    usage
    exit 22
fi

## SOURCE ENV FILE AND FUNCTIONS ##
source $srcDir/bash_functions.env
source $srcDir/bash_variables.env

# these export are for fim_pipeline only.
export runName=$runName
export jobHucLimit=$jobHucLimit
huc_list_output_file="${outputDestDir}/huc_list.txt"

# TODO: July 2026: using setfacl is a power tool to help manage perms settings
# but it is not yet in our Docker build. See notes in Dockerfile.dev
# Set default permissions for the owner, group, and others
# This forces 775 (rwxrwxr-x) on all newly created files and folders
# setfacl -d -m u::rwx $tempHucDataDir
# setfacl -d -m g::rwx $tempHucDataDir
# setfacl -d -m o::rx $tempHucDataDir
# In the meantime, we have a weird combination of inefficient chmod everywhere.

if [ -d $tempRunDir ]; then
    chmod 777 -R $tempRunDir  # in theory we do not need this (but needed for aborts)
    rm -rdf $tempRunDir
    mkdir -p $tempRunDir
    chmod 777 $tempRunDir
    # TODO: Hold until a new docker image is created    
    # Set default permissions for the owner, group, and others
    # This forces 775 (rwxrwxr-x) on all newly created files and folders
    # setfacl -d -m u::rwx $tempRunDir
    # setfacl -d -m g::rwx $tempRunDir
    # setfacl -d -m o::rx $tempRunDir
fi

# make dirs
if [ ! -d $outputDestDir ]; then
    mkdir -p $outputDestDir
    chmod 777 -R $outputDestDir
    # TODO: Hold until a new docker image is created
    # Set default permissions for the owner, group, and others
    # This forces 775 (rwxrwxr-x) on all newly created files and folders
    # setfacl -d -m u::rwx $tempRunDir
    # setfacl -d -m g::rwx $tempRunDir
    # setfacl -d -m o::rx $tempRunDir

else
    # remove these directories and files on a new or overwrite run
    rm -rdf $outputDestDir/logs
    # rm -rdf $outputDestDir/branch_errors
    rm -rdf $outputDestDir/eval
    rm -f $outputDestDir/crosswalk_table.csv
    rm -f $outputDestDir/fim_inputs*
    rm -f $outputDestDir/*.env
    rm -f $huc_list_output_file
fi

mkdir -p $outputDestDir/logs
chmod 777 $outputDestDir/logs

# Yes.. if this fails in AWS, we don't see it easily but can see it in its CloudWatch
# This is a parsed, cleaned up line by line huc list, regardless if it is a file, single huc, multiple hucs, etc
num_hucs=$(python3 $srcDir/check_huc_inputs.py -u ${hucList} -i ${FULL_HUC_LIST_PATH} -o ${huc_list_output_file} )
echo
echo "--- Number of HUCs to process is $num_hucs"


# copy over config file and rename it (note.. yes, the envFile file can still be
# loaded from command line and have its own values, it simply gets renamed and saved)
cp $envFile $outputDestDir/params.env

# create an new .env file on the fly that contains all runtime values
# that any unit can load it independently (in seperate AWS objects, AWS fargates)
# or via pipeline. There is likely a more elegent way to do this.

args_file=$outputDestDir/runtime_args.env

# reset it again (this time recursive for the new incoming folders
chmod 777 -R $outputDestDir

# the jobHucLimit is not from the args files, only jobBranchLimit
echo "export runName=$runName" >> $args_file
echo "export jobHucLimit=$jobHucLimit" >> $args_file
echo "export jobBranchLimit=$jobBranchLimit" >> $args_file
echo "export deny_unit_list=$deny_unit_list" >> $args_file
echo "export deny_branches_list=$deny_branches_list" >> $args_file
echo "export deny_branch_zero_list=$deny_branch_zero_list" >> $args_file
echo "export has_deny_branch_zero_override=$has_deny_branch_zero_override" >> $args_file
echo "export evaluateCrosswalk=$evaluateCrosswalk" >> $args_file

chmod 777 $args_file

echo "--- Pre-processing is complete"
echo

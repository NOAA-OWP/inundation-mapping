#!/bin/bash -e
:
usage ()
{
    echo
    echo 'Produce FIM hydrofabric datasets for unit and branch scale.'
    echo 'Usage : [REQ: -u <hucs> -n <run name> ]'
    echo '        [OPT: -h -c <config file> -o'
    echo '          -ud <unit deny list file>'
    echo '          -bd <branch deny list file>'
    echo '          -zd <branch zero deny list file>'
    echo '          -jh <number of jobs for HUCs>'
    echo '          -jb <number of jobs for branches>'
    echo '          -skipcal <skip the calibration points process>'
    echo '          -isaws ]'
    echo ''
    echo 'REQUIRED:'
    echo '  -u/--hucList    : HUC8s to run or multiple passed in quotes (space delimited) file.'
    echo '                    A line delimited file, with a .lst extension, is also acceptable.'
    echo '                    HUCs must present in inputs directory.'
    echo '  -n/--runName    : A name to tag the output directories and log files.'
    echo 
    echo 'OPTIONS:'
    echo '  -h/--help       : Help file'
    echo '  -c/--config     : Configuration file with bash environment variables to export'
    echo '                    Default (if arg not added) : /foss_fim/config/params_template.env'    
    echo '  -ud/--unitDenylist'
    echo '                    A file with a line delimited list of files in UNIT (HUC) directories to be removed'
    echo '                    upon completion (see config/deny_unit.lst for a starting point)'
    echo '                    Default (if arg not added) : /foss_fim/config/deny_unit.lst'
    echo '                    -- Note: if you want to keep all output files (aka.. no files removed),'
    echo '                    use the word NONE as this value for this parameter.'
    echo '  -bd/--branchDenylist'
    echo '                    A file with a line delimited list of files in BRANCHES directories to be removed' 
    echo '                    upon completion of branch processing.'
    echo '                    (see config/deny_branches.lst for a starting point)'
    echo '                    Default: /foss_fim/config/deny_branches.lst'   
    echo '                    -- Note: if you want to keep all output files (aka.. no files removed),'
    echo '                    use the word NONE as this value for this parameter.'
    echo '  -zd/--branchZeroDenylist'
    echo '                    A file with a line delimited list of files in BRANCH ZERO directories to' 
    echo '                    be removed upon completion of branch zero processing.'
    echo '                    (see config/deny_branch_zero.lst for a starting point)'
    echo '                    Default: /foss_fim/config/deny_branch_zero.lst'   
    echo '                    -- Note: if you want to keep all output files (aka.. no files removed),'
    echo '                    use the word NONE as this value for this parameter.'    
    echo '  -jh/--jobLimit   : Max number of concurrent HUC jobs to run. Default 1 job at time.'
    echo '  -jb/--jobBranchLimit : Max number of concurrent Branch jobs to run. Default 1 job at time.' 
    echo '                         NOTE: Make sure that the multiplication of jh and jb subtract 2 (jh x jb -2) does not'
    echo '                         exceed the total number of cores available.'
    echo '  -o               : Overwrite outputs if already exist'
    echo '  -skipcal         : If this param is included, updating the S.R.C. via the calibration points will be skipped'    
    echo '  -isaws           : If this param is included, the code will use AWS objects where possible'
    echo '                   : Note: This feature is not yet implemented'
    echo
    exit
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
    -skipcal)
        skipcal=1
        ;;
    -isaws)
        isAWS=1
        ;;
    *) ;;
    esac
    shift
done

# print usage if arguments empty
if [ "$hucList" = "" ]
then
    echo "ERROR: Missing -u Huclist argument"
    usage
fi
if [ "$runName" = "" ]
then
    echo "ERROR: Missing -n run time name argument"
    usage
fi

# outputsDir & workDir come from the Dockerfile
outputDestDir=$outputsDir/$runName
tempRunDir=$workDir/$runName

# default values
if [ "$envFile" = "" ]; then envFile=/$projectDir/config/params_template.env; fi
if [ "$jobHucLimit" = "" ]; then jobHucLimit=1; fi
if [ "$jobBranchLimit" = "" ]; then jobBranchLimit=1; fi
if [ -z "$overwrite" ]; then overwrite=0; fi
if [ -z "$skipcal" ]; then skipcal=0; fi
if [ -z "$isAWS" ]; then isAWS=0; fi

# validate and set defaults for the deny lists
if [ "$deny_unit_list" = "" ]
then
    deny_unit_list=$projectDir/config/deny_unit.lst
elif [ "${deny_unit_list^^}" != "NONE" ] && [ ! -f "$deny_unit_list" ]
then
    # NONE is not case sensitive
    echo "Error: The -ud <unit deny file> does not exist and is not the word NONE"
    usage
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
    else
        # only if the deny branch zero has been overwritten and file exists
        has_deny_branch_zero_override=1 
    fi
else
    has_deny_branch_zero_override=1 # it is the value of NONE and is overridden
fi

# Safety feature to avoid accidentaly overwrites
if [ -d $outputDestDir ] && [ $overwrite -eq 0 ]; then
    echo
    echo "ERROR: Output dir $outputDestDir exists. Use overwrite -o to run."
    echo        
    usage
fi

## SOURCE ENV FILE AND FUNCTIONS ##
source $srcDir/bash_functions.env

# these export are for fim_pipeline only.
export runName=$runName
export jobHucLimit=$jobHucLimit

num_hucs=$(python3 $srcDir/check_huc_inputs.py -u $hucList -i $inputsDir)
echo
echo "--- Number of HUCs to process is $num_hucs"

# make dirs
if [ ! -d $outputDestDir ]; then
    mkdir -p $outputDestDir
    mkdir -p $tempRunDir
else
    # remove these directories and files on a new or overwrite run
    rm -rdf $outputDestDir/logs
    rm -rdf $outputDestDir/branch_errors
    rm -rdf $outputDestDir/unit_errors
    rm -rdf $outputDestDir/eval
    rm -f $outputDestDir/crosswalk_table.csv
    rm -f $outputDestDir/fim_inputs*
    rm -f $outputDestDir/*.env
fi

mkdir -p $outputDestDir/logs/unit
mkdir -p $outputDestDir/logs/branch
mkdir -p $outputDestDir/unit_errors
mkdir -p $outputDestDir/branch_errors

# copy over config file and rename it (note.. yes, the envFile file can still be
# loaded from command line and have its own values, it simply gets renamed and saved)
cp $envFile $outputDestDir/params.env

# create an new .env file on the fly that contains all runtime values
# that any unit can load it independently (in seperate AWS objects, AWS fargates)
# or via pipeline. There is likely a more elegent way to do this.

args_file=$outputDestDir/runtime_args.env

# the jobHucLimit is not from the args files, only jobBranchLimit
echo "export runName=$runName" >> $args_file
echo "export jobHucLimit=$jobHucLimit" >> $args_file
echo "export jobBranchLimit=$jobBranchLimit" >> $args_file
echo "export deny_unit_list=$deny_unit_list" >> $args_file
echo "export deny_branches_list=$deny_branches_list" >> $args_file
echo "export deny_branch_zero_list=$deny_branch_zero_list" >> $args_file
echo "export has_deny_branch_zero_override=$has_deny_branch_zero_override" >> $args_file
echo "export isAWS=$isAWS" >> $args_file
echo "export skipcal=$skipcal" >> $args_file

echo "--- Pre-processing is complete"

echo

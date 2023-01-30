#!/bin/bash -e

# Why is this file here and it appears to be using duplicate export variables?
# For AWS, we need to make a direct call to this files with two params, hucNumber first, 
# then the runName same as the -n flag in fim_pipeline and fim_pre_processing.sh

# This file will also catch any and all errors from run_by_huc_wb, even script aborts from that file

# You really can not call directly to run_unit_wb.sh as that file relys on export values
# from this file.
# run_by_huc_wb will futher process branches with its own iterator (parallelization).

# Sample Usage: /foss_fim/src/process_unit_wb.sh rob_test_wb_1 05030104

## START MESSAGE ##

echo

usage ()
{
    echo
    echo 'Produce FIM hydrofabric datasets for a single unit and branch scale.'
    echo 'NOTE: fim_pre_processing must have been already run and this tool'
    echo '      will not include post processing. Only single independent single'
    echo '      huc and its branches.'    
    echo 'Usage : There are no arg keys (aka.. no dashes)'
    echo '        you need the run name first, then the huc.'
    echo '        ie ) /foss_fim/src/process_unit_wb.sh rob_test_1 05030104'
    echo
    exit
}

export runName=$1
export hucNumber=$2

# print usage if arguments empty
if [ "$runName" = "" ]
then
    echo "ERROR: Missing run time name argument (1st argument)"
    usage
fi

if [ "$hucNumber" = "" ]
then
    echo "ERROR: Missing hucNumber argument (2nd argument)"
    usage
fi

re='^[0-9]+$'
if ! [[ $hucNumber =~ $re ]] ; then
   echo "Error: hucNumber is not a number" >&2; exit 1
   usage
fi

echo "=========================================================================="
echo "---- Start of huc processing for $hucNumber"

# outputDataDir, srcDir and others come from the Dockerfile

export outputRunDataDir=/$outputDataDir/$runName
export outputHucDataDir=$outputRunDataDir/$hucNumber
export outputBranchDataDir=$outputHucDataDir/branches
export current_branch_id=0

## huc data
if [ -d "$outputHucDataDir" ]; then
    rm -rf $outputHucDataDir
fi

# make outputs directory
mkdir -p $outputHucDataDir
mkdir -p $outputBranchDataDir

# Clean out previous unit logs and branch logs starting with this huc
rm -f $outputRunDataDir/logs/unit/"$hucNumber"_unit.log
rm -f $outputRunDataDir/logs/branch/"$hucNumber"_summary_branch.log
rm -f $outputRunDataDir/logs/branch/"$hucNumber"*.log
rm -f $outputRunDataDir/unit_errors/"$hucNumber"*.log
rm -f $outputRunDataDir/branch_errors/"$hucNumber"*.log
hucLogFileName=$outputRunDataDir/logs/unit/"$hucNumber"_unit.log

# Process the actual huc
/usr/bin/time -v $srcDir/run_unit_wb.sh | tee $hucLogFileName

#exit ${PIPESTATUS[0]} (and yes.. there can be more than one)
return_codes=( "${PIPESTATUS[@]}" )

#echo "huc return codes are:"
#echo $return_codes

# we do this way instead of working directly with stderr and stdout
# as they were messing with output logs which we always want.
for code in "${return_codes[@]}"
do
    # Make an extra copy of the branch log in a new folder  if an error
    # Note: It was tricky to load in the fim_enum into bash, so we will just 
    # go with the code for now
    if [ $code -eq 0 ]; then
        echo
        # do nothing
    elif [ $code -eq 60 ]; then
        echo
        echo "***** Unit has no valid branches *****"
    elif [ $code -eq 61 ]; then
        echo
        echo "***** Unit has not a valid unit *****"        
    else
        echo
        echo "***** An error has occured  *****"
        # copy the error log over to the unit_errors folder to better isolate it
        cp $hucLogFileName $outputRunDataDir/unit_errors
    fi
done

# TODO: Check its output logs for this huc and its branches here

echo "=========================================================================="
# we always return a success at this point (so we don't stop the loops / iterator)
exit 0

#!/bin/bash -e

#outputRunDataDir=$1

echo "Aggregating FIM required outputs"

# make aggregate fim outputs dir
outputRunDataDir=$1
fimAggregateOutputsDir=$outputRunDataDir/aggregate_fim_outputs
mkdir $fimAggregateOutputsDir

# cd to make vrt paths relative
cd $fimAggregateOutputsDir

# build rem vrt
gdalbuildvrt -q rem.vrt ../*/rem_clipped_zeroed_masked.tif

# build catchments vrt
gdalbuildvrt -q catchments.vrt ../*/gw_catchments_reaches_clipped_addedAttributes.tif

# aggregate hydro-table
i=0 #inialize counter variable
for f in $(find $outputRunDataDir -type f -name hydroTable.csv); do
    if [ "$i" -gt 0 ];then # aggregate remaining files without header
        tail -n+2 $f >> $fimAggregateOutputsDir/hydroTable.csv 
    else # copy first file over with header
        cat $f > $fimAggregateOutputsDir/hydroTable.csv  
    fi 
    ((i=i+1)) #counter variable
done

# cd back
cd $OLDPWD

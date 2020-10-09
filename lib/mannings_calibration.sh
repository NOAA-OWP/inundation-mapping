#!/bin/bash -e


param_set="$1"
hucdir="/data/outputs/"$indir/$huc

# Generate mannings_table.json
# python3 foss_fim/config/create_mannings_table.py -d $param_set -f $paramfolder/mannings_template.json

subdir=$paramfolder/$outdir/$huc"_""$count"
mkdir -p $subdir

$libDir/add_crosswalk.py $hucdir/gw_catchments_reaches_filtered_addedAttributes.gpkg $hucdir/demDerived_reaches_split_filtered.gpkg $hucdir/src_base.csv $hucdir/majority.geojson $subdir/gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg $subdir/demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg $subdir/src_full_crosswalked.csv $subdir/src.json $subdir/crosswalk_table.csv $subdir/hydroTable.csv $hucdir/wbd8_clp.gpkg $hucdir/nwm_subset_streams.gpkg $paramfolder/mannings_template.json $param_set

python3 foss_fim/tests/run_test_case_calibration.py -r $indir/$huc -d $subdir -t $huc"_ble" -b $subdir

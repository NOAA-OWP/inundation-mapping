#!/bin/bash -e

param_set="$1"
IFS=',' read -r -a array <<< $param_set

strorder="${array[1]}"
mannings_row=1+"$strorder"
mannings_value="${array[$mannings_row]}"

subdir=$outdir/$huc"_"$strorder"_"$mannings_value
mkdir -p $subdir

$libDir/add_crosswalk.py -d $hucdir/gw_catchments_reaches_filtered_addedAttributes.gpkg -a $hucdir/demDerived_reaches_split_filtered.gpkg -s $hucdir/src_base.csv -u $hucdir/majority.geojson -l $subdir/gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg -f $subdir/demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg -r $subdir/src_full_crosswalked.csv -j $subdir/src.json -x $subdir/crosswalk_table.csv -t $subdir/hydroTable.csv -w $hucdir/wbd8_clp.gpkg -b $hucdir/nwm_subset_streams.gpkg -m $param_set -c

python3 foss_fim/tests/run_test_case_calibration.py -r $indir/$huc -d $subdir -t $huc"_ble" -b "mannings_calibration_new"/$strorder/$mannings_value

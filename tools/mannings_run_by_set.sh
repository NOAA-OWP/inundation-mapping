#!/bin/bash -e

param_set="$1"
IFS=',' read -r -a array <<< $param_set

strorder="${array[1]}"
mannings_row=1+"$strorder"
mannings_value="${array[$mannings_row]}"

subdir=$outdir/$huc"_"$strorder"_"$mannings_value
mkdir -p $subdir

$srcDir/add_crosswalk.py -d $hucdir/gw_catchments_reaches_filtered_addedAttributes.gpkg -a $hucdir/demDerived_reaches_split_filtered.gpkg -s $hucdir/src_base.csv -l $subdir/gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg -f $subdir/demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg -r $subdir/src_full_crosswalked.csv -j $subdir/src.json -x $subdir/crosswalk_table.csv -t $subdir/hydroTable.csv -w $hucdir/wbd8_clp.gpkg -b $hucdir/nwm_subset_streams.gpkg -y $hucdir/nwm_catchments_proj_subset.tif -m $param_set -z $input_NWM_Catchments -p FR -c

python3 foss_fim/tests/run_test_case_calibration.py -r $fimdir/$huc -d $subdir -t $huc"_ble" -b "mannings_calibration"/$strorder/$mannings_value

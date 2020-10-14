#!/bin/bash -e

param_set="$1"
hucdir="/data/outputs/"$indir/$huc
IFS=',' read -r -a array <<< $param_set

iter="${array[0]}" # needs to be mannings n instead of iteration
strorder="${array[1]}"
subdir=$paramfolder/$outdir/$huc"_"$strorder"_"$iter
mkdir -p $subdir

$libDir/add_crosswalk.py -d $hucdir/gw_catchments_reaches_filtered_addedAttributes.gpkg -a $hucdir/demDerived_reaches_split_filtered.gpkg -s $hucdir/src_base.csv -u $hucdir/majority.geojson -l $subdir/gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg -f $subdir/demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg -r $subdir/src_full_crosswalked.csv -j $subdir/src.json -x $subdir/crosswalk_table.csv -t $subdir/hydroTable.csv -h $hucdir/wbd8_clp.gpkg -b $hucdir/nwm_subset_streams.gpkg -m $paramfolder/mannings_template.json -c

python3 foss_fim/tests/run_test_case_calibration.py -r $indir/$huc -d $subdir -t $huc"_ble" -b "mannings_calibration"/$strorder/$iter

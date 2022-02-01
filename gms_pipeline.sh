#!/bin/bash -e

#################################
#### Sample commands for GMS ####
#################################

# produce FIM 3 commands for preprocessing as normal
# as a comment, we should look into consolidating these three steps below into one

## acquire nhd and nwm inputs then prepare them
# acquire_and_preprocess_inputs.py

## reproject rasters and convert to meters
# preprocess_rasters.py

## aggregate nhd vectors
# aggregate_vectors.py


## GMS ##

## produce gms hydrofabric at unit level first 
gms_run_unit.sh -u 11140105 -n test_output -c /foss_fim/config/params_template.env -j 1 -d /foss_fim/config/deny_gms_unit_default.lst

## produce level path or branch level datasets
gms_run_branch.sh -n test_output -u 11140105 -c /foss_fim/config/params_template.env -j 7 -d /foss_fim/config/deny_gms_branches_default.lst


## Do either FIM or Test case information below ## 

## FIM ##

# make sure you have an outputs directory for FIMs setup first
# using /data/temp here for demo

# produce level path scale FIM
/foss_fim/tools/gms_tools/inundate_gms.py -y /data/outputs/test_output -u 11140105 -f /data/test_cases/ble_test_cases/validation_data_ble/11140105/100yr/ble_huc_11140105_flows_100yr.csv -w 7 -i /data/temp/11140105_inundation_test.tif -o /data/temp/11140105_inundation_test.csv -v

# mosaic level path FIM
/foss_fim/tools/gms_tools/mosaic_inundation.py -i /data/temp/11140105_inundation_test.csv -a /data/outputs/test_output/11140105/wbd.gpkg -v -t inundation_rasters -m /data/temp/11140105_inundation_test.tif

## Test Cases ##

# singular test case
/foss_fim/tools/run_test_case.py -r test_output/11140105 -b test_output -t 11140105_ble -e GMS -m HUC -w 7 -v -vg

# batch of test cases
# synthesize_test_cases.py
/foss_fim/tools/synthesize_test_cases.py -e GMS -jh 2 -jb 3 -b ble -vr -v test_output -s your_string

# aggregate results from synthesize test cases
# will fill in more here later
/foss_fim/tools/consolidate_metrics.py -v test_output <a FR outputs dir> <a MS outputs dir> -o /data/temp/metrics.csv
# if only one of each is provided above (FR, MS, GMS) then you can pass -i to impute FR metrics in HUCs with missing MSoutputs

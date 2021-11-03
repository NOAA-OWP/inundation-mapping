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

# produce level path scale FIM
# inundate_gms.py

# mosaic level path FIM
# mosaic_inundation.py


## Test Cases ##

# singular test case
# run_test_case.py

# batch of test cases
# synthesize_test_cases.py

# aggregate results from synthesize test cases


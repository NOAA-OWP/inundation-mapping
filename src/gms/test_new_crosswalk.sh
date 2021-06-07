#!/bin/bash -e


python3 /foss_fim/src/gms/crosswalk_nwm_demDerived.py -n /data/outputs/first_batch_test_FR_c/12090301/nwm_subset_streams.gpkg -d /data/outputs/first_batch_test_FR_c/12090301/demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg -v -c /data/temp/continuity/cross_walk_$1.csv -e /data/temp/continuity/demDerived_crosswalked_$1.gpkg -m /data/temp/continuity/nwm_traversal_$1.gpkg -w /data/temp/continuity/wbd.gpkg -a $1

#!/bin/bash -e

for d in $(ls -d /data/outputs/3dep_test_*_1m_GMS_*of2/**/branches/*);do 
    huc8=${d:24:8}
    huc12=${d:45:12}
    branch=$(basename $d)
    version=${d:40:1}
    
    /foss_fim/src/gms/outputs_cleanup.py -d /data/outputs/3dep_test_"$huc8"_1m_GMS_"$version"of2/"$huc12"/branches/"$branch" -l /foss_fim/config/deny_gms_branches_default.lst -v -b
done

#!/bin/bash -e

# Universal
b_arg=$tempCurrentBranchDataDir/nwm_subset_streams_levelPaths_$current_branch_id.gpkg
z_arg=$tempCurrentBranchDataDir/nwm_catchments_proj_subset_levelPaths_$current_branch_id.gpkg
slope_min=0 ## TODO doublecheck this number

# HUC- and branch- specific
tempCurrentBranchDataDir=branch_outlet_backpools/fim_runs/control_13080002_copy
hucNumber=13080002
current_branch_id=6077000088 ## TODO: doublecheck this number

## CATCH AND MITIGATE BRANCH OUTLET BACKPOOL ERROR ##
echo -e $startDiv"Catching and mitigating branch outlet backpool issue $hucNumber $current_branch_id"
date -u
# Tstart
$srcDir/mitigate_branch_outlet_backpool.py \
    -b $tempCurrentBranchDataDir \
    -cp $tempCurrentBranchDataDir/gw_catchments_pixels_$current_branch_id.tif \
    -cpp $tempCurrentBranchDataDir/TEMP_TESTING_gw_catchments_pixels_$current_branch_id.gpkg \ 
    -cr $tempCurrentBranchDataDir/gw_catchments_reaches_$current_branch_id.tif \
    -s $tempCurrentBranchDataDir/demDerived_reaches_split_$current_branch_id.gpkg \
    -p $tempCurrentBranchDataDir/demDerived_reaches_split_points_$current_branch_id.gpkg \
    -n $b_arg \
    -d $tempCurrentBranchDataDir/dem_thalwegCond_$current_branch_id.tif \
    -t $slope_min \
    -cs True
# Tcount


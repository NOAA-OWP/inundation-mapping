#!/bin/bash

source ../config/test2.env

echo -e "\n""RichDEM and D8""\n"
for i in 1 1 2 2 4 4 8 8 16 16;
do
    echo -e "\n""Number of cores: "$i"\n"
    time $libDir/fill_and_resolve_flats.py $outputDataDir/dem_thalwegCond.tif dem_thalwegCond_filled.tif
    time mpiexec -n $i $taudemDir/d8flowdir -fel dem_thalwegCond_filled.tif -p flowdir_d8_thalwegCond_filled.tif -sd8 slopes_d8.tif
done

echo -e "\n""PitRemove and D8""\n"
for i in 1 1 2 2 4 4 8 8 16 16;
do
    echo -e "\n""Number of cores: "$i"\n"
    time mpiexec -n $i $taudemDir/pitremove -z $outputDataDir/dem_thalwegCond.tif -fel dem_thalwegCond_filled.tif
    time mpiexec -n $i $taudemDir/d8flowdir -fel dem_thalwegCond_filled.tif -p flowdir_d8_thalwegCond_filled.tif -sd8 slopes_d8.tif
done

echo -e "\n""GageWatershed""\n"
for i in 1 1 2 2 4 4 8 8 16 16;
do
    echo -e "\n""Number of cores: "$i"\n"
    time mpiexec -n $i $taudemDir/gagewatershed -p flowdir_d8_thalwegCond_filled.tif -gw gw_catchments_reaches.tif -o $outputDataDir/flows_points_reachid.gpkg -id idFile.txt
    time mpiexec -n $i $taudemDir/gagewatershed -p flowdir_d8_thalwegCond_filled.tif -gw gw_catchments_pixels.tif -o $outputDataDir/flows_points_pixels.gpkg -id idFile.txt
done

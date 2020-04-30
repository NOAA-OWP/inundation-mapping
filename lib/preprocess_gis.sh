#!/bin/bash

# usgsProjection='+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=37.5 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m no_defs'

# works best
usgsProjection="+proj=aea +datum=NAD83 +x_0=0.0 +y_0=0.0 +lon_0=96dW +lat_0=23dN +lat_1=29d30'N +lat_2=45d30'N +towgs84=-0.9956000824677655,1.901299877314078,0.5215002840524426,0.02591500053005733,0.009425998542707753,0.01159900118427752,-0.00062000005129903 +no_defs +units=m"
nwmDir='/home/fernandoa/projects/foss_fim/data/nwm'
outputDataDir='/home/fernandoa/projects/foss_fim/data/test2/inputs'

NHD_HUC_downloadLink='https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Hydrography/NHDPlusHR/Beta/GDB/'
NHD_HUC_raster_prefix='NHDPLUS_H_'
NHD_HUC_raster_postfix='_HU4_RASTER.7z'
# NHD_HUC_gdb='NHDPLUS_H_0102_HU4_GDB.zip'
huc4_download_list="1207 1209 1210"

NHD_PLUS_ALL='https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHD/National/HighResolution/GDB/NHD_H_National_GDB.zip'
OPTION_2='https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHD/National/HighResolution/GDB/NATIONAL_NHD_GDB.zip'
NHD_PLUS_WBD_ALL='https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/WBD/National/GDB/WBD_National_GDB.zip'

# download
# wget -c -nc .....
# https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Hydrography/NHDPlusHR/Beta/GDB/


# extract
# for i in ./*; do 7z x $i ; done
# 7z e NHDPLUS_H_1207_HU4_RASTER.7z HRNHDPlusRasters1207/elev_cm.tif

# ~% FILE="example.tar.gz"
#
# ~% echo "${FILE%%.*}"
# example
#
# ~% echo "${FILE%.*}"
# example.tar
#
# ~% echo "${FILE#*.}"
# tar.gz
#
# ~% echo "${FILE##*.}"
# gz

# basename /path/to/dir/filename.txt
# filename.txt

# build vrt mosaic
gdalbuildvrt -overwrite -a_srs "$usgsProjection" -r bilinear mosaic.vrt  HRNHDPlusRasters1207/elev_cm.tif HRNHDPlusRasters1209/elev_cm.tif HRNHDPlusRasters1210/elev_cm.tif

# convert mosaic to GeoTIFF
# gdal_translate -of "GTiff" -stats -co "COMPRESS=LZW" -co "TILED=YES" -co "BIGTIFF=YES" -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" mosaic.vrt mosaic.tif

# project nwm flows
ogr2ogr -overwrite -progress -f GPKG $nwmDir/nwm_flows.gpkg $nwmDir/RouteLink_FL_2019_05_09.shp -nln nwm_flowlines
ogr2ogr -overwrite -progress -f GPKG -t_srs "$usgsProjection" $nwmDir/nwm_flows_proj.gpkg $nwmDir/nwm_flows.gpkg

# project nwm headwater nodes
ogr2ogr -overwrite -progress -f GPKG $nwmDir/nwm_headwaters.gpkg $nwmDir/Headwater_Nodes_20190509.shp -nln nwm_headwaters
ogr2ogr -overwrite -progress -f GPKG -t_srs "$usgsProjection" $nwmDir/nwm_headwaters_proj.gpkg $nwmDir/nwm_headwaters.gpkg

# project nwm lakes
ogr2ogr -overwrite -progress -f GPKG $nwmDir/nwm_lakes.gpkg $nwmDir/OutputLakes2_20190509.shp -nln nwm_lakes
ogr2ogr -overwrite -progress -f GPKG -t_srs "$usgsProjection" $nwmDir/nwm_lakes_proj.gpkg $nwmDir/nwm_lakes.gpkg

# project catchments
ogr2ogr -overwrite -progress -f GPKG $nwmDir/nwm_catchments.gpkg $nwmDir/NWMCatchment.shp -nln nwm_catchments
ogr2ogr -overwrite -progress -f GPKG -t_srs "$usgsProjection" $nwmDir/nwm_catchments_proj.gpkg $nwmDir/nwm_catchments.gpkg

# add HUC6 code to NWM flows
# py3 get_nwm_flows_by_huc6.py $nwmDir/channel_props/ $nwmDir/nwm_flows_proj.gpkg $nwmDir/nwm_flows_proj_huc6.gpkg

# extract geodatabase
ogr2ogr -overwrite -progress -f GPKG -t_srs "$usgsProjection" $outputDataDir/WBDHU6_1209.gpkg $outputDataDir/NHDPLUS_H_1209_HU4_GDB.gdb WBDHU6
ogr2ogr -overwrite -progress -f GPKG -t_srs "$usgsProjection" $outputDataDir/NHDPlusBurnLineEvent_1209.gpkg $outputDataDir/NHDPLUS_H_1209_HU4_GDB.gdb NHDPlusBurnLineEvent
ogr2ogr -overwrite -progress -f GPKG -t_srs "$usgsProjection" $outputDataDir/NHDPlusFlowlineVAA_1209.gpkg $outputDataDir/NHDPLUS_H_1209_HU4_GDB.gdb NHDPlusFlowlineVAA

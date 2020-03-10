#!/bin/bash

# usgsProjection='+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=37.5 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m no_defs'

# works best
usgsProjection="+proj=aea +datum=NAD83 +x_0=0.0 +y_0=0.0 +lon_0=96dW +lat_0=23dN +lat_1=29d30'N +lat_2=45d30'N +towgs84=-0.9956000824677655,1.901299877314078,0.5215002840524426,0.02591500053005733,0.009425998542707753,0.01159900118427752,-0.00062000005129903 +no_defs"
nwmDir='/home/fernandoa/projects/foss_fim/data/nwm'

# download
# wget -c .....

# extract
# for i in ./*; do 7z x $i ; done

# build vrt mosaic
gdalbuildvrt -overwrite -a_srs "$usgsProjection" -r bilinear mosaic.vrt  HRNHDPlusRasters1207/elev_cm.tif HRNHDPlusRasters1209/elev_cm.tif HRNHDPlusRasters1210/elev_cm.tif

# convert mosaic to GeoTIFF
# gdal_translate -of "GTiff" -stats -co "COMPRESS=LZW" -co "TILED=YES" -co "BIGTIFF=YES" -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" mosaic.vrt mosaic.tif

# testing
# gdal_calc.py -A mosaic.vrt --type=Float32 --overwrite --calc="A/1000" --outfile=mosaic_meters.tif --co "COMPRESS=LZW" --co "TILED=YES" --co "BIGTIFF=YES" --co "BLOCKXSIZE=512" --co "BLOCKYSIZE=512"

# project flows
ogr2ogr -progress -f GPKG -t_srs "$usgsProjection" $nwmDir/nwm_flows_proj.gpkg $nwmDir/RouteLink_FL_2019_05_09.shp -nln nwm_flowlines

# project headwater nodes
ogr2ogr -progress -f GPKG -t_srs "$usgsProjection" $nwmDir/nwm_headwaters_proj.gpkg $nwmDir/Headwater_Nodes_20190509.shp -nln nwm_headwaters

# project lakes
ogr2ogr -progress -f GPKG -t_srs "$usgsProjection" $nwmDir/nwm_lakes_proj.gpkg -nln nwm_lakes

# add HUC6 code to NWM flows
py3 get_nwm_flows_by_huc6.py $nwmDir/channel_props/ $nwmDir/nwm_flows_proj.gpkg $nwmDir/nwm_flows_proj_huc6.gpkg

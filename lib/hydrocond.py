# -*- coding: utf-8 -*-
"""
Created on Fri Sep 25 14:43:43 2020

@author: trevor.grout
"""

import rasterio

#In this section we import the flows grid boolean raster which denotes river raster (river = 1, no river = 0), we flip this raster (river = 0 and no river =1) to perform later calclations.
flows_grid_boolean = r'D:\deleteme\AGREE_test\flows_grid_boolean.tif'
with rasterio.open(flows_grid_boolean) as rivers:
    rivers_profile = rivers.profile
    rivers_mask = rivers.read(1)

#Flip flows grid boolean raster
rivers_mask_flip = (~rivers_mask.astype('bool')).astype('uint8')

#Write out raster to file.       
outpath = r'D:\deleteme\AGREE_test\rivers_mask_flip.tif'
with rasterio.Env():
    with rasterio.open(outpath, 'w', **rivers_profile) as raster:
        raster.write(rivers_mask_flip.astype('int32'),1)
###############################################################################
#Import the dem and raise by 1000m everywhere EXCEPT in the stream channel (*Re-evaluate this as the stream network does not extend to the edge of the raster so the stream is being filled to the downstream elevation anyways)
#One alternative may be to split up raster (river vs non-river) and do a fill only on the riverine cells (using original elevation from raster) then doing a fill on remaining raster. Then overlay the flow direction of the riverine cells onto the flow direction from the remaining raster???????
#To separate try
        #dem_m * rivers_mask_flip = non-riverine cells
        #dem_m * rivers_mask = riverine cells
dem_m = r'D:\deleteme\FBTest\zz_test_level_path_patch\120903\dem_meters.tif'
with rasterio.open(dem_m) as temp:
    dem_m_profile = temp.profile
    dem_m = temp.read(1)
raise_dem = dem_m + (rivers_mask_flip * 1000)

#Write out raster to file.       
outpath = r'D:\deleteme\AGREE_test\raise_dem.tif'
with rasterio.Env():
    with rasterio.open(outpath, 'w', **dem_m_profile) as raster:
        raster.write(raise_dem,1)
###############################################################################
#Get proximity raster define it so that all areas outside max distance is equal to the max distance value.
#Example command: python3 -m gdal_proximity -srcband 1 -distunits GEO -values 1 -maxdist 50.0 -nodata 50.0 -ot Float32 -of GTiff D:/deleteme/AGREE_test/flows_grid_boolean.tif D:/deleteme/AGREE_test/proximity_v2.tif
#Probably best to but this in run_test_case initially? 


max_dist = 50 #This is the max search distance from gdal proximity
smooth_drop = 10 #This is the denominator adjustment factor. Proximity/smooth_drop or the maximum elevation drop (in m). 50m/10 = 5m drop immediately near channel.
#This section accomplishes a poor-man's smooth drop similar to AGREE DEM. It is inspired by the USDA Inverse Distance Drainage Enforcement Algorithm. Instead of continually lowering elevations out to infinity, it will only lower them up to the maximum search distance. Additionally, it only lower (or condition) elevations that are not immediately on the river centerline (as those should be treated a little differently)
proximity_raster = r'D:\deleteme\AGREE_test\proximity_v2.tif'
with rasterio.open(proximity_raster) as prox:
    prox_profile = prox.profile
    prox_data = prox.read(1)
#Flip the proximity raster so that the channel centerline has values of 50 (or whatever the max search distance is) and areas away from the channel gradually go to 0.
flip_proxy = abs(max_dist - prox_data)

#Write out raster to file.       
outpath = r'D:\deleteme\AGREE_test\flip_proximity.tif'
with rasterio.Env():
    with rasterio.open(outpath, 'w', **prox_profile) as raster:
        raster.write(flip_proxy,1)

###############################################################################
#This actually accomplishes the hydroconditioning where elevations are tapered downward by a calculated magnitude as one moves closer to the channel.

raise_dem_cond_smooth = raise_dem - (flip_proxy/smooth_drop)*rivers_mask_flip

#Write out raster to file.       
outpath = r'D:\deleteme\AGREE_test\dem_for_fdr_only.tif'
with rasterio.Env():
    with rasterio.open(outpath, 'w', **dem_m_profile) as raster:
        raster.write(raise_dem_cond_smooth,1)


#as mentioned prior maybe doing a fill on the entire flow conditioned raster (raise_dem_cond_smooth) then doing a fill strictly on the river network. Then doing a flow direction on both and then merging the two. If we do this route, raising the DEM may not be necessary (probably better not to?)??? Maybe we just need to drop the channel 1000m do a fill then flow direction, then do a fill on the channel dem_m elevations and then do a flow direction. This will preserve slope/flow directions in the channel.



#!/usr/bin/env python3

import rasterio
import argparse

def hydrocond(flows_grid_boolean, dem_m, proximity, smooth_drop_adj_factor, sharp_drop, hydrocond_tif):

    #Import raster data
    with rasterio.open(flows_grid_boolean) as rivers:
        river_cells = rivers.read(1)
    
    with rasterio.open(dem_m) as dem:
        dem_m_profile = dem.profile
        dem_m = dem.read(1)
        
    with rasterio.open(proximity) as prox:
        prox_data = prox.read(1)
    
    #Flip flows grid boolean raster (river cell = 0 and non-river cell = 1)
    non_river_cells = (~river_cells.astype('bool')).astype('uint8')
    
    #Flip the proximity raster so that the channel centerline has maximum values and decreasing outward. 
    max_prox_value = prox_data.max()
    flip_proximity = abs(max_prox_value - prox_data)
    
    #Hydrocondition in 2 steps, first implement the shallow drop in non-river cells based on the proximity raster. Then burn in the steep drop in river cells.
    dem_cond_smooth = dem_m - (flip_proximity/smooth_drop_adj_factor)*non_river_cells
    dem_cond_smooth_sharp = dem_cond_smooth - sharp_drop*river_cells
 
    with rasterio.Env():
        dem_dtype = dem_m_profile['dtype']
        with rasterio.open(hydrocond_tif, 'w', **dem_m_profile) as raster:
            raster.write(dem_cond_smooth_sharp.astype(dem_dtype),1)

if __name__ == '__main__':
    #Parse arguments
    parser = argparse.ArgumentParser(description = 'Hydrocondition DEM')
    parser.add_argument('-b', '--flows-grid-boolean', help = 'flows grid boolean layer', required = True)
    parser.add_argument('-d', '--dem-m',  help = 'DEM_m raster', required = True)
    parser.add_argument('-p', '--proximity', help = 'proximity raster', required = True)
    parser.add_argument('-sm', '--smooth-drop-adj-factor', help = 'Smooth drop adjustment factor', required = False, type = float, default = 10.0)
    parser.add_argument('-sh', '--sharp-drop', help = 'sharp drop (m)', required = False, type = float, default = 1000.0)
    parser.add_argument('-o',  '--hydrocond-tif', help = 'Output hydroconditioned raster', required = True)

    #Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    # rename variable inputs
    flows_grid_boolean = args['flows-grid-boolean']
    dem_m = args['dem-m']
    proximity = args['proximity']
    smooth_drop_adj_factor = args['smooth-drop-adj-factor']
    sharp_drop = args['sharp-drop']
    hydrocond_tif = args['hydrocond-tif']

    #Run hydrocond
    hydrocond(flows_grid_boolean, dem_m, proximity, smooth_drop_adj_factor, sharp_drop, hydrocond_tif)


# import os

# workingdir = r'D:\deleteme\dev-alt-hydrocond\120903'
# flows_grid_boolean = os.path.join(workingdir,'flows_grid_boolean.tif')
# dem_m = os.path.join(workingdir, 'dem_meters.tif')
# proximity = os.path.join(workingdir, 'stream_proximity.tif')
# smooth_drop_adj_factor = 10.0
# sharp_drop = 1000.0
# hydrocond_tif = os.path.join(workingdir, "test_output.tif")










# flows_grid_boolean = r'D:\deleteme\AGREE_test\flows_grid_boolean.tif'
# dem_m = r'D:\deleteme\FBTest\zz_test_level_path_patch\120903\dem_meters.tif'
# proximity_raster = r'D:\deleteme\AGREE_test\proximity_v2.tif'
# smooth_drop_adj_factor = 10
# sharp_drop = 1000
# outpath = r'D:\deleteme\AGREE_test\n.tif'
# hydrocond(flows_grid_boolean, dem_m, proximity_raster, smooth_drop_adj_factor, sharp_drop, outpath)







###############################################################################
# #In this section we import the flows grid boolean raster which denotes river raster (river = 1, no river = 0), we flip this raster (river = 0 and no river =1) to perform later calclations.
# flows_grid_boolean = r'D:\deleteme\AGREE_test\flows_grid_boolean.tif'
# with rasterio.open(flows_grid_boolean) as rivers:
#     rivers_profile = rivers.profile
#     rivers_mask = rivers.read(1)

# #Flip flows grid boolean raster
# rivers_mask_flip = (~rivers_mask.astype('bool')).astype('uint8')

# #Write out raster to file.       
# outpath = r'D:\deleteme\AGREE_test\rivers_mask_flip.tif'
# with rasterio.Env():
#     with rasterio.open(outpath, 'w', **rivers_profile) as raster:
#         raster.write(rivers_mask_flip.astype('int32'),1)
# ###############################################################################
# dem_m = r'D:\deleteme\FBTest\zz_test_level_path_patch\120903\dem_meters.tif'
# with rasterio.open(dem_m) as temp:
#     dem_m_profile = temp.profile
#     dem_m = temp.read(1)
# ###############################################################################
# #Get proximity raster define it so that all areas outside max distance is equal to the max distance value.
# #Example command: python3 -m gdal_proximity -srcband 1 -distunits GEO -values 1 -maxdist 50.0 -nodata 50.0 -ot Float32 -of GTiff D:/deleteme/AGREE_test/flows_grid_boolean.tif D:/deleteme/AGREE_test/proximity_v2.tif
# #Probably best to but this in run_test_case initially? 


# max_dist = 50 #This is the max search distance from gdal proximity
# smooth_drop = 10 #This is the denominator adjustment factor. Proximity/smooth_drop or the maximum elevation drop (in m). 50m/10 = 5m drop immediately near channel.
# sharp_drop = 1000 #This is the drop that the channel centerline is burned in.

# #This section accomplishes a poor-man's smooth drop similar to AGREE DEM. It is inspired by the USDA Inverse Distance Drainage Enforcement Algorithm. Instead of continually lowering elevations out to infinity, it will only lower them up to the maximum search distance. Additionally, it only lower (or condition) elevations that are not immediately on the river centerline (as those should be treated a little differently)
# proximity_raster = r'D:\deleteme\AGREE_test\proximity_v2.tif'
# with rasterio.open(proximity_raster) as prox:
#     prox_profile = prox.profile
#     prox_data = prox.read(1)
# #Flip the proximity raster so that the channel centerline has values of 50 (or whatever the max search distance is) and areas away from the channel gradually go to 0.
# flip_proximity = abs(max_dist - prox_data)

# #Write out raster to file.       
# outpath = r'D:\deleteme\AGREE_test\flip_proximity.tif'
# with rasterio.Env():
#     with rasterio.open(outpath, 'w', **prox_profile) as raster:
#         raster.write(flip_proximity,1)

# ###############################################################################
# #This actually accomplishes the hydroconditioning where elevations are tapered downward by a calculated magnitude as one moves closer to the channel.

# dem_cond_smooth = dem_m - (flip_proximity/smooth_drop)*rivers_mask_flip
# dem_cond_smooth_sharp = dem_cond_smooth - (sharp_drop*rivers_mask)

# #Write out raster to file.       
# outpath = r'D:\deleteme\AGREE_test\dem_for_fdr_only.tif'
# with rasterio.Env():
#     with rasterio.open(outpath, 'w', **dem_m_profile) as raster:
#         raster.write(dem_cond_smooth_sharp.astype('float32'),1)

# rivers_only = dem_m * rivers_mask
# rivers_only_profile = dem_m_profile.copy()
# rivers_only_profile.update(nodata = 0)
# outpath = r'D:\deleteme\AGREE_test\rivers_only_dem.tif'
# with rasterio.Env():
#     with rasterio.open(outpath, 'w', **rivers_only_profile) as raster:
#         raster.write(rivers_only.astype('float32'),1)



#Import the dem and raise by 1000m everywhere EXCEPT in the stream channel (*Re-evaluate this as the stream network does not extend to the edge of the raster so the stream is being filled to the downstream elevation anyways)
#Graveyard (potentially resuscitate)
#raise_dem = dem_m + (rivers_mask_flip * 1000)

# #Write out raster to file.       
# outpath = r'D:\deleteme\AGREE_test\raise_dem.tif'
# with rasterio.Env():
#     with rasterio.open(outpath, 'w', **dem_m_profile) as raster:
#         raster.write(raise_dem,1)
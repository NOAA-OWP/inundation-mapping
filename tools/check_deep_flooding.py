import argparse
import os
from multiprocessing import Pool
import numpy as np
import rasterio.shutil
from rasterio.warp import calculate_default_transform, reproject, Resampling
import rasterio.crs
import rasterio
import rasterio.mask
import geopandas as gpd
from shapely.geometry import box


def check_deep_flooding(args):
    
    depth_grid_path = args[0]
    shapefile_path = args[1]
    depth_threshold = args[2]
    output_dir = args[3]
    
    print("Checking " + depth_grid_path + "...")
    
    # Open depth_grid_path and shapefile_path and perform np.wheres
    depth_src = rasterio.open(depth_grid_path)
    depth_array = depth_src.read(1)
    reference = depth_src
    
    #Read layer using the bbox option. CRS mismatches are handled if bbox is passed a geodataframe (which it is).
    bounding_box = gpd.GeoDataFrame({'geometry': box(*reference.bounds)}, index=[0], crs=reference.crs)
    poly_all = gpd.read_file(shapefile_path, bbox = bounding_box)

    # Make sure features are present in bounding box area before projecting. Continue to next layer if features are absent.
    if poly_all.empty:
        return

    #Project layer to reference crs.
    poly_all_proj = poly_all.to_crs(reference.crs)
    # check if there are any lakes within our reference raster extent.
    if poly_all_proj.empty:
        #If no features within reference raster extent, create a zero array of same shape as reference raster.
        poly_mask = np.zeros(reference.shape)
    else:
        #Perform mask operation on the reference raster and using the previously declared geometry geoseries. Invert set to true as we want areas outside of poly areas to be False and areas inside poly areas to be True.
        geometry = poly_all_proj.geometry
        in_poly,transform,c = rasterio.mask.raster_geometry_mask(reference, geometry, invert = True)
        #Write mask array, areas inside polys are set to 1 and areas outside poly are set to 0.
        poly_mask = np.where(in_poly == True, 1,0)

        # Filter depth_array by depth_threshold
        filtered_depth_array = np.where(depth_array > depth_threshold, depth_array, -1)

        # Perform mask.
        masked_depth_array = np.where(poly_mask == 1, filtered_depth_array, -1)

        if np.amax(masked_depth_array) > 0:

            file_handle = os.path.split(depth_grid_path)[1]
    
            checked_depth_raster = os.path.join(output_dir, "checked_" + str(depth_threshold) + "_" + file_handle)
    
            print("Writing " + checked_depth_raster + "...")
            # Write output.
            with rasterio.Env():
                profile = depth_src.profile
                profile.update(nodata=-1)
                with rasterio.open(checked_depth_raster, 'w', **profile) as dst:
                    dst.write(masked_depth_array, 1)


if __name__ == '__main__':
    
    # Parse arguments.
    parser = argparse.ArgumentParser(description='Checks for deep flooding in a specified shapefile. Requires a directory of depth grids and a shapefile.')
    parser.add_argument('-d','--depth-grid-dir',help='Name of directory containing outputs of depth outputs of inundation.py',required=True)
    parser.add_argument('-s','--shapefile-path',help='Path to shapefile to be used as the overlay.',required=True)
    parser.add_argument('-t','--depth-threshold',help='Depth in meters to use as checking threshold.',required=True)
    parser.add_argument('-o', '--output-dir',help='The path to a directory to write the outputs. If not used, the inundation_review directory is used by default -> type=str',required=True, default="")
    parser.add_argument('-j', '--job-number',help='The number of jobs',required=False,default=1)
        
    args = vars(parser.parse_args())
    
    depth_grid_dir = args['depth_grid_dir']
    shapefile_path = args['shapefile_path']
    depth_threshold = int(args['depth_threshold'])
    output_dir = args['output_dir']
    job_number = int(args['job_number'])
    
    # Get list of files in depth_grid_dir.
    # Loop through files and determine which ones are depth grids, adding them to a list.
    
    depth_grid_dir_list = os.listdir(depth_grid_dir)
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    
    procs_list = []
    
    for f in depth_grid_dir_list:
        if 'depth' in f:
            full_f_path = os.path.join(depth_grid_dir, f)
            
#            check_deep_flooding([full_f_path, shapefile_path, depth_threshold, output_dir])
            procs_list.append([full_f_path, shapefile_path, depth_threshold, output_dir])
            
    # Multiprocess.
    with Pool(processes=job_number) as pool:
        pool.map(check_deep_flooding, procs_list)

            
            
            
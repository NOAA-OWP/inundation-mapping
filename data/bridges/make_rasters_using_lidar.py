import argparse
from datetime import datetime, timezone
import laspy
import numpy as np
import pdal
import json
import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd
import os
import glob
from pathlib import Path
from shapely.geometry import Point,MultiPoint    
from scipy.spatial import KDTree
import xarray as xr
from multiprocessing import Pool
import utils.shared_functions as sf
from utils.shared_functions import FIM_Helpers as fh
import logging
import traceback
import sys

def download_lidar_points(args):
    osmid,poly_geo, lidar_url,output_dir=args
    poly_wkt=poly_geo.wkt
    las_file_path=os.path.join(output_dir,'point_files','%s.las'%str(osmid))

    my_pipe={
    "pipeline": [
        {
            "polygon": str(poly_wkt),
            "filename": lidar_url,
            "type": "readers.ept",
            "tag": "readdata"
        },
        {
        "type": "filters.returns",
        "groups": "last,only"  #need both last and only because 'last' applies only when there are multiple returns and does not include cases with a single return. 
        },

        # {
        #     "in_srs":'EPSG:3857',
        #     "out_srs": 'EPSG:%d'%tif_crs,
        #     "type": "filters.reprojection",
        #     "tag": "reprojected",
        # },

        {
        "filename": las_file_path ,
        "tag": "writerslas",
        "type": "writers.las"
        }
            ]
        }

    # Create a PDAL pipeline object
    pipeline = pdal.Pipeline(json.dumps(my_pipe))

    # Execute the pipeline
    pipeline.execute()



def las_to_gpkg(las_path):
    las=laspy.read(las_path)

    #make x,y coordinates
    x_y = np.vstack((np.array(las.x), np.array(las.y))).transpose()

    #convert the coordinates to a list of shapely Points
    las_points = list(MultiPoint(x_y).geoms)

    #put the points in a GeoDataFrame for a more standard syntax through Geopandas
    points_gdf= gpd.GeoDataFrame(geometry=las_points, crs="epsg:3857")


    #add other required data into gdf...here only elevation
    z_values=np.array(las.z)
    points_gdf['z']=z_values

    return_values=np.array(las.return_number)
    points_gdf['return']=return_values

    class_values=np.array(las.classification)
    points_gdf['classification']=class_values

    number_of_returns = np.array(las.number_of_returns)
    points_gdf['number_of_returns'] = number_of_returns
    
    #note that pdal uses below scenarios of 'return number' and 'number_of_return' as described in documentauon below to identify, first, and last returns ...
    # There is no other way to do this classifcation task. Note that x,y of different returns of a specifc pulse can be in different locations. so you should not 
    # expect to have multiple points at a exact x, y with different return numbers. 
    #  Also, 'point_source_id' are usually not reliable.  So, the only way to assign return numbers is by comparing return number and 'number of returns' as pdal is doing:
    #https://pdal.io/en/latest/stages/filters.returns.html 

    return points_gdf


def handle_noises(points_gdf):
    # Replace non-bridge point values with the average of the nearest two bridge points (with classification codes 13 or 17_).

    points_gdf.loc[:,'x'] = points_gdf.geometry.x
    points_gdf.loc[:,'y'] = points_gdf.geometry.y

    #save the original z
    points_gdf.loc[:, 'origi_z'] = points_gdf['z'].values


    noise_bool=  ~points_gdf['classification'].isin([17,13]) 
    points_gdf.loc[:, 'noise'] = np.where(noise_bool, 'y', 'n')

    non_noises=points_gdf[points_gdf['noise']=='n']
    noises=points_gdf[(points_gdf['noise']=='y')]
    if len(non_noises)/len(points_gdf)<0.05: #if there is few class 13 and 17
        return None

    # # Create KDTree using x and y coordinates of non-noise points across all classes
    tree = KDTree(non_noises[['x', 'y']])

    # Find the 2 nearest neighbors for each outlier in class 1 based on x, y coordinates
    distances, indices = tree.query(noises[['x', 'y']].values, k=2)

    # Get the z values of the nearest 2 neighbors
    nearest_z_values = non_noises.iloc[indices.flatten()]['z'].values.reshape(indices.shape)
    noises.loc[:,'z'] = np.mean(nearest_z_values, axis=1)


    modified_points_gdf = pd.concat([noises, non_noises])
    return modified_points_gdf


def make_local_tifs(modified_las_path,raster_resolution,tif_crs, tif_path):
    my_pipe={
        "pipeline": [
            {
            "type": "readers.las",
            "filename": modified_las_path, 
            "spatialreference": "EPSG:3857"  # specify the correct coordinate reference system
            },

             { #reproject to 5070 or Alaska crs?
            "in_srs":'EPSG:3857',
            "out_srs": 'EPSG:%d'%tif_crs,
            "type": "filters.reprojection",
             },
            
            {
            "type": "writers.gdal",
            "filename": tif_path,
            "dimension": "Z",
            "output_type": "idw", # or try  "mean",
            "resolution": raster_resolution,
            "nodata": -999,
            "data_type": "float32",
            }
                ]
    }

    # Create a PDAL pipeline object
    pipeline = pdal.Pipeline(json.dumps(my_pipe))

    # Execute the pipeline
    pipeline.execute()

def gpkg_to_las(points_gdf):

    x = points_gdf.geometry.x
    y = points_gdf.geometry.y
    z = points_gdf['z'].values 

    header = laspy.LasHeader()
    las_obj = laspy.LasData(header)
    las_obj.x = x
    las_obj.y = y
    las_obj.z = z
    return las_obj

def make_lidar_footprints():
    str_hobu_footprints = (
        r"https://raw.githubusercontent.com/hobu/usgs-lidar/master/boundaries/boundaries.topojson"
    )
    entwine_footprints_gdf = gpd.read_file(str_hobu_footprints)
    entwine_footprints_gdf.set_crs("epsg:4326", inplace=True)  #it is geographic (lat-long degrees) commonly used for GPS for accurate locations
    #it is important to reproject to 3857 because poly_wkt in process_lidar must be in that crs to properly apply "readers.ept" step.

    entwine_footprints_gdf.to_crs("epsg:3857", inplace=True) 
    return entwine_footprints_gdf


def make_rasters_in_parallel(args):
    osmid,points_path,output_dir,raster_resolution,tif_crs=args
    try:

        # #make a gpkg file from points
        points_gdf=las_to_gpkg(points_path)
        if not points_gdf.empty:
            modified_points_gdf=handle_noises(points_gdf)
            if modified_points_gdf is None:
                return

            #make a las file for subsequent pdal pipeline
            modified_las_path=os.path.join(output_dir,'point_files','%s_modified.las'%osmid)
            las_obj=gpkg_to_las(modified_points_gdf)
            las_obj.write(modified_las_path)
            
            #make tif files
            tif_output=os.path.join(output_dir,'lidar_osm_rasters','%s.tif'%osmid)
            make_local_tifs(modified_las_path,raster_resolution,tif_crs, tif_output)
            os.remove(modified_las_path)
        else:
            logging.info("Not enough valid points available for osmid: %s"%str(osmid))
            print("Not enough valid points available for osmid: %s"%str(osmid))

    except:
        logging.info("something is wrong for osmid: %s"%str(osmid))
        print("something is wrong for osmid: %s"%str(osmid))


def process_bridges_lidar_data(OSM_bridge_file,buffer_width,raster_resolution,output_dir):
    # start time and setup logs
    start_time = datetime.now(timezone.utc)
    fh.print_start_header('Making HUC6 elev difference rasters', start_time)

    __setup_logger(output_dir)
    logging.info(f"Saving results in {output_dir}")

    try:

        # Create subfolders 'point_points' and 'lidar_osm_rasters' inside the moutput folder
        point_dir = os.path.join(output_dir, 'point_files')
        tif_files_dir = os.path.join(output_dir, 'lidar_osm_rasters')

        os.makedirs(point_dir, exist_ok=True)
        os.makedirs(tif_files_dir, exist_ok=True)

        tif_crs=5070 # consider changing for Alaska ?

        #produce footprints of lidar dataset over conus
        text='generating footprints of available CONUS lidar datasets'
        print(text)
        logging.info(text)
        entwine_footprints_gdf=make_lidar_footprints()

        text='read osm bridge lines and make a polygon foortprint'
        print(text)
        logging.info(text)
        OSM_bridge_lines_gdf=gpd.read_file(OSM_bridge_file)
        OSM_polygons_gdf=OSM_bridge_lines_gdf.copy()
        OSM_polygons_gdf['geometry'] = OSM_polygons_gdf['geometry'].buffer(buffer_width)  
        OSM_polygons_gdf.to_crs(entwine_footprints_gdf.crs, inplace=True)
        OSM_polygons_gdf.rename(columns={'name':'bridge_name'}, inplace=True)
            
        #intersect with lidar urls
        text='Identify USGS/Entwine lidar URLs for intersecting with each bridge polygon'
        print(text)
        logging.info(text)
        OSM_polygons_gdf = gpd.overlay(OSM_polygons_gdf, entwine_footprints_gdf, how='intersection')

        OSM_polygons_gdf.to_file(os.path.join(output_dir,'buffered_bridges.gpkg'))

        #filter if there are multiple urls for a bridge, keep the url with highest count
        OSM_polygons_gdf = OSM_polygons_gdf.loc[OSM_polygons_gdf.groupby('osmid')['count'].idxmax()]
        OSM_polygons_gdf = OSM_polygons_gdf.reset_index(drop=True)

        text='download last-return lidar points (with epsg:3857) within each bridge polygon from the identified URLs'
        print(text)
        logging.info(text)
        pool_args=[]
        for i, row in OSM_polygons_gdf.iterrows():
            osmid,poly_geo, lidar_url = row.osmid, row.geometry, row.url
            pool_args.append((osmid,poly_geo, lidar_url,output_dir))

        print('There are %d files to get downloaded'%len(pool_args))
        with Pool(15) as pool:
            pool.map(download_lidar_points, pool_args)

        text='Generate raster files after filtering the points for bridge classification codes'
        print(text)
        logging.info(text)
        downloaded_points_files = glob.glob(os.path.join(output_dir,'point_files', '*.las'))

        pool_args=[]
        for points_path in downloaded_points_files:
            osmid= os.path.basename(points_path).split('.las')[0]
            pool_args.append((osmid,points_path,output_dir,raster_resolution,tif_crs ))

        with Pool(10) as pool:
            pool.map(make_rasters_in_parallel, pool_args)

        # Record run time 
        end_time = datetime.now(timezone.utc)
        tot_run_time = end_time - start_time
        fh.print_end_header('Making osm rasters complete', start_time, end_time)
        logging.info('TOTAL RUN TIME: ' + str(tot_run_time))
        logging.info(fh.print_date_time_duration(start_time, end_time))

    except Exception as ex:
        summary = traceback.StackSummary.extract(traceback.walk_stack(None))
        print(f"*** {ex}")
        print(''.join(summary.format()))
        logging.critical(f"*** {ex}")
        logging.critical(''.join(summary.format()))
        sys.exit(1)

def __setup_logger(output_folder_path):
    start_time = datetime.now(timezone.utc)
    file_dt_string = start_time.strftime("%Y_%m_%d-%H_%M_%S")
    log_file_name = f"osm_lidar_rasters-{file_dt_string}.log"

    log_file_path = os.path.join(output_folder_path, log_file_name)

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    logger = logging.getLogger()
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)

    logging.info(f'Started (UTC): {start_time.strftime("%m/%d/%Y %H:%M:%S")}')
    logging.info("----------------")


if __name__ == "__main__":  

    '''
        Sample usage:
       python create_osm_raster_using_lidar.py
       -i osm_all_bridges.gpkg
       -b 1.5
       -r 3
       -o /results/02050206
    '''

    parser = argparse.ArgumentParser(description='Download lidar points for buffered OSM bridges and make tif files')

    parser.add_argument(
        '-i', '--OSM_bridge_file', help='REQUIRED: A gpkg that contains the bridges lines', required=True
    ) 

    parser.add_argument(
        '-b',
        '--buffer_width',
        help='OPTIONAL: Buffer to apply to OSM bridge lines to select lidar points within the buffered area. Default value is 1.5m (on each side)',
        required=False,
        default=1.5,
        type=float,
    )

    parser.add_argument(
        '-r',
        '--raster_resolution',
        help='OPTIONAL: Resolution of bridge raster files generated from lidar. Default value is 3m',
        required=False,
        default=3.0,
        type=float,
    )

    parser.add_argument(
        '-o',
        '--output_dir',
        help='REQUIRED: folder path where individual HUC8 results will be saved to.'
        ' File names are hardcoded to format hucxxxxxxxx_bridges_lidar.',
        required=True,
    )


    args = vars(parser.parse_args())

    process_bridges_lidar_data(**args)






    








    











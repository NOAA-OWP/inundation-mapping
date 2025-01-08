import argparse
import datetime as dt
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
        "groups": "last,only"  #last mean the last if when there are multiple returns..it does not include cases When there is only one return. so we need both "last" and "only" 
        },

        # {
        #     "in_srs":'EPSG:3857',
        #     "out_srs": 'EPSG:%d'%tif_crs,
        #     "type": "filters.reprojection",
        #     "tag": "reprojected",
        # },

        {
        # "filename": os.path.join(st.session_state['this_bridge_output'] , 'all_last_return_points.las'),
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


def create_dem_diff(original_dem_path,local_tif_paths,diff_dem_path):
    # Step 1: Open the regional TIFF as an xarray DataArray
    original_da = xr.open_dataarray(original_dem_path, engine="rasterio")
    original_nodata=original_da.rio.nodata
    enhanced_da = original_da.copy()

    for i, local_tif_path in enumerate(local_tif_paths): 
        print(local_tif_path)
    
        # Step 2: Open the local TIFF as an xarray DataArray and reproject to match the regional grid, if needed
        local_da = xr.open_dataarray(local_tif_path, engine="rasterio")

        if local_da.rio.crs != original_da.rio.crs:
            local_da = local_da.rio.reproject_match(original_da) 

        # Step 3: Replace values in the regional DataArray with the local DataArray values at overlapping locations
        enhanced_da = enhanced_da.where(local_da.isnull(), other=local_da)

    # # Step 4: Set nodata value to be consistent
    enhanced_da=enhanced_da.fillna(original_nodata)
    enhanced_da.rio.write_nodata(original_nodata, inplace=True)

    # Sif needed, Explicitly assign the CRS from the regional data to the final combined data
    # updated_regional_da.rio.write_crs(regional_da.rio.crs, inplace=True)

    # enhanced_da.rio.to_raster(enhanced_dem_path)

    diff = enhanced_da - original_da

    # Save the result to a new TIFF file
    diff.rio.to_raster(diff_dem_path)

def make_rasers_in_parallel(args):
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
            tif_output=os.path.join(output_dir,'tif_files','%s.tif'%osmid)
            make_local_tifs(modified_las_path,raster_resolution,tif_crs, tif_output)
            os.remove(modified_las_path)
    except:
        print("something wrong with %s"%str(osmid))


def process_bridges_lidar_data(OSM_bridge_file,buffer_width,raster_resolution,output_dir):
    start_time = dt.datetime.now(dt.timezone.utc)

    # Create subfolders 'lidar_points' and 'classifications' inside the main folder
    point_dir = os.path.join(output_dir, 'point_files')
    tif_files_dir = os.path.join(output_dir, 'tif_files')

    os.makedirs(point_dir, exist_ok=True)
    os.makedirs(tif_files_dir, exist_ok=True)

    tif_crs=5070 # consider changing for Alaska ?

    #produce footprints of lidar dataset over conus
    entwine_footprints_gdf=make_lidar_footprints()

    OSM_bridge_lines_gdf=gpd.read_file(OSM_bridge_file)
    OSM_polygons_gdf=OSM_bridge_lines_gdf.copy()
    OSM_polygons_gdf['geometry'] = OSM_polygons_gdf['geometry'].buffer(buffer_width)  
    OSM_polygons_gdf.to_crs(entwine_footprints_gdf.crs, inplace=True)
    OSM_polygons_gdf.rename(columns={'name':'bridge_name'}, inplace=True)
        
    #intersect with lidar urls
    OSM_polygons_gdf = gpd.overlay(OSM_polygons_gdf, entwine_footprints_gdf, how='intersection')

    OSM_polygons_gdf.to_file(os.path.join(output_dir,'buffered_bridges.gpkg'))

    #filter if there are multiple urls for a bridge, keep the url with highest count
    OSM_polygons_gdf = OSM_polygons_gdf.loc[OSM_polygons_gdf.groupby('osmid')['count'].idxmax()]
    OSM_polygons_gdf = OSM_polygons_gdf.reset_index(drop=True)

    pool_args=[]
    for i, row in OSM_polygons_gdf.iterrows():
        osmid,poly_geo, lidar_url = row.osmid, row.geometry, row.url
        pool_args.append((osmid,poly_geo, lidar_url,output_dir))

    print('There are %d files to get downloaded'%len(pool_args))
    with Pool(15) as pool:
        pool.map(download_lidar_points, pool_args)

    downloaded_points_files = glob.glob(os.path.join(output_dir,'point_files', '*.las'))

    pool_args=[]
    for points_path in downloaded_points_files:
        osmid= os.path.basename(points_path).split('.las')[0]
        pool_args.append((osmid,points_path,output_dir,raster_resolution,tif_crs ))

    with Pool(10) as pool:
        pool.map(make_rasers_in_parallel, pool_args)



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
        help='OPTIONAL: Buffer to apply to OSM bridge lines to convert them to polygon. Default value is 1.5m (on each side)',
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

    # try:
    process_bridges_lidar_data(**args)

    # except Exception:
    #     print('something wrong')
    #     #logging.info(traceback.format_exc())
    #     end_time = dt.datetime.now(dt.timezone.utc)
    #     #logging.info(f"   End time: {end_time.strftime('%m/%d/%Y %H:%M:%S')}")





    








    











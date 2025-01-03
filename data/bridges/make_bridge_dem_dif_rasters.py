

import rasterio
from rasterio.merge import merge
import xarray as xr
import rioxarray  # Extension of xarray for geospatial data
import glob
import os
import time
import argparse
import geopandas as gpd
import traceback


def merge_bridge_tifs(lidar_tif_dir,HUC6_lidar_tif_osmids,merged_file_path):

    merge_start_time = time.time()
    
    src_files_to_mosaic = []

    for osmid in HUC6_lidar_tif_osmids:
        fp=os.path.join(lidar_tif_dir,'%s.tif'%str(osmid))
        src = rasterio.open(fp)
        src_files_to_mosaic.append(src)

    mosaic, out_trans = merge(src_files_to_mosaic,nodata=src_files_to_mosaic[0].nodata)
    # Close all the source files
    for src in src_files_to_mosaic:
        src.close()

    out_meta = {
        'driver': 'GTiff',
        'dtype': 'float32',
        'compress': 'LZW',
        'tiled': True,
        'blockxsize': 256,
        'blockysize': 256,
        'BIGTIFF': 'YES',
        'height': mosaic.shape[1],  # Number of rows in the merged raster
        'width': mosaic.shape[2],   # Number of columns in the merged raster
        'transform': out_trans,
        'crs': src_files_to_mosaic[0].crs,
        'count': mosaic.shape[0],
        'nodata': src_files_to_mosaic[0].nodata
    }
    

    with rasterio.open(merged_file_path, 'w', **out_meta) as dest:
        dest.write(mosaic)

    merge_end_time = time.time()
    print(f"merging time: {merge_end_time - merge_start_time:.4f} seconds")



    # src_files_to_mosaic = []

    # for osmid in HUC6_lidar_tif_osmids:
    #     fp=os.path.join(lidar_tif_dir,'%s.tif'%str(osmid))
    #     # print(fp)

    #     src = rasterio.open(fp)
    #     src_files_to_mosaic.append(src)

    # mosaic, out_trans = merge(src_files_to_mosaic)
    # # Close all the source files
    # for src in src_files_to_mosaic:
    #     src.close()
    # # Create an empty raster that covers the entire domain
    # # Define the metadata
    # out_meta = src.meta.copy()
    # # out_meta.update({
    # #     "driver": "GTiff",
    # #     "height": mosaic.shape[1],
    # #     "width": mosaic.shape[2],
    # #     "transform": out_trans,
    # #     "crs": 'EPSG:3857'
    # # })

    # out_meta.update({
    #         'driver': 'GTiff',
    #         'dtype': 'float32',
    #         'compress': 'LZW',
    #         'tiled': True,
    #         'blockxsize': 256,
    #         'blockysize': 256,
    #         'BIGTIFF': 'YES',
    #         # 'nodata': original_nodata,
    #         'width': mosaic.shape[2],
    #         'height': mosaic.shape[1],
    #         'transform': out_trans
    #     })

    # with rasterio.open(merged_file_path, 'w', **out_meta) as dest:
    #     dest.write(mosaic)



def identify_bridges_with_lidar(OSM_bridge_lines_gdf,lidar_tif_dir):
    #identify osmids with lidar-tif or not
    
    tif_ids = set(os.path.splitext(os.path.basename(f))[0] for f in os.listdir(lidar_tif_dir) if f.endswith('.tif'))
    OSM_bridge_lines_gdf['has_lidar_tif'] = OSM_bridge_lines_gdf['osmid'].apply(lambda x: 'Y' if str(x) in tif_ids else 'N')
    return OSM_bridge_lines_gdf


def make_dif_ratsers(OSM_bridge_file,dem_dir,lidar_tif_dir,output_dir):
    try:
        #add HUC6 info update the osm bridge line file with existence of lidar 
        OSM_bridge_lines_gdf=gpd.read_file(OSM_bridge_file)
        OSM_bridge_lines_gdf['HUC6']=OSM_bridge_lines_gdf['HUC'].str[:6]
        OSM_bridge_lines_gdf=identify_bridges_with_lidar(OSM_bridge_lines_gdf,lidar_tif_dir)

        dem_files=list(glob.glob(os.path.join(dem_dir, '*.tif')))
        available_dif_files=list(glob.glob(os.path.join(output_dir, '*.tif')))
        # dem_files=['/data/inputs/dems/3dep_dems/10m_5070/20240916/HUC6_020502_dem.tif']
        # dem_files=[]
        for dem_file in dem_files:
            base_name, extension = os.path.splitext(os.path.basename(dem_file))
            output_file_name=f"{base_name}_diff{extension}"
            output_file_path = os.path.join(output_dir, output_file_name)

            HUC6=base_name.split('_')[1]
            print('working on HUC6%s'%str(HUC6))

            #get alis t of osmids in this HUC6 with lidar-generated tif files
            HUC6_lidar_tif_osmids=OSM_bridge_lines_gdf[(OSM_bridge_lines_gdf['HUC6']==HUC6)&(OSM_bridge_lines_gdf['has_lidar_tif']=='Y') ]['osmid'].values.tolist()


            if HUC6_lidar_tif_osmids and output_file_path not in available_dif_files and HUC6 not in (['051201','101800', '102500']) :  # remove the second condition...it is just a temporarily workaround
                print('making diff for HUC6_%s with %d tif'%(HUC6,len(HUC6_lidar_tif_osmids)))
                
                merged_file_path=os.path.join(output_dir,'merged.tif')
                merge_bridge_tifs(lidar_tif_dir,HUC6_lidar_tif_osmids,merged_file_path)

                local_da = xr.open_dataarray(merged_file_path, engine="rasterio", chunks={"x": 1024, "y": 1024})
                #remove the merged file which is not needed anymore
                os.remove(os.path.join(output_dir,'merged.tif'))

                # Retrieve profile from the original DEM file
                with rasterio.open(dem_file) as src:
                    dem_profile = src.profile

                # Open the regional TIFF as an xarray DataArray
                original_da = xr.open_dataarray(dem_file, engine="rasterio", chunks={"x": 1024, "y": 1024})
                original_nodata=original_da.rio.nodata
                enhanced_da = original_da.copy()

                # Open the local TIFF as an xarray DataArray and reproject to match the regional grid, if needed
                if local_da.rio.crs != original_da.rio.crs:
                    print('check the inputs. The crs must match between lidar-tif files and DEM. ')
                    exit()
                    # local_da = local_da.rio.reproject_match(original_da) 
                # Replace values in the regional DataArray with the local DataArray values at overlapping locations
                local_da = local_da.reindex_like(enhanced_da, method='nearest')
                enhanced_da = enhanced_da.where(local_da.isnull(), other=local_da)
                # # Set nodata value to be consistent
                enhanced_da=enhanced_da.fillna(original_nodata)
                enhanced_da.rio.write_nodata(original_nodata, inplace=True)

                diff = enhanced_da - original_da
                del enhanced_da  #good practice to clean the memory especially for big rasters

                # Save the result to a new TIFF file
                # diff.rio.to_raster(os.path.join(output_dir,output_file_name),compress="LZW") first version
                # Update the profile with the desired settings
                # Ensure CRS is properly set for the output
                if diff.rio.crs is None:
                    diff.rio.write_crs(original_da.rio.crs, inplace=True)

                # Update the profile with the desired settings
                output_profile = dem_profile.copy()
                output_profile.update({
                    'driver': 'GTiff',
                    'dtype': 'float32',
                    'compress': 'LZW',
                    'tiled': True,
                    'blockxsize': 256,
                    'blockysize': 256,
                    'BIGTIFF': 'YES',
                    'nodata': original_nodata,
                    'width': original_da.rio.width,  # Use original DEM's width
                    'height': original_da.rio.height,  # Use original DEM's height
                    'transform': original_da.rio.transform()
                })

                # Ensure diff.values has the correct shape
                data = diff.values
                if data.ndim == 4:
                    data = data.squeeze()  # Remove extra dimensions if present
                if data.ndim == 2:  # Single-band case (height, width)
                    data = data[None, :, :]  # Add band dimension

                # Write the final output raster
                
                with rasterio.open(output_file_path, 'w', **output_profile) as dst:
                    dst.write(data)


            else: #if there are no lidar raster for this HUC6, just return a zero diff file
                print('There is no lidar-generated raster file for osm bridges in HUC%s'%HUC6)


        # #save the osm bridge line file
        base, ext = os.path.splitext(os.path.basename(OSM_bridge_file)) 
        OSM_bridge_lines_gdf.to_file(os.path.join(output_dir,f"{base}_modified{ext}"))
    except Exception:
        print(traceback.format_exc())



if __name__ == "__main__":  

    '''
        Sample usage:
       python create_osm_raster_using_lidar.py
       -i osm_all_bridges.gpkg
       -b 1.5
       -r 3
       -o /results/02050206
    '''

    parser = argparse.ArgumentParser(description='Make bridge dem difference rasters')

    parser.add_argument(
        '-i', '--OSM_bridge_file', help='REQUIRED: A gpkg that contains the bridges lines', required=True
    ) 

    parser.add_argument(
        '-d',
        '--dem_dir',
        help='REQUIRED: folder path where 3DEP dems are loated.',
        required=True,
    )

    parser.add_argument(
        '-l',
        '--lidar_tif_dir',
        help='REQUIRED: folder path where lidar-gerenared bridge elevtions are located.',
        required=True,
    )


    parser.add_argument(
        '-o',
        '--output_dir',
        help='REQUIRED: folder path for output diff rasters.',
        required=True,
    )


    args = vars(parser.parse_args())
    print('starting...')
    make_dif_ratsers(**args)


import argparse
import geopandas as gpd
from geopandas.tools import sjoin
import os
import rasterio

temp_workspace = r''
HAND_CRS = 'EPSG:3857'


def ingest_points_layer(points_layer, fim_directory, wbd_path):
    
    # Read wbd_path and points_layer to determine which HUC6 each point is in.
    wbd_huc8_read = gpd.read_file(wbd_path, layer='WBDHU6')    
    points_layer_read = gpd.read_file(points_layer)
    
    # Update CRS of points_layer_read.
    points_layer_read = points_layer_read.to_crs(HAND_CRS)
    wbd_huc8_read = wbd_huc8_read.to_crs(HAND_CRS)
    
    # Spatial join the two layers.
    point_huc6_sjoin = sjoin(points_layer_read, wbd_huc8_read)
    
    # Convert to GeoDataFrame.
    gdf = gpd.GeoDataFrame(point_huc6_sjoin)
    
    # Add two columns for X and Y.
    gdf['X'] = gdf['geometry'].x
    gdf['Y'] = gdf['geometry'].y
            
    # Extract information into dictionary.
    points_dict = {}
    for index, row in gdf.iterrows():        
        try:
            points_dict[row['HUC6']]['points'].update({str(row['geometry']): {'flow': row['flow'], 'submitter': row['submitter'], 'flow_unit': row['flow_unit']}})
        except KeyError:
            points_dict.update({row['HUC6']: {'points': {}}})

    # Define coords variable to be used in point raster value attribution.
    coords = [(x,y) for x, y in zip(point_huc6_sjoin.X, point_huc6_sjoin.Y)]
    
    # Define paths to relevant HUC6 HAND data.
    for huc6 in points_dict:
        # Define paths to relevant HUC6 HAND data and get necessary metadata for point rasterization.
        hand_path = os.path.join(fim_directory, huc6, 'hand_grid_' + huc6 + '.tif')
        catchments_path = os.path.join(fim_directory, huc6, 'catchments_' + huc6 + '.tif')
        
        # Use point geometry to determine pixel values at catchment and HAND grids.
        hand_src = rasterio.open(hand_path)
        point_huc6_sjoin['hand'] = [h[0] for h in hand_src.sample(coords)]
        hand_src.close()
        catchments_src = rasterio.open(catchments_path)
        point_huc6_sjoin['hydroid'] = [c[0] for c in catchments_src.sample(coords)]
               
        output_csv = os.path.join(fim_directory, huc6, 'user_supplied_n_vals.csv')
        
        data_to_write = point_huc6_sjoin[['flow', 'flow_unit', 'submitter', 'hand', 'hydroid', 'X', 'Y']]     
        data_to_write.to_csv(output_csv)


if __name__ == '__main__':
    
    # Parse arguments.
    parser = argparse.ArgumentParser(description='Adjusts rating curve given a shapefile containing points of known water boundary.')
    parser.add_argument('-p','--points-layer',help='Path to points layer containing known water boundary locations',required=True)
    parser.add_argument('-d','--fim-directory',help='Parent directory of FIM-required datasets.',required=True)
    parser.add_argument('-w','--wbd-path', help='Path to national HUC6 layer.',required=True)
    
    # Assign variables from arguments.
    args = vars(parser.parse_args())
    points_layer = args['points_layer']
    fim_directory = args['fim_directory']
    wbd_path = args['wbd_path']
    
    ingest_points_layer(points_layer, fim_directory, wbd_path)
    
    # Open catchment, HAND, and point grids and determine pixel values for Hydroid, HAND value, and discharge value, respectively.
    
    # Open rating curve file(s).
    
    # Use three values to determine the hydroid rating curve(s) to update, then update them using a variation of Manning's Equation.
    
    # Ensure the JSON rating curve is updated and saved (overwitten). Consider adding attributes to document what was performed.
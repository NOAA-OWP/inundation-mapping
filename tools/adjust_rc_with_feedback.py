import argparse
import geopandas as gpd
from geopandas.tools import sjoin
import os
import rasterio

temp_workspace = r''
HAND_CRS = 'EPSG:3857'

def update_rating_curve(grouped_median, huc6):
    pass


def ingest_points_layer(points_layer, fim_directory, wbd_path):
    
    # Read wbd_path and points_layer to determine which HUC6 each point is in.
    wbd_huc8_read = gpd.read_file(wbd_path, layer='WBDHU6')    
    points_layer_read = gpd.read_file(points_layer)
    
    # Update CRS of points_layer_read.
    points_layer_read = points_layer_read.to_crs(HAND_CRS)
    wbd_huc8_read = wbd_huc8_read.to_crs(HAND_CRS)
    
    # Spatial join the two layers.
    water_edge_df = sjoin(points_layer_read, wbd_huc8_read)
        
    # Convert to GeoDataFrame.
    gdf = gpd.GeoDataFrame(water_edge_df)
    
    # Add two columns for X and Y.
    gdf['X'] = gdf['geometry'].x
    gdf['Y'] = gdf['geometry'].y
            
    # Extract information into dictionary.
    huc6_list = []
    for index, row in gdf.iterrows():
        huc6 = row['HUC6']
        if huc6 not in huc6_list:
            huc6_list.append(huc6)

    # Define coords variable to be used in point raster value attribution.
    coords = [(x,y) for x, y in zip(water_edge_df.X, water_edge_df.Y)]
        
    # Define paths to relevant HUC6 HAND data.
    for huc6 in huc6_list:
        print(huc6)
        
        # Define paths to relevant HUC6 HAND data and get necessary metadata for point rasterization.
        hand_path = os.path.join(fim_directory, huc6, 'hand_grid_' + huc6 + '.tif')
        if not os.path.exists(hand_path):
            print("HAND grid for " + huc6 + " does not exist.")
            continue
        catchments_path = os.path.join(fim_directory, huc6, 'catchments_' + huc6 + '.tif')
        if not os.path.exists(catchments_path):
            print("Catchments grid for " + huc6 + " does not exist.")
            continue
        
#        water_edge_df = water_edge_df[water_edge_df['HUC6'] == huc6]
        
        # Use point geometry to determine pixel values at catchment and HAND grids.
        hand_src = rasterio.open(hand_path)
        water_edge_df['hand'] = [h[0] for h in hand_src.sample(coords)]
        hand_src.close()
        catchments_src = rasterio.open(catchments_path)
        water_edge_df['hydroid'] = [c[0] for c in catchments_src.sample(coords)]
                
        # Get median HAND value for appropriate groups.
        grouped_median = water_edge_df.groupby(["hydroid", "flow", "submitter", "coll_time", "flow_unit"])['hand'].median()
               
        output_csv = os.path.join(fim_directory, huc6, 'user_supplied_n_vals_' + huc6 + '.csv')
        
        grouped_median.to_csv(output_csv)
        
        update_rating_curve(grouped_median, huc6)


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
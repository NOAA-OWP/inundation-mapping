#!/usr/bin/env python3

import argparse
from osgeo import gdal
import geopandas as gpd

def rasterize_by_order(vector_filename, raster_filename, x_min, y_min, x_max, y_max, columns, rows, nodata_value, stream_layer, branch_id_attribute, branch_id, order_attribute, min_order):
    """
    Rasterizes polygon layers exceeding a stream order threshold
    """
    
    streams = gpd.read_file(stream_layer)

    # Rasterize branch zero or if branch is at least the minimum order
    if branch_id == 0 or int(streams.loc[streams[branch_id_attribute].astype(int)==branch_id, order_attribute].max()) >= min_order:

        # Open the data source and read in the extent
        source_ds = gdal.OpenEx(vector_filename)

        gdal.Rasterize(raster_filename,
                        source_ds,
                        format='GTIFF',
                        outputType=gdal.GDT_Int32,
                        creationOptions=["COMPRESS=LZW", "BIGTIFF=YES", "TILES=YES"],
                        noData=nodata_value,
                        initValues=1,
                        width=columns,
                        height=rows,
                        allTouched=True,
                        burnValues=nodata_value,
                        outputBounds=[x_min, y_min, x_max, y_max])


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Rasterize a polygon layer')
    parser.add_argument('-v', '--vector-filename', help='Input vector filename', required=True)
    parser.add_argument('-f', '--raster-filename', help='Output raster filename', required=True)
    parser.add_argument('--x-min', help='Minimum x coordinate', required=True)
    parser.add_argument('--y-min', help='Minimun y coordinate', required=True)
    parser.add_argument('--x-max', help='Maximum x coordinate', required=True)
    parser.add_argument('--y-max', help='Maximum y coordinate', required=True)
    parser.add_argument('-c', '--columns', help='Output raster columns', required=True)
    parser.add_argument('-r', '--rows', help='Output raster rows', required=True)
    parser.add_argument('-n', '--nodata-value', help='NoData value', required=True)
    parser.add_argument('-s', '--stream-layer', help='Stream layer filename', required=True)
    parser.add_argument('-b', '--branch-id-attribute', help='Branch ID attribute name', required=False, default='levpa_id')
    parser.add_argument('-i', '--branch-id', help='Branch ID', type=int, required='True')
    parser.add_argument('-a', '--order-attribute', help='Stream order attribute name', required=False, default='order_')
    parser.add_argument('-o', '--min-order', help='Minimum order to process', type=int, required=False, default=10)

    args = vars(parser.parse_args())

    rasterize_by_order(**args)

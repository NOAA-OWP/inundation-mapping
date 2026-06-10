#!/usr/bin/env python3
import argparse
import sys
import geopandas as gpd
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_bounds

def parse_co(co_list):
    """Parses a list of ['KEY=VALUE', 'KEY2=VALUE2'] into a dictionary."""
    co_dict = {}
    if not co_list:
        return co_dict
    for item in co_list:
        if '=' in item:
            k, v = item.split('=', 1)
            co_dict[k.lower()] = v
        else:
            print(f"Warning: Invalid creation option '{item}'. Expected KEY=VALUE.", file=sys.stderr)
    return co_dict

def rasterize_parquet(input_src, output_dst, te, ts, quiet=False, all_touched=False, ot="Int32", burn=1.0, attribute=None, init=0.0, a_nodata=-9999.0, co=None):
    """Core rasterization logic supporting fixed burn values or dynamic attribute columns."""
    xmin, ymin, xmax, ymax = te
    ncols, nrows = ts
    creation_options = parse_co(co)

    if not quiet:
        print(f"Reading input vector: {input_src}...")
    
    try:
        gdf = gpd.read_parquet(input_src)
    except Exception as e:
        print(f"Error reading parquet file: {e}", file=sys.stderr)
        sys.exit(1)

    # Calculate spatial transform grid matrix
    transform = from_bounds(xmin, ymin, xmax, ymax, ncols, nrows)

    if not quiet:
        print("Rasterizing geometries...")
        
    # Generate shapes iterable
    if attribute:
        if attribute not in gdf.columns:
            print(f"Error: Attribute column '{attribute}' not found in the input parquet file.", file=sys.stderr)
            sys.exit(1)
        
        # Pull values dynamically from the column, fallback to burn value if entry is null/NaN
        shapes = (
            (geom, float(val) if not gpd.pd.isna(val) else burn) 
            for geom, val in zip(gdf.geometry, gdf[attribute]) 
            if geom is not None
        )
    else:
        # Use fixed static burn value
        shapes = ((geom, burn) for geom in gdf.geometry if geom is not None)

    try:
        rasterized_array = rasterize(
            shapes=shapes,
            out_shape=(nrows, ncols),
            transform=transform,
            fill=init,
            all_touched=all_touched,
            dtype=ot.lower()
        )
    except Exception as e:
        print(f"Error during rasterization processing: {e}", file=sys.stderr)
        sys.exit(1)

    if not quiet:
        print(f"Writing output GeoTIFF: {output_dst}...")
        
    try:
        with rasterio.open(
            output_dst,
            'w',
            driver='GTiff',
            height=nrows,
            width=ncols,
            count=1,
            dtype=ot.lower(),
            crs=gdf.crs,
            transform=transform,
            nodata=a_nodata,
            **creation_options
        ) as dst:
            dst.write(rasterized_array, 1)
            
        if not quiet:
            print("Done successfully!")
            
    except Exception as e:
        print(f"Error writing GeoTIFF output: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Python equivalent of gdal_rasterize supporting Parquet input and attribute burning."
    )
    
    # Core Positional/File Arguments
    parser.add_argument("input_src", help="Path to the input vector (.parquet) file")
    parser.add_argument("output_dst", help="Path to the output raster (.tif) file")
    
    # Matching gdal_rasterize flags exactly
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress logging progress outputs")
    parser.add_argument("-at", "--all-touched", action="store_true", help="All touched")
    parser.add_argument("-ot", default="Int32", help="Output data type (e.g., Int32, Float32, Byte). Default: Int32")
    parser.add_argument("-burn", type=float, default=1.0, help="Fixed value to burn into the raster if -a is not used. Default: 1")
    parser.add_argument("-a", dest="attribute", default=None, help="Name of the attribute column in the parquet file to burn into the raster grid")
    parser.add_argument("-init", type=float, default=0.0, help="Value to initialize the raster band with. Default: 0")
    parser.add_argument("-a_nodata", type=float, default=-9999.0, help="Assign a specified nodata value to output bands")
    
    # Creation Options (-co "BIGTIFF=YES")
    parser.add_argument("-co", action="append", help="Creation options for output format (e.g., -co BIGTIFF=YES). Can be specified multiple times.")
    
    # Extent & Resolution
    parser.add_argument("-te", nargs=4, type=float, required=True, metavar=('XMIN', 'YMIN', 'XMAX', 'YMAX'),
                        help="Target extent: xmin ymin xmax ymax")
    parser.add_argument("-ts", nargs=2, type=int, required=True, metavar=('NCOLS', 'NROWS'),
                        help="Target size: ncols nrows")

    args = parser.parse_args()

    # Pass the CLI arguments to the executor function
    rasterize_parquet(**vars(args))

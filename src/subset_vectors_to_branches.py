#!/usr/bin/env python3
import os
import sys
import argparse
import geopandas as gpd

def convert_layer(input_path, output_path, crs, where_clause):
    """Reads, filters, reprojects, and saves a layer."""
    if not os.path.exists(input_path):
        print(f"Skipping: Input file not found -> {input_path}")
        return

    print(f"Processing: {os.path.basename(input_path)} -> {os.path.basename(output_path)}")
    try:
        # Read with the SQL WHERE filter applied at the OGR/GDAL layer level
        df = gpd.read_file(input_path, where=where_clause)
        
        if df.empty:
            print(f"  Warning: No matching rows for filter ({where_clause})")
            df = df.to_crs(crs)  # Keeps structural schema intact
        else:
            df = df.to_crs(crs)
            
        # Write natively to Parquet via PyArrow
        df.to_parquet(output_path, index=False)
        print("  Success!")
        
    except Exception as e:
        print(f"  Error processing {os.path.basename(input_path)}: {e}", file=sys.stderr)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dynamically convert specified GPKG files to Parquet.")
    parser.add_argument("--where", required=True, help="SQL WHERE clause filter (e.g., 'attr=value')")
    parser.add_argument("--crs", required=True, help="Target CRS (e.g., EPSG:4326)")
    parser.add_argument(
        "--files", 
        required=True, 
        nargs='+', 
        help="Space-separated pairs of input_file output_file (e.g., in1.gpkg out1.parquet in2.gpkg out2.parquet)"
    )

    args = parser.parse_args()
    
    # Ensure we have an even number of arguments for pairs
    if len(args.files) % 2 != 0:
        print("Error: The --files argument requires an even number of paths (input/output pairs).", file=sys.stderr)
        sys.exit(1)
        
    # Group the flat list into (input, output) tuples
    tasks = list(zip(args.files[0::2], args.files[1::2]))
    
    # Run the extractions sequentially
    for in_file, out_file in tasks:
        convert_layer(in_file, out_file, args.crs, args.where)
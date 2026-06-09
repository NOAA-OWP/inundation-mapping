#!/usr/bin/env python3
import os
import sys
import argparse
import re

import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq


def parse_where_to_filters(where_clause, input_path):
    """
    Parses a simple standard SQL where clause string into a PyArrow filters tuple list,
    matching the exact data type of the column from the Parquet metadata.
    """
    if not where_clause:
        return None
        
    # Regex to capture: column, operator, and the value (with or without quotes)
    match = re.match(r"^\s*([\w]+)\s*(=|==|!=|<|<=|>|>=)\s*['\"]?([^'\"]+)['\"]?\s*$", where_clause)
    if not match:
        raise ValueError(
            f"Could not parse query context for Parquet engine: '{where_clause}'. "
            "Ensure it follows a simple 'column operator value' syntax."
        )
        
    col, op, val = match.groups()
    
    # Standardize SQL '=' to PyArrow '=='
    if op == '=':
        op = '=='
        
    # --- Dynamic Type Matching via Parquet Metadata ---
    try:
        # Read just the schema metadata (incredibly fast, doesn't load the table data)
        schema = pq.read_schema(input_path)
        
        if col in schema.names:
            col_type = schema.field(col).type
            
            # If the Parquet column is a string/binary type, treat our value as a string
            if pa.types.is_string(col_type) or pa.types.is_binary(col_type):
                val = str(val)
            # If it's an integer type, cast our value to an int
            elif pa.types.is_integer(col_type):
                val = int(float(val))  # float first handles cases like '123.0' safely
            # If it's a float type, cast our value to a float
            elif pa.types.is_floating(col_type):
                val = float(val)
        else:
            print(f"Warning: Column '{col}' not found in Parquet schema metadata.", file=sys.stderr)
    except Exception as e:
        # Fallback to standard string/digit guessing if metadata reading fails
        if val.isdigit():
            val = int(val)
            
    return [(col, op, val)]

def convert_layer(input_path, output_path, crs, where_clause):
    """Reads, filters, reprojects, and saves a layer."""
    if not os.path.exists(input_path):
        print(f"Skipping: Input file not found -> {input_path}")
        return

    print(f"Processing: {os.path.basename(input_path)} -> {os.path.basename(output_path)}")
    try:
        # Determine the file format extension
        ext = os.path.splitext(input_path)[-1].lower()

        # Change this line inside your convert_layer function:
        if ext == '.parquet':
            # Pass input_path as the second argument
            parquet_filters = parse_where_to_filters(where_clause, input_path)
            df = gpd.read_parquet(input_path, filters=parquet_filters)
        else:
            # Standard OGR driver load for .gpkg, .shp, etc.
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
    parser = argparse.ArgumentParser(description="Dynamically convert specified vector layers to Parquet.")
    parser.add_argument("--where", required=True, help="SQL WHERE clause filter (e.g., 'attr=value')")
    parser.add_argument("--crs", required=True, help="Target CRS (e.g., EPSG:4326)")
    parser.add_argument(
        "--files", 
        required=True, 
        nargs='+', 
        help="Space-separated pairs of input_file output_file (e.g., in1.gpkg out1.parquet)"
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

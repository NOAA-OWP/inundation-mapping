#!/usr/bin/env python3

'''
 Description: Mask raster layers using 'mainstems' stream buffer
'''

import sys
import os
import geopandas as gpd
import rasterio.mask

split_flows_fileName    = sys.argv[1]
fdr_fr                  = sys.argv[2]
dem_fr                  = sys.argv[3]
slope_fr                = sys.argv[4]
fdr_ms_filename         = sys.argv[5]
dem_ms_filename         = sys.argv[6]
slope_ms_filename       = sys.argv[7]
str_pixel_fr            = sys.argv[8]
str_pixel_ms_filename   = sys.argv[9]
ms_buffer_dist          = int(os.environ['ms_buffer_dist'])

# create output layer names
split_flows = gpd.read_file(split_flows_fileName)

# Limit the rasters to the buffer distance around the draft streams.
print ("Limiting rasters to buffer area ({} meters) around model streams".format(str(ms_buffer_dist)))

split_flows_ms_buffer = split_flows.unary_union.buffer(ms_buffer_dist)

print('Writing raster outputs ...')

# Mask nhddem
with rasterio.open(dem_fr) as src:
    out_image, out_transform = rasterio.mask.mask(src, [split_flows_ms_buffer], crop=True)
    out_meta = src.meta

out_meta.update({"driver": "GTiff",
     "height": out_image.shape[1],
     "width": out_image.shape[2],
     "transform": out_transform})

with rasterio.open(os.path.join(os.path.dirname(dem_fr), dem_ms_filename), "w", **out_meta) as dest:
    dest.write(out_image)

# Mask fdr
with rasterio.open(fdr_fr) as src:
    out_image, out_transform = rasterio.mask.mask(src, [split_flows_ms_buffer], crop=True)
    out_meta = src.meta

out_meta.update({"driver": "GTiff",
     "height": out_image.shape[1],
     "width": out_image.shape[2],
     "transform": out_transform})

with rasterio.open(os.path.join(os.path.dirname(fdr_fr), fdr_ms_filename), "w", **out_meta) as dest:
    dest.write(out_image)

# Mask slope
with rasterio.open(slope_fr) as src:
    out_image, out_transform = rasterio.mask.mask(src, [split_flows_ms_buffer], crop=True)
    out_meta = src.meta

out_meta.update({"driver": "GTiff",
     "height": out_image.shape[1],
     "width": out_image.shape[2],
     "transform": out_transform})

with rasterio.open(os.path.join(os.path.dirname(slope_fr), slope_ms_filename), "w", **out_meta) as dest:
    dest.write(out_image)

# Mask stream pixels
with rasterio.open(str_pixel_fr) as src:
    out_image, out_transform = rasterio.mask.mask(src, [split_flows_ms_buffer], crop=True)
    out_meta = src.meta

out_meta.update({"driver": "GTiff",
     "height": out_image.shape[1],
     "width": out_image.shape[2],
     "transform": out_transform})

with rasterio.open(os.path.join(os.path.dirname(str_pixel_fr), str_pixel_ms_filename), "w", **out_meta) as dest:
    dest.write(out_image)

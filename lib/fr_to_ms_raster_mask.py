#!/usr/bin/env python3

'''
 Description: Mask raster layers using 'mainstems' stream buffer
'''

import sys
import geopandas as gpd
import rasterio.mask

split_flows_fileName      = sys.argv[1]
fdrFR                     = sys.argv[2]
nhddemFR                  = sys.argv[3]
slpFR                     = sys.argv[4]
fdrMSname                 = sys.argv[5]
nhddemMSname              = sys.argv[6]
slpMSname                 = sys.argv[7]
floodAOIbuf               = sys.argv[8]

# create output layer names
split_flows = gpd.read_file(split_flows_fileName)

# Limit the rasters to the buffer distance around the draft streams.
print ("Limiting rasters to buffer area ({} meters) around model streams".format(str(floodAOIbuf)))

MSsplit_flows_gdf_buffered = split_flows.unary_union.buffer(int(floodAOIbuf))

print('Writing raster outputs ...')

# Mask nhddem
with rasterio.open(nhddemFR) as src:
    out_image, out_transform = rasterio.mask.mask(src, [MSsplit_flows_gdf_buffered], crop=True)
    out_meta = src.meta

out_meta.update({"driver": "GTiff",
     "height": out_image.shape[1],
     "width": out_image.shape[2],
     "transform": out_transform})

with rasterio.open(os.path.join(os.path.dirname(nhddemFR), nhddemMSname), "w", **out_meta) as dest:
    dest.write(out_image)

# Mask fdr
with rasterio.open(fdrFR) as src:
    out_image, out_transform = rasterio.mask.mask(src, [MSsplit_flows_gdf_buffered], crop=True)
    out_meta = src.meta

out_meta.update({"driver": "GTiff",
     "height": out_image.shape[1],
     "width": out_image.shape[2],
     "transform": out_transform})

with rasterio.open(os.path.join(os.path.dirname(fdrFR), fdrMSname), "w", **out_meta) as dest:
    dest.write(out_image)

# Mask slope
with rasterio.open(slpFR) as src:
    out_image, out_transform = rasterio.mask.mask(src, [MSsplit_flows_gdf_buffered], crop=True)
    out_meta = src.meta

out_meta.update({"driver": "GTiff",
     "height": out_image.shape[1],
     "width": out_image.shape[2],
     "transform": out_transform})

with rasterio.open(os.path.join(os.path.dirname(slpFR), slpMSname), "w", **out_meta) as dest:
    dest.write(out_image)

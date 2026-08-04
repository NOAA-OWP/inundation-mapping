#!/usr/bin/env python3
"""
GDAL CLI Utilities Modernization
--------------------------------
Native Python/GDAL replacements for rasterization and virtual filesystem (vsimem) utilities.
"""

from pathlib import Path

from osgeo import gdal


gdal.UseExceptions()


def gdal_rasterize_vector(
    src_vector: str,
    template_raster: str,
    dst_raster: str,
    attribute: str = None,
    burn_value: float = None,
    init_value: float = 0,
    use_3d: bool = False,
):
    """
    Native GDAL C-API replacement for `gdal_rasterize`.
    Rasterizes vector features onto a template raster grid matching its bounds,
    resolution, projection, and extent.

    Parameters
    ----------
    src_vector : str
        Path to input vector dataset (e.g. .gpkg, .shp)
    template_raster : str
        Path to template raster providing target dimensions, extent, and SRS
    dst_raster : str
        Path to output GeoTIFF raster
    attribute : str, optional
        Name of attribute column to burn (e.g. 'HydroID', 'zelev')
    burn_value : float, optional
        Constant value to burn if attribute is not supplied
    init_value : float, optional
        Initial background value for raster creation (default: 0)
    use_3d : bool, optional
        Burn 3D Z-coordinate geometries instead of an attribute column (-3d)
    """
    ds_tmpl = gdal.Open(str(template_raster))
    gt = ds_tmpl.GetGeoTransform()
    ncols = ds_tmpl.RasterXSize
    nrows = ds_tmpl.RasterYSize
    proj = ds_tmpl.GetProjection()
    ds_tmpl = None

    xmin = gt[0]
    ymax = gt[3]
    xmax = xmin + gt[1] * ncols
    ymin = ymax - abs(gt[5]) * nrows

    extra_options = []
    if use_3d:
        extra_options.append("-3d")

    options_kwargs = {
        "format": "GTiff",
        "outputType": gdal.GDT_Float32,
        "outputBounds": [xmin, ymin, xmax, ymax],
        "xRes": abs(gt[1]),
        "yRes": abs(gt[5]),
        "outputSRS": proj,
        "initValues": [init_value],
        "options": extra_options,
        "creationOptions": ["BLOCKXSIZE=512", "BLOCKYSIZE=512", "TILED=YES", "COMPRESS=LZW", "BIGTIFF=YES"],
    }

    if attribute and not use_3d:
        options_kwargs["attribute"] = attribute

    if burn_value is not None:
        options_kwargs["burnValues"] = [burn_value]

    opts = gdal.RasterizeOptions(**options_kwargs)
    gdal.Rasterize(str(dst_raster), str(src_vector), options=opts)


def export_vsimem_to_disk(vsimem_path: str, output_disk_path: str):
    """
    Exports a GDAL virtual memory dataset (/vsimem/...) directly to a GeoTIFF on disk.

    Parameters
    ----------
    vsimem_path : str
        The virtual memory path (e.g., '/vsimem/temp_raster.tif')
    output_disk_path : str or Path
        Target filepath on the physical filesystem
    """
    ds = gdal.Open(str(vsimem_path))
    if ds is None:
        raise FileNotFoundError(f"Virtual memory dataset not found at '{vsimem_path}'")

    options = gdal.TranslateOptions(
        format="GTiff",
        outputType=ds.GetRasterBand(1).DataType,
        creationOptions=["BLOCKXSIZE=512", "BLOCKYSIZE=512", "TILED=YES", "COMPRESS=LZW", "BIGTIFF=YES"],
    )

    output_disk_path = Path(output_disk_path)
    output_disk_path.parent.mkdir(parents=True, exist_ok=True)

    gdal.Translate(str(output_disk_path), ds, options=options)
    ds = None

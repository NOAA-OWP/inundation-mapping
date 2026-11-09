"""Small fixtures shared by the workflow tests (geospatial libraries are imported only when used)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path


HAS_PYARROW = importlib.util.find_spec("pyarrow") is not None
HAS_RASTERIO = (
    importlib.util.find_spec("rasterio") is not None and importlib.util.find_spec("numpy") is not None
)


def write_geoparquet(path: Path, columns: dict[str, list], crs: int) -> None:
    """Write a parquet file whose GeoParquet metadata declares `crs`; geometry values are placeholders."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    geo = {
        "version": "1.0.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["LineString"],
                "crs": {"id": {"authority": "EPSG", "code": crs}},
            }
        },
    }
    table = pa.table(columns).replace_schema_metadata({b"geo": json.dumps(geo).encode("utf-8")})
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def write_raster(path: Path, crs: int, values: list[list[float]], nodata: float = -9999.0) -> None:
    """Write a small float32 GeoTIFF."""
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    data = np.array(values, dtype="float32")
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=data.shape[1],
        height=data.shape[0],
        count=1,
        dtype="float32",
        crs=f"EPSG:{crs}",
        transform=from_origin(0, data.shape[0], 1, 1),
        nodata=nodata,
    ) as dataset:
        dataset.write(data, 1)


def write_vrt(path: Path, source: Path, crs: int, size: int = 3) -> None:
    """Write a one-source VRT over `source` (a size x size GeoTIFF), like create_vrt_file does."""
    path.write_text(
        f"""<VRTDataset rasterXSize="{size}" rasterYSize="{size}">
  <SRS>EPSG:{crs}</SRS>
  <GeoTransform>0, 1, 0, {size}, 0, -1</GeoTransform>
  <VRTRasterBand dataType="Float32" band="1">
    <NoDataValue>-9999</NoDataValue>
    <SimpleSource>
      <SourceFilename relativeToVRT="1">{source.name}</SourceFilename>
      <SourceBand>1</SourceBand>
      <SrcRect xOff="0" yOff="0" xSize="{size}" ySize="{size}"/>
      <DstRect xOff="0" yOff="0" xSize="{size}" ySize="{size}"/>
    </SimpleSource>
  </VRTRasterBand>
</VRTDataset>
""",
        encoding="utf-8",
    )

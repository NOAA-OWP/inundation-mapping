import os

from argparse import ArgumentParser
from pathlib import Path

import numpy as np
import rasterio

import whitebox

# Set wbt envs
wbt = whitebox.WhiteboxTools()
wbt.verbose = False

def run_d8flowdir(dem_path, out_p_path, out_sd8_path):

    """
    Using a conditioned DEM, create either a flow dir file (-p), a slope file (-sd) or both
    This is a far faster replacement for TauDEM d8flowdir.
    """

    # Remap ESRI D8 codes to TauDEM 1-8 codes
    # Lookup table: WhiteboxTools (esri_pntr=False) -> TauDEM 1-8
    lut = np.zeros(129, dtype=np.int16)
    lut[1] = 2  # NE
    lut[2] = 1  # E
    lut[4] = 8  # SE
    lut[8] = 7  # S
    lut[16] = 6  # SW
    lut[32] = 5  # W
    lut[64] = 4  # NW
    lut[128] = 3  # N

    tmp_folder, tmp_file_name = os.path.split(dem_path)
    stage_p_path = ""
    stage_sd8_path = ""

    try:

        # TODO: put these on the deny lists.

        # TODO: figure out orig nodata for both Conus and AK (look at dem files incoming. Bash variable)
        # we do not want to open it if we do not have too.

        orig_nodata = "0"  # FOR NOW

        # also check the dtype for the output flow file and slope file both conus and AK

        """
        orig 05030104
            - in dem is float 32 
            - out flow dir is int 16 and no data is ??
            - out slope is float 32 and no data is ??
        
        orig 19030201
            - in dem is 
            - out flow dir is int 16 and no data  ??
            - out slope is 

        """


        # Can do either p (flow dir) or -sd8 slope or both
        # ====================
        # p (Flow Dir) file
        if out_p_path is not None and out_p_path != "":
            stage_p_path = Path(os.path.join(tmp_folder, f"stage_d8_p_{tmp_file_name}"))
            stage_p_path.unlink(missing_ok=True)  # In case residue exists

            # Compute D8 pointer (using TauDEM power-of-1 codes)
            wbt.d8_pointer(dem=dem_path, output=stage_p_path, esri_pntr=False)

            with rasterio.open(stage_p_path) as src:
                profile = src.profile.copy()
                nodata = src.nodata

                print(f"No data for p is {nodata}")
                # for 05030104 is it -32768

                data = src.read(1)

                taudem_p = np.full(data.shape, -32768, dtype=np.int16)
                valid = (data > 0) & (data <= 128)
                if nodata is not None:
                    valid &= data != nodata
                taudem_p[valid] = lut[data[valid].astype(np.int32)]

                profile.update(dtype=rasterio.int16, nodata=-32768, compress="lzw")

                # profile.update(
                #     dtype=rasterio.int16,
                #     nodata=-32768,
                #     compress="zstd",
                #     predictor=2,
                #     zstd_level=6,
                #     tiled=True,
                #     blockxsize=256,
                #     blockysize=256,
                #     BIGTIFF="IF_SAFER",
                # )
            with rasterio.open(out_p_path, "w", **profile) as dst:
                dst.write(taudem_p, 1)

        # ====================
        # sd8 (slope) file
        if out_sd8_path is not None and out_sd8_path != "":
            stage_sd8_path = Path(os.path.join(tmp_folder, f"stage_d8_sd8_{tmp_file_name}"))
            stage_sd8_path.unlink(missing_ok=True)  # In case residue exists

            wbt.d8_slope(dem=dem_path, output=stage_sd8_path, units="percent")
            with rasterio.open(stage_sd8_path) as src:
                s_profile = src.profile.copy()
                s_nodata = src.nodata

                print(f"No data for sd8 is {s_nodata}")

                slope_pct = src.read(1)

                sd8_nodata = float(orig_nodata) if orig_nodata is not None else -1
                taudem_sd8 = slope_pct / 100.0
                if s_nodata is not None:
                    taudem_sd8 = np.where(slope_pct == s_nodata, sd8_nodata, taudem_sd8)

                s_profile.update(
                    dtype=rasterio.float32,
                    nodata=sd8_nodata,
                    compress="lzw",
                    predictor=3,
                    zstd_level=6,
                    tiled=True,
                    blockxsize=256,
                    blockysize=256,
                    BIGTIFF="IF_SAFER",
                )
                with rasterio.open(out_sd8_path, "w", **s_profile) as dst:
                    dst.write(taudem_sd8.astype(np.float32), 1)

    except Exception:
        # catch error and re-raise
        raise

    finally:
        if stage_p_path != "" and isinstance(stage_p_path, Path):
            stage_p_path.unlink(missing_ok=True)  # remove the temp file

        if stage_sd8_path != "" and isinstance(stage_sd8_path, Path):
            stage_sd8_path.unlink(missing_ok=True)  # remove the temp file


if __name__ == '__main__':

    parser = ArgumentParser(description="Process d8flowdir including flow direction and slope."
                            " You can use -p (flow dir), -sd8 (slope) or both.")

    parser.add_argument("-fel", "--dem-path", type=str, required=True, help="REQUIRED: Input filled DEM path")
    parser.add_argument("-p", "--out-p-path", type=str, required=False,
                         help="OPTIONAL: Output flow direction path. BUT... you need either -p, -sd8 or both")
    parser.add_argument("-sd8", "--out-sd8_path", type=str, required=False, help="OPTIONAL: Output slope path")

    args = vars(parser.parse_args())

    run_d8flowdir(**args)
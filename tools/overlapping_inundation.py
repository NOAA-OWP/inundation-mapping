#!/usr/bin/env python
# coding: utf-8

import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from threading import Lock

import geopandas as gpd
import numpy as np
import rasterio
from affine import Affine
from numba import njit
from rasterio.mask import mask
from rasterio.windows import from_bounds
from scipy.optimize import newton


gpd.options.io_engine = "pyogrio"


class OverlapWindowMerge:
    def __init__(self, inundation_rsts, num_partitions=None, window_xy_size=None):
        """
        Initialize the object

        :param inundation_rsts: list of inundation paths or datasets
        :param num_partitions: tuple of integers representing num windows in x and y space
        :param window_xy_size: tuple of integers represeting num of pixels in windows in x an y space
        """

        # sort for largest spanning dataset (todo: handle mismatched resolutions)
        # size_func = lambda x: np.abs(x.bounds.left - x.bounds.right) * np.abs(x.bounds.top - x.bounds.bottom)
        def size_func(x):
            return np.abs(x.bounds.left - x.bounds.right) * np.abs(x.bounds.top - x.bounds.bottom)

        # key_sort_func = lambda x: x["size"]
        def key_sort_func(x):
            return x['size']

        datasets = [rasterio.open(ds) for ds in inundation_rsts]
        ds_dict = [{"dataset": ds, "size": size_func(ds)} for ds in datasets]
        ds_dict.sort(key=key_sort_func, reverse=True)

        # load sample overlapping inundation depth rasters
        self.depth_rsts = [x["dataset"] for x in ds_dict]
        del ds_dict

        self.rst_dims = [[x.height, x.width] for x in self.depth_rsts]

        self.res = self.depth_rsts[0].meta["transform"][0]
        self.depth_bounds = (
            np.array(
                [[[x.bounds.top, x.bounds.left], [x.bounds.bottom, x.bounds.right]] for x in self.depth_rsts]
            )
            / self.res
        )

        # get transform, width, height and bounds
        (self.proc_unit_transform, self.proc_unit_width, self.proc_unit_height, final_bounds) = (
            self.get_final_dims()
        )

        self.proc_unit_bounds = np.array(
            [[final_bounds["top"], final_bounds["left"]], [final_bounds["bottom"], final_bounds["right"]]]
        )

        self.proc_unit_bounds = self.proc_unit_bounds / self.res

        self.lat_lon_sign = [
            np.sign(self.proc_unit_bounds[1, 0] - self.proc_unit_bounds[0, 0]),
            np.sign(self.proc_unit_bounds[1, 1] - self.proc_unit_bounds[0, 1]),
        ]

        self.partitions = num_partitions
        self.window_sizes = window_xy_size

    @staticmethod
    def __vprint(message, verbose):
        if verbose:
            print(message)

    @staticmethod
    @njit
    def get_res_bbox_min(x, v, z, y):
        """
        Optimize for bounds that fit the final resolution

        :param x: float of compare
        :param v: float representing min bound
        :param z: float representing max bound
        :param y: float representing resolution
        """
        return np.abs(z - x) - np.round(np.abs(z - v) / y) * y

    def get_final_dims(self):
        """
        Get transform, width, height, and bbox of final dataset

        :return: Affine transform, int width, int height, dict bounds
        """

        left = np.min([d.bounds.left for d in self.depth_rsts])
        top = np.max([d.bounds.top for d in self.depth_rsts])
        right = np.max([d.bounds.right for d in self.depth_rsts])
        bottom = np.min([d.bounds.bottom for d in self.depth_rsts])

        left = newton(self.get_res_bbox_min, left, args=(left, right, self.res))
        bottom = newton(self.get_res_bbox_min, bottom, args=(bottom, top, self.res))

        transform = self.depth_rsts[0].meta["transform"]

        width = int(np.abs(right - left) / self.res)
        height = int(np.abs(top - bottom) / self.res)
        new_transform = Affine(transform[0], transform[1], left, transform[3], transform[4], top)

        return new_transform, width, height, {"left": left, "top": top, "right": right, "bottom": bottom}

    def get_window_coords(self):
        """
        Return ul/br bounds of window and its respective window idx

        :param partitions: tuple or list of partition sizes for x and y
        :param sizes: tuple or list of pixel sizes for x and y
        :return: list of ul/br bounds of window, int of respective window idx
        """

        # Set up desired number of partitions (can also be set pixel size)
        if self.partitions is not None:
            x_res, y_res = self.partitions
        elif self.window_sizes is not None:
            x_res, y_res = self.window_sizes
        else:
            raise ("in bran crunch")

        # Get window widths (both normal and edge windows)
        window_width1 = np.repeat(int(self.proc_unit_width / x_res), x_res) * self.lat_lon_sign[1]
        window_width2 = window_width1.copy()
        window_width2[-1] += self.proc_unit_width - window_width1[0] * x_res * self.lat_lon_sign[1]

        # Get window heights (both normal and edge windows)
        window_height1 = np.repeat(int(self.proc_unit_height / y_res), y_res) * self.lat_lon_sign[0]
        window_height2 = window_height1.copy()
        window_height2[-1] += self.proc_unit_height - window_height1[0] * y_res * self.lat_lon_sign[0]

        # Get window sizes (both normal and edge windows)
        window_bounds1 = np.flip(
            np.array(np.meshgrid(window_width1, window_height1)).T.reshape(-1, 2), axis=1
        ).astype(int)
        window_bounds2 = np.flip(
            np.array(np.meshgrid(window_width2, window_height2)).T.reshape(-1, 2), axis=1
        ).astype(int)

        window_idx = np.array(np.unravel_index(np.arange(y_res * x_res), (y_res, x_res), order="F"))

        return [window_bounds1, window_bounds2], window_idx

    def create_lat_lons(self, window_bounds, window_idx):
        """
        Return bbox of window and list of latitudes and longitudes

        :param window_bounds: tuple or list of partition sizes for x and y
        :param window_idx: int representing index of window
        :return: list of float latitudes, list of float longitudes, list of window bbox,
                    list of ul/br coords for window
        """

        upper_left = window_idx.T * window_bounds[0]
        lower_right = upper_left + window_bounds[1]

        # Merge point arrays, convert back to original units, and get drawable path for each window
        bbox = np.hstack([upper_left, lower_right])
        scaled_path_points = [
            np.array(np.meshgrid([st[0], st[2]], [st[1], st[3]])).T.reshape(-1, 2) for st in bbox
        ]
        path_points = (scaled_path_points + self.proc_unit_bounds[0]) * self.res

        # Create arange of latitudes and longitudes and add half of window size
        latitudes = np.arange(
            self.proc_unit_bounds[0, 0],
            self.proc_unit_bounds[1, 0] + self.lat_lon_sign[0],
            window_bounds[1][0][0],
        )[:-1] + (window_bounds[1][0][0] / 2)
        longitudes = np.arange(
            self.proc_unit_bounds[0, 1],
            self.proc_unit_bounds[1, 1] + self.lat_lon_sign[1],
            window_bounds[1][0][1],
        )[:-1] + (window_bounds[1][0][1] / 2)

        return latitudes, longitudes, path_points, bbox

    @staticmethod
    def get_window_idx(latitudes, longitudes, coords, partitions):
        """
        Return raveled window indices

        :param latitudes: list of latitudes within bounds
        :param longitudes: list of longitudes within bounds

        :return: ndarray of raveled multi indexes
        """
        # Get difference of upper-left and lower-right boundaries and computed lat lons
        lat_dif = [np.abs(latitudes - coords[0, 0]), np.abs(latitudes - coords[1, 0])]
        lon_dif = [np.abs(longitudes - coords[0, 1]), np.abs(longitudes - coords[1, 1])]

        # Create range between the closest idx for both lats and lons
        lon_range = np.arange(np.argmin(lon_dif[0]), np.argmin(lon_dif[1]) + 1)
        lat_range = np.arange(np.argmin(lat_dif[0]), np.argmin(lat_dif[1]) + 1)

        # Create mesh grid for each possible set of coords and ravel to get window idx
        grid = np.array(np.meshgrid(lat_range, lon_range)).T.reshape(-1, 2)
        del lon_range, lat_range, lat_dif, lon_dif
        return np.ravel_multi_index([grid[:, 0], grid[:, 1]], partitions, order="F")

    def read_rst_data(self, win_idx, datasets, path_points, bbox, meta):
        """
        Return data windows and final bounds of window

        :param win_idx: int window index
        :param datasets: list of int representing dataset inx
        :param path_points: list of bbox for windows
        :param bbox: list of ul/br coords of windows
        :param meta: metadata for final dataset

        :return: rasterio window object for final window, rasterio window of data window bounds,
        data for each raster in window,
        """
        # Get window bounding box and get final array output dimensions
        window = path_points[win_idx]
        window_height, window_width = np.array(
            [np.abs(bbox[win_idx][2] - bbox[win_idx][0]), np.abs(bbox[win_idx][3] - bbox[win_idx][1])]
        ).astype(int)

        bnds = []
        data = []
        for ds in datasets:
            # Get rasterio window for each pair of window bounds and depth dataset

            bnd = from_bounds(
                window[0][1],
                window[-1][0],
                window[-1][1],
                window[0][0],
                transform=self.depth_rsts[ds].transform,
            )

            bnds.append(bnd)

            # Read raster data with window
            read_data = self.depth_rsts[ds].read(1, window=bnd).astype(np.float32)
            # Convert all no data to nan values
            read_data[read_data == np.float32(self.depth_rsts[ds].meta["nodata"])] = np.nan
            data.append(read_data)
            del bnd

        final_bnds = from_bounds(
            window[0][1], window[-1][0], window[-1][1], window[0][0], transform=meta["transform"]
        )

        return [final_bnds, bnds, data]

    def merge_rasters(self, out_fname, nodata=-9999, threaded=False, workers=4, quiet=True):
        """
        Merge multiple raster datasets

        :param out_fname: str path for final merged dataset
        :param nodata: int/float representing no data value
        """

        window_bounds, window_idx = self.get_window_coords()
        latitudes, longitudes, path_points, bbox = self.create_lat_lons(window_bounds, window_idx)

        windows = [
            self.get_window_idx(latitudes, longitudes, coords, self.partitions)
            for coords in self.depth_bounds
        ]

        # Create dict with window idx key and dataset idx vals
        data_dict = {}
        for idx, win in enumerate(windows):
            for win_idx in win:
                if win_idx in data_dict:
                    data_dict[win_idx].append(idx)
                else:
                    data_dict[win_idx] = [idx]

        agg_function = partial(np.nanmax, axis=0)

        meta = self.depth_rsts[0].meta

        meta.update(
            transform=self.proc_unit_transform,
            width=self.proc_unit_width,
            height=self.proc_unit_height,
            nodata=nodata,
            blockxsize=256,
            blockysize=256,
            tiled=True,
            compress="lzw",
        )

        def __data_generator(data_dict, path_points, bbox, meta):
            for key, val in data_dict.items():
                f_window, window, dat = self.read_rst_data(key, val, path_points, bbox, meta)
                yield (dat, window, f_window, val)
                # final_windows.append(f_window)
                # data_windows.append(window)
                # data.append(dat)
                # del f_window, window, dat

        # create data generator
        dgen = __data_generator(data_dict, path_points, bbox, meta)

        lock = Lock()

        with rasterio.open(out_fname, "w", **meta) as rst:
            merge_partial = partial(
                merge_data,
                rst=rst,
                lock=lock,
                dtype=meta["dtype"],
                agg_function=agg_function,
                nodata=meta["nodata"],
                rst_dims=self.rst_dims,
            )

            if not threaded:
                # for d, dw, fw, ddict in zip(data,
                #                            data_windows,
                #                            final_windows,
                #                            data_dict.values()):
                for d, dw, fw, ddict in dgen:
                    merge_partial(d, dw, fw, ddict)
            else:
                executor = ThreadPoolExecutor(max_workers=workers)
                results = {executor.submit(merge_partial, *wg): 1 for wg in dgen}

                for future in as_completed(results):
                    try:
                        future.result()
                    except Exception as exc:
                        self.__vprint("Exception {} for {}".format(exc, results[future]), not quiet)
                    else:
                        if results[future] is not None:
                            self.__vprint("... {} complete".format(results[future]), not quiet)
                        else:
                            self.__vprint("... complete", not quiet)

    def mask_mosaic(self, mosaic, polys, polys_layer=None, outfile=None):
        # rem_array,window_transform = mask(rem,[shape(huc['geometry'])],crop=True,indexes=1)

        # input rem
        if isinstance(mosaic, str):
            mosaic = rasterio.open(mosaic)
        elif isinstance(mosaic, rasterio.DatasetReader):
            pass
        else:
            raise TypeError("Pass rasterio dataset or filepath for mosaic")

        if isinstance(polys, str):
            polys = gpd.read_file(polys, layer=polys_layer)
        elif isinstance(polys, gpd.GeoDataFrame):
            pass
        else:
            raise TypeError("Pass geopandas dataset or filepath for catchment polygons")

        # fossid = huc['properties']['fossid']
        # if polys.HydroID.dtype != 'str': polys.HydroID = polys.HydroID.astype(str)
        # polys=polys[polys.HydroID.str.startswith(fossid)]
        mosaic_array, window_transform = mask(mosaic, polys["geometry"], crop=True, indexes=1)

        if outfile:
            out_profile = mosaic.profile
            out_profile.update(
                height=mosaic_array.shape[0],
                width=mosaic_array.shape[1],
                transform=window_transform,
                driver="GTiff",
                blockxsize=256,
                blockysize=256,
                tiled=True,
                compress="lzw",
            )

            with rasterio.open(outfile, "w", **out_profile) as otfi:
                otfi.write(mosaic_array, indexes=1)

        return (mosaic_array, out_profile)


# Quasi multi write
# Throughput achieved assuming processing time is not identical between windows
# and queued datasets, preferably approx N/2 threads for 9 windows
# @njit
def merge_data(
    rst_data, window_bnds, final_window, datasets, dtype, rst, lock, agg_function, nodata, rst_dims
):
    """
    Merge data in to final dataset (multi threaded)

    :param rst_data: list of rst data from window
    :param window_bnds: list rasterio windows representing data window bounds
    :param final_window: rasterio window representing final window bounds
    :param datasets: list of int representing dataset idx
    :param dtype: data type of final output
    :param rst: rasterio writer for final dataset
    :param lock: thread concurrency lock
    :param agg_function: function to aggregate datasets
    :param nodata: nodata of final output
    :param rst_dims: dimensions of overlapping rasters
    """

    nan_tile = np.array([np.nan])
    window_data = np.tile(float(nan_tile), [int(final_window.height), int(final_window.width)])

    for data, bnds, idx in zip(rst_data, window_bnds, datasets):
        # Get indices to apply to base

        col_slice = slice(
            int(np.max([0, np.ceil(bnds.col_off * -1)])),
            int(np.min([bnds.width, rst_dims[idx][1] - bnds.col_off])),
        )

        row_slice = slice(
            int(np.max([0, np.ceil(bnds.row_off * -1)])),
            int(np.min([bnds.height, rst_dims[idx][0] - bnds.row_off])),
        )

        win_shape = window_data[row_slice, col_slice].shape

        if not np.all(np.sign(np.array(win_shape) - np.array(data.shape)) > 0):
            data = data[: win_shape[0], : win_shape[1]]
        # Assign the data to the base array with aggregate function
        merge = [window_data[row_slice, col_slice], data]

        del data

        with warnings.catch_warnings():
            # This `with` block supresses the RuntimeWarning thrown by numpy when aggregating nan values
            warnings.simplefilter("ignore", category=RuntimeWarning)
            window_data[row_slice, col_slice] = agg_function(merge)

        window_data[np.isnan(window_data)] = nodata
        del merge

    del rst_data, window_bnds, datasets

    window_data[(window_data == nan_tile) | (np.isnan(window_data))] = nodata

    with lock:
        rst.write_band(1, window_data.astype(dtype), window=final_window)
    del window_data


if __name__ == "__main__":
    # import tracemalloc
    import glob
    import time

    # print('start', time.localtime())
    # project_path = r'../documentation/data'
    # overlap = OverlapWindowMerge([project_path + '/overlap1.tif',
    #                               project_path + '/overlap2.tif',
    #                               project_path + '/overlap3.tif',
    #                              ],
    #                              (3, 3))
    # overlap.merge_rasters(project_path + '/merged_overlap.tif', nodata=0)
    # print('end', time.localtime())
    # tracemalloc.start()
    print("start", time.localtime())
    # project_path = r'../documentation/data'
    # project_path = '*/mosaicing_data/1_fr_ms_composite'
    # overlap = OverlapWindowMerge([project_path + '/inundation_extent_12090301_FR.tif',
    #                               project_path + '/inundation_extent_12090301_MS.tif'
    #                              ],
    #                              (30, 30))
    # overlap.merge_rasters(project_path + '/merged_final5.tif', threaded=True, workers=4, nodata=0)

    # tracemalloc.start()
    print("start", time.localtime())
    # project_path = r'../documentation/data'
    # project_path = '*/mosaicing_data/2_gms'
    # a = glob.glob(project_path + '/inundation*.tif')
    # overlap = OverlapWindowMerge(a,
    #                              (30, 30))
    # overlap.merge_rasters(project_path + '/merged_final5.tif', threaded=True, workers=4, nodata=-2e9)
    # current, peak = tracemalloc.get_traced_memory()
    # print(f"Current memory usage is {current / 10 ** 6}MB; Peak was {peak / 10 ** 6}MB")
    # tracemalloc.stop()

    project_path = "*"
    overlap = OverlapWindowMerge(
        [project_path + "/nwm_resampled.tif", project_path + "/rnr_inundation_031403_2020092000.tif"], (1, 1)
    )
    overlap.merge_rasters(project_path + "/merged_final5.tif", threaded=False, workers=4)

    print("end", time.localtime())

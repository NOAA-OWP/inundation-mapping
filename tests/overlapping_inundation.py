#!/usr/bin/env python
# coding: utf-8

import rasterio
from rasterio.windows import from_bounds
import numpy as np
from functools import partial
from affine import Affine
from scipy.optimize import newton
from threading import Lock
import concurrent.futures
from numba import njit
from glob import iglob


class OverlapWindowMerge:

    def __init__(self,
                 inundation_rsts,
                 num_partitions=None,
                 window_xy_size=None):
        """
        Initialize the object

        :param inundation_rsts: list of inundation paths or datasets
        :param num_partitions: tuple of integers representing num windows in x and y space
        :param window_xy_size: tuple of integers represeting num of pixels in windows in x an y space
        """

        # sort for largest spanning dataset (todo: handle mismatched resolutions)
        size_func = lambda x: np.abs(x.bounds.left - x.bounds.right) * \
                              np.abs(x.bounds.top - x.bounds.bottom)
        key_sort_func = lambda x: x['size']
        datasets = [rasterio.open(ds) for ds in inundation_rsts]
        ds_dict = [{'dataset': ds, 'size': size_func(ds)} for ds in datasets]
        ds_dict.sort(key=key_sort_func, reverse=True)

        # load sample overlapping inundation depth rasters
        self.depth_rsts = [x['dataset'] for x in ds_dict]
        del ds_dict

        self.rst_dims = [[x.height, x.width] for x in self.depth_rsts]

        self.res = self.depth_rsts[0].meta['transform'][0]
        self.depth_bounds = np.array([[[x.bounds.top,
                                        x.bounds.left],
                                       [x.bounds.bottom,
                                        x.bounds.right]] for x in self.depth_rsts]) / self.res


        # get transform, width, height and bounds
        self.proc_unit_transform, self.proc_unit_width, \
        self.proc_unit_height, final_bounds = \
        self.get_final_dims()

        self.proc_unit_bounds = np.array([[final_bounds['top'],
                                           final_bounds['left']],
                                          [final_bounds['bottom'],
                                           final_bounds['right']]])

        self.proc_unit_bounds = self.proc_unit_bounds / self.res

        self.lat_lon_sign = [np.sign(self.proc_unit_bounds[1, 0] - self.proc_unit_bounds[0, 0]),
                             np.sign(self.proc_unit_bounds[1, 1] - self.proc_unit_bounds[0, 1])]

        self.partitions = num_partitions
        self.window_sizes = window_xy_size

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

        :param x: float of compare
        :param v: float representing min bound
        :param z: float representing max bound
        :param y: float representing resolution
        :return: Affine transform, int width, int height, dict bounds
        """

        left = np.min([d.bounds.left for d in self.depth_rsts])
        top = np.max([d.bounds.top for d in self.depth_rsts])
        right = np.max([d.bounds.right for d in self.depth_rsts])
        bottom = np.min([d.bounds.bottom for d in self.depth_rsts])

        left = newton(self.get_res_bbox_min, left, args=(left, right, self.res))
        bottom = newton(self.get_res_bbox_min, bottom, args=(bottom, top, self.res))

        transform = self.depth_rsts[0].meta['transform']

        width = int(np.abs(right - left) / self.res)
        height = int(np.abs(top - bottom) / self.res)
        new_transform = Affine(transform[0],
                               transform[1],
                               left,
                               transform[3],
                               transform[4],
                               top)

        return new_transform, width, height, {'left': left,
                                              'top': top,
                                              'right': right,
                                              'bottom': bottom}

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
            raise('in bran crunch')

        # Get window widths (both normal and edge windows)
        window_width1 = np.repeat(int(self.proc_unit_width / x_res), x_res) * self.lat_lon_sign[1]
        window_width2 = window_width1.copy()
        window_width2[-1] += self.proc_unit_width - window_width1[0] * x_res * self.lat_lon_sign[1]

        # Get window heights (both normal and edge windows)
        window_height1 = np.repeat(int(self.proc_unit_height / y_res), y_res) * self.lat_lon_sign[0]
        window_height2 = window_height1.copy()
        window_height2[-1] += self.proc_unit_height - window_height1[0] * y_res * self.lat_lon_sign[0]

        # Get window sizes (both normal and edge windows)
        window_bounds1 = np.flip(np.array(np.meshgrid(window_width1,
                                                      window_height1)).T.reshape(-1, 2),
                               axis=1).astype(np.int)
        window_bounds2 = np.flip(np.array(np.meshgrid(window_width2,
                                                      window_height2)).T.reshape(-1, 2),
                               axis=1).astype(np.int)

        window_idx = np.array(np.unravel_index(np.arange(y_res * x_res), (y_res, x_res), order='F'))

        return [window_bounds1, window_bounds2], window_idx

    def create_lat_lons(self,
                        window_bounds,
                        window_idx):
        """
        Return bbox of window and list of latitudes and longitudes

        :param window_bounds: tuple or list of partition sizes for x and y
        :param window_idx: int representing index of window
        :return: list of float latitudes, list of float longitudes, list of window bbox, list of ul/br coords for window
        """

        upper_left = (window_idx.T * window_bounds[0])
        lower_right = upper_left + window_bounds[1]

        # Merge point arrays, convert back to original units, and get drawable path for each window
        bbox = np.hstack([upper_left, lower_right])
        scaled_path_points = [np.array(np.meshgrid([st[0], st[2]], [st[1], st[3]])).T.reshape(-1, 2) for st in bbox]
        path_points = (scaled_path_points + self.proc_unit_bounds[0]) * self.res

        # Create arange of latitudes and longitudes and add half of window size
        latitudes = np.arange(self.proc_unit_bounds[0, 0],
                              self.proc_unit_bounds[1, 0] + self.lat_lon_sign[0],
                              window_bounds[1][0][0])[:-1] + (window_bounds[1][0][0] / 2)
        longitudes = np.arange(self.proc_unit_bounds[0, 1],
                               self.proc_unit_bounds[1, 1] + self.lat_lon_sign[1],
                               window_bounds[1][0][1])[:-1] + (window_bounds[1][0][1] / 2)

        return latitudes, longitudes, path_points, bbox

    @staticmethod
    def get_window_idx(latitudes,
                       longitudes,
                       coords,
                       partitions):
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
        return np.ravel_multi_index([grid[:, 0], grid[:, 1]], partitions, order='F')

    def read_rst_data(self,
                      win_idx,
                      datasets,
                      path_points,
                      bbox,
                      meta):
        """
        Return data windows and final bounds of window

        :param wind_idx: int window index
        :param datasets: list of int representing dataset inx
        :param path_points: list of bbox for windows
        :param bbox: list of ul/br coords of windows
        :param meta: metadata for final dataset

        :return: rasterio window object for final window, rasterio window of data window bounds,
        data for each raster in window,
        """
        # Get window bounding box and get final array output dimensions
        window = path_points[win_idx]
        window_height, window_width = np.array([np.abs(bbox[win_idx][2] - bbox[win_idx][0]),
                                                np.abs(bbox[win_idx][3] - bbox[win_idx][1])]).astype(np.int)

        bnds = []
        data = []
        for ds in datasets:
            # Get rasterio window for each pair of window bounds and depth dataset

            bnd = from_bounds(window[0][1],
                              window[-1][0],
                              window[-1][1],
                              window[0][0],
                              transform=self.depth_rsts[ds].transform,
                              height=window_height,
                              width=window_width)

            bnds.append(bnd)

            # Read raster data with window
            data.append(self.depth_rsts[ds].read(1, window=bnd).astype(np.float32))
            del bnd

        final_bnds = from_bounds(window[0][1],
                                 window[-1][0],
                                 window[-1][1],
                                 window[0][0],
                                 transform=meta['transform'],
                                 height=window_height,
                                 width=window_width)

        return [final_bnds, bnds, data]

    def merge_rasters(self, out_fname, nodata=-9999, threaded=False, workers=4):
        """
        Merge multiple raster datasets

        :param out_fname: str path for final merged dataset
        :param nodata: int/float representing no data value
        """

        window_bounds, window_idx = self.get_window_coords()
        latitudes, longitudes, path_points, bbox = self.create_lat_lons(window_bounds,
                                                                        window_idx)
        windows = [self.get_window_idx(latitudes,
                                       longitudes,
                                       coords,
                                       self.partitions)
                   for coords in self.depth_bounds]

        # Create dict with window idx key and dataset idx vals
        data_dict = {}
        for idx, win in enumerate(windows):
            for win_idx in win:
                if win_idx in data_dict:
                    data_dict[win_idx].append(idx)
                else:
                    data_dict[win_idx] = [idx]

        agg_function = partial(np.max, axis=0)

        meta = self.depth_rsts[0].meta

        meta.update(transform=self.proc_unit_transform,
                    width=self.proc_unit_width,
                    height=self.proc_unit_height,
                    nodata=nodata)

        final_windows, data_windows, data = [], [], []
        for key, val in data_dict.items():

          f_window, window, dat = self.read_rst_data(key,
                                    val,
                                    path_points,
                                    bbox,
                                    meta
                                    )
          final_windows.append(f_window)
          data_windows.append(window)
          data.append(dat)
          del f_window, window, dat

        lock = Lock()

        with rasterio.open(out_fname, 'w', **meta) as rst:

            merge_partial = partial(merge_data,
                             rst=rst,
                             lock=lock,
                             dtype=meta['dtype'],
                             agg_function=agg_function,
                             nodata=meta['nodata'],
                             rst_dims=self.rst_dims)

            if not threaded:
                for d, dw, fw, ddict in zip(data,
                                            data_windows,
                                            final_windows,
                                            data_dict.values()):
                    merge_partial(d, dw, fw, ddict)
            else:
                with concurrent.futures.ThreadPoolExecutor(
                        max_workers=workers
                ) as executor:
                    executor.map(merge_partial,
                                 data,
                                 data_windows,
                                 final_windows,
                                 data_dict.values()
                                 )


# Quasi multi write
# Throughput achieved assuming processing time is not identical between windows
# and queued datasets, preferably approx N/2 threads for 9 windows
# @njit
def merge_data(rst_data,
               window_bnds,
               final_window,
               datasets,
               dtype,
               rst,
               lock,
               agg_function,
               nodata,
               rst_dims
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

    nodata = np.array([nodata]).astype(dtype)[0]
    window_data = np.tile(float(nodata), [int(final_window.height), int(final_window.width)])

    for data, bnds, idx in zip(rst_data, window_bnds, datasets):
        # Get indices to apply to base

        col_slice = slice(int(np.max([0,
                                      np.ceil(bnds.col_off * -1)])),
                          int(np.min([bnds.width,
                                      rst_dims[idx][1] - bnds.col_off])))

        row_slice = slice(int(np.max([0,
                                      np.ceil(bnds.row_off * -1)])),
                          int(np.min([bnds.height,
                                      rst_dims[idx][0] - bnds.row_off])))

        # Assign the data to the base array with aggregate function
        merge = [window_data[row_slice,
                                   col_slice],
                    data]

        del data
        window_data[row_slice, col_slice] = agg_function(merge)
        del merge

    del rst_data, window_bnds, datasets

    with lock:
        rst.write_band(1, window_data.astype(dtype), window=final_window)
    del window_data


if __name__ == '__main__':
    import time
    # import tracemalloc

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
    print('start', time.localtime())
    project_path = r'../documentation/data'
    overlap = OverlapWindowMerge([project_path + '/rnr_inundation_031403_2020092000.tif',
                                  project_path + '/nwm_resampled.tif'
                                 ],
                                 (30, 30))
    overlap.merge_rasters(project_path + '/merged_final5.tif', threaded=True, workers=4)
    # current, peak = tracemalloc.get_traced_memory()
    # print(f"Current memory usage is {current / 10 ** 6}MB; Peak was {peak / 10 ** 6}MB")
    # tracemalloc.stop()
    print('end', time.localtime())

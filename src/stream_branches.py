#!/usr/bin/env python3

import os
import sys
from collections import deque
from os.path import isfile, splitext
from random import sample

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from fiona.errors import DriverError
from rasterio.io import DatasetReader
from rasterio.mask import mask
from scipy.stats import mode
from shapely.geometry import LineString, MultiLineString, MultiPoint, Point
from shapely.ops import linemerge, unary_union
from shapely.strtree import STRtree
from tqdm import tqdm

from utils.fim_enums import FIM_exit_codes
from utils.shared_variables import PREP_CRS


gpd.options.io_engine = "pyogrio"


class StreamNetwork(gpd.GeoDataFrame):
    """
    Notes:
        - Many of the methods support two attributes called branch_id_attribute and values_excluded.
          This can be used to filter out records.
            ie) When calling the nwm_subset_streams.gpkg, you can filter some records like this:
                StreamNetwork.from_file(filename=outputs/<some huc>/nwm_subset_streams.gpkg,
                                                 branch_id_attribute="order_",
                                                 values_excluded=[1,2]
                (which means drop all records that have an order_ of 1 or 2.

        - Note: from_file is using its branch_id_attribute and values_excluded as intended but other
             methods may not be incomplete and not filtering as expected.
    """

    geom_name = "geometry"  # geometry attribute name
    branch_id_attribute = None  # branch id attribute name
    values_excluded = None
    attribute_excluded = None

    def __init__(self, *args, **kwargs):
        if kwargs:
            branch_id_attribute = kwargs.pop("branch_id_attribute", None)
            values_excluded = kwargs.pop("values_excluded", None)
            attribute_excluded = kwargs.pop("attribute_excluded", None)

        super().__init__(*args, **kwargs)

        self.branch_id_attribute = branch_id_attribute
        self.values_excluded = values_excluded
        self.attribute_excluded = attribute_excluded

    @classmethod
    def from_file(
        cls,
        filename,
        branch_id_attribute=None,
        values_excluded=None,
        attribute_excluded=None,
        verbose=False,
        *args,
        **kwargs,
    ):
        """loads stream network from file to streamnetwork geopandas"""

        if kwargs:
            inputs = {
                "branch_id_attribute": kwargs.pop("branch_id_attribute", None),
                "values_excluded": kwargs.pop("values_excluded", None),
                "attribute_excluded": kwargs.pop("attribute_excluded", None),
            }

            verbose = kwargs.pop("verbose", None)
        else:
            inputs = {
                "branch_id_attribute": branch_id_attribute,
                "values_excluded": values_excluded,
                "attribute_excluded": attribute_excluded,
            }

        if verbose:
            print("Loading file")

        raw_df = gpd.read_file(filename, *args, **kwargs)

        # Reproject
        if raw_df.crs.to_authority() != PREP_CRS.to_authority():
            raw_df.to_crs(PREP_CRS)

        filtered_df = gpd.GeoDataFrame()

        if (branch_id_attribute is not None) and (values_excluded is not None):
            filtered_df = raw_df[~raw_df[branch_id_attribute].isin(values_excluded)]
        else:
            filtered_df = raw_df

        if verbose:
            print("======" + filename)
            print("Number of df rows = " + str(filtered_df.shape[0]))

        return cls(filtered_df, **inputs)

    def GeoDataFrame_to_StreamNetwork(self, gdf):
        branch_id_attribute = self.branch_id_attribute
        attribute_excluded = self.attribute_excluded
        values_excluded = self.values_excluded
        crs = self.crs

        self = gdf
        self = self.set_crs(crs)

        self = StreamNetwork(
            self,
            branch_id_attribute=branch_id_attribute,
            attribute_excluded=attribute_excluded,
            values_excluded=values_excluded,
        )
        return self

    def write(self, fileName, layer=None, index=True, verbose=False):
        """Gets driver Name from file extension for Geopandas writing"""

        if verbose:
            print("Writing to {}".format(fileName))

        # sets driver
        driverDictionary = {".gpkg": "GPKG", ".geojson": "GeoJSON", ".shp": "ESRI Shapefile"}
        driver = driverDictionary[splitext(fileName)[1]]

        self.to_file(fileName, driver=driver, layer=layer, index=index, engine='fiona')

    def set_index(self, reach_id_attribute, drop=True):
        branch_id_attribute = self.branch_id_attribute
        attribute_excluded = self.attribute_excluded
        values_excluded = self.values_excluded
        crs = self.crs

        self = super(gpd.GeoDataFrame, self)
        self = self.set_index(reach_id_attribute, drop=drop)
        self = self.set_crs(crs)

        self = StreamNetwork(
            self,
            branch_id_attribute=branch_id_attribute,
            attribute_excluded=attribute_excluded,
            values_excluded=values_excluded,
        )
        return self

    def reset_index(self, drop=True):
        branch_id_attribute = self.branch_id_attribute
        attribute_excluded = self.attribute_excluded
        values_excluded = self.values_excluded
        crs = self.crs

        self = super(gpd.GeoDataFrame, self)
        self = self.reset_index(drop=drop)
        self = self.set_crs(crs)

        self = StreamNetwork(
            self,
            branch_id_attribute=branch_id_attribute,
            attribute_excluded=attribute_excluded,
            values_excluded=values_excluded,
        )
        return self

    def drop(self, labels=None, axis=0):
        branch_id_attribute = self.branch_id_attribute
        attribute_excluded = self.attribute_excluded
        values_excluded = self.values_excluded

        self = super(gpd.GeoDataFrame, self)
        self = self.drop(labels=labels, axis=axis)

        self = StreamNetwork(
            self,
            branch_id_attribute=branch_id_attribute,
            attribute_excluded=attribute_excluded,
            values_excluded=values_excluded,
        )
        return self

    def rename(self, columns):
        branch_id_attribute = self.branch_id_attribute
        attribute_excluded = self.attribute_excluded
        values_excluded = self.values_excluded

        self = super(gpd.GeoDataFrame, self)
        self = self.rename(columns=columns)

        self = StreamNetwork(
            self,
            branch_id_attribute=branch_id_attribute,
            attribute_excluded=attribute_excluded,
            values_excluded=values_excluded,
        )
        return self

    def dissolve(self, by=None):
        branch_id_attribute = self.branch_id_attribute
        attribute_excluded = self.attribute_excluded
        values_excluded = self.values_excluded
        crs = self.crs
        geometry = self.geometry

        self = gpd.GeoDataFrame(self, crs=crs, geometry=geometry)
        self = self.dissolve(by=by)

        self = StreamNetwork(
            self,
            branch_id_attribute=branch_id_attribute,
            attribute_excluded=attribute_excluded,
            values_excluded=values_excluded,
        )
        return self

    def apply(self, *args, **kwargs):
        branch_id_attribute = self.branch_id_attribute
        attribute_excluded = self.attribute_excluded
        values_excluded = self.values_excluded
        crs = self.crs

        self = super().apply(*args, **kwargs)
        self = self.set_crs(crs)

        self = StreamNetwork(
            self,
            branch_id_attribute=branch_id_attribute,
            attribute_excluded=attribute_excluded,
            values_excluded=values_excluded,
        )

        return self

    def multilinestrings_to_linestrings(self):
        branch_id_attribute = self.branch_id_attribute
        attribute_excluded = self.attribute_excluded
        values_excluded = self.values_excluded

        def convert_to_linestring(row):
            geometry = row["geometry"]

            if isinstance(geometry, MultiLineString):
                linestring = LineString(sum([list(item.coords) for item in list(geometry.geoms)], []))
                row["geometry"] = linestring

            return row

        self = StreamNetwork(
            self.apply(convert_to_linestring, axis=1),
            branch_id_attribute=branch_id_attribute,
            attribute_excluded=attribute_excluded,
            values_excluded=values_excluded,
        )

        return self

    def explode(self, **kwargs):
        branch_id_attribute = self.branch_id_attribute
        attribute_excluded = self.attribute_excluded
        values_excluded = self.values_excluded

        self = StreamNetwork(
            super().explode(**kwargs),
            branch_id_attribute=branch_id_attribute,
            attribute_excluded=attribute_excluded,
            values_excluded=values_excluded,
        )

        return self

    def to_df(self, *args, **kwargs):
        """Converts back to dataframe"""

        self = pd.DataFrame(self, *args, **kwargs)

        return self

    def merge(self, *args, **kwargs):
        branch_id_attribute = self.branch_id_attribute
        attribute_excluded = self.attribute_excluded
        values_excluded = self.values_excluded

        self = super().merge(*args, **kwargs)

        self = StreamNetwork(
            self,
            branch_id_attribute=branch_id_attribute,
            attribute_excluded=attribute_excluded,
            values_excluded=values_excluded,
        )

        return self

    def merge_stream_branches(
        self,
        stream_branch_dataset,
        on="ID",
        branch_id_attribute="LevelPathI",
        attributes="StreamOrde",
        stream_branch_layer_name=None,
    ):
        """Merges stream branch id attribute from another vector file"""

        # load vaas
        if isinstance(stream_branch_dataset, str):
            stream_branch_dataset = gpd.read_file(stream_branch_dataset, layer=stream_branch_layer_name)
        elif isinstance(stream_branch_dataset, gpd.GeoDataFrame):
            pass
        else:
            raise TypeError("Pass stream_branch_dataset argument as filepath or GeoDataframe")

        # merge and drop duplicate columns
        if isinstance(attributes, list):
            what = [on] + [branch_id_attribute] + attributes
        elif isinstance(attributes, str):
            what = [on] + [branch_id_attribute] + [attributes]

        self = self.merge(stream_branch_dataset[what], on=on, how="inner")

        # make sure it's the correct object type
        self = StreamNetwork(self, branch_id_attribute=branch_id_attribute)

        return self

    @staticmethod
    def flip_inlet_outlet_linestring_index(linestring_index):
        # returns -1 for 0 and 0 for -1
        inlet_outlet_linestring_index_dict = {0: -1, -1: 0}

        try:
            return inlet_outlet_linestring_index_dict[linestring_index]
        except KeyError:
            raise ValueError("Linestring index should be 0 or -1")

    def derive_nodes(
        self,
        toNode_attribute="ToNode",
        fromNode_attribute="FromNode",
        reach_id_attribute="ID",
        outlet_linestring_index=0,
        node_prefix=None,
        max_node_digits=8,
        verbose=False,
    ):
        if verbose:
            print("Deriving nodes ...")

        inlet_linestring_index = StreamNetwork.flip_inlet_outlet_linestring_index(outlet_linestring_index)

        # set node prefix to string
        if node_prefix is None:
            node_prefix = ""

        # handle digits and values for node ids
        max_post_node_digits = max_node_digits - len(node_prefix)
        max_node_value = int("9" * max_post_node_digits)

        # sets index of stream branches as reach id attribute
        # if self.index.name != reach_id_attribute:
        # self = self.set_index(reach_id_attribute,drop=True)

        # inlet_coordinates, outlet_coordinates = dict(), dict()
        node_coordinates = dict()
        toNodes, fromNodes = [None] * len(self), [None] * len(self)
        current_node_id = "1".zfill(max_post_node_digits)

        for i, (_, row) in enumerate(self.iterrows()):
            reach_id = row[reach_id_attribute]

            # get foss id for node_prefix
            if len(node_prefix) > 0:
                current_node_prefix = node_prefix
            else:
                current_node_prefix = str(reach_id)[0:4]

            # makes list of coordinates. Merges multi-part geoms
            reach_coordinates = list(row["geometry"].coords)

            inlet_coordinate = reach_coordinates[inlet_linestring_index]
            outlet_coordinate = reach_coordinates[outlet_linestring_index]

            if inlet_coordinate not in node_coordinates:
                current_node_id_with_prefix = current_node_prefix + current_node_id
                node_coordinates[inlet_coordinate] = current_node_id_with_prefix
                fromNodes[i] = current_node_id_with_prefix

                current_node_id = int(current_node_id.lstrip("0")) + 1
                if current_node_id > max_node_value:
                    raise ValueError("Current Node ID exceeding max. Look at source code to change.")
                current_node_id = str(current_node_id).zfill(max_post_node_digits)

            else:
                fromNodes[i] = node_coordinates[inlet_coordinate]

            if outlet_coordinate not in node_coordinates:
                current_node_id_with_prefix = current_node_prefix + current_node_id
                node_coordinates[outlet_coordinate] = current_node_id_with_prefix
                toNodes[i] = current_node_id_with_prefix

                current_node_id = int(current_node_id.lstrip("0")) + 1
                if current_node_id > max_node_value:
                    raise ValueError("Current Node ID exceeding max. Look at source code to change.")
                current_node_id = str(current_node_id).zfill(max_post_node_digits)

            else:
                toNodes[i] = node_coordinates[outlet_coordinate]

        self.loc[:, fromNode_attribute] = fromNodes
        self.loc[:, toNode_attribute] = toNodes

        return self

    def derive_outlets(
        self,
        toNode_attribute="ToNode",
        fromNode_attribute="FromNode",
        outlets_attribute="outlet_id",
        verbose=False,
    ):
        if verbose:
            print("Deriving outlets ...")

        fromNodes = set(i for i in self[fromNode_attribute])

        outlets = [-1] * len(self)

        for i, tn in enumerate(self[toNode_attribute]):
            if tn not in fromNodes:
                outlets[i] = i + 1

        self[outlets_attribute] = outlets

        return self

    def derive_inlets(
        self,
        toNode_attribute="ToNode",
        fromNode_attribute="FromNode",
        inlets_attribute="inlet_id",
        verbose=False,
    ):
        if verbose:
            print("Deriving inlets ...")

        toNodes = set(i for i in self[toNode_attribute])

        inlets = [-1] * len(self)

        for i, fn in enumerate(self[fromNode_attribute]):
            if fn not in toNodes:
                inlets[i] = i + 1

        self[inlets_attribute] = inlets

        return self

    def derive_inlet_points_by_feature(self, branch_id_attribute, outlet_linestring_index):
        """Finds the upstream point of every feature in the stream network"""

        inlet_linestring_index = StreamNetwork.flip_inlet_outlet_linestring_index(outlet_linestring_index)

        feature_inlet_points_gdf = gpd.GeoDataFrame(self.copy())

        self_copy = self.copy()

        for idx in self_copy.index:
            row = self_copy.loc[[idx]]
            if row.geom_type[idx] == "MultiLineString":
                # Convert MultiLineString to LineString
                row = row.explode(index_parts=False)
                row.loc[row[branch_id_attribute].duplicated(), branch_id_attribute] = np.nan
                row = row.dropna(subset=[branch_id_attribute])

            feature_inlet_point = Point(row.geometry[0].coords[inlet_linestring_index])

            feature_inlet_points_gdf.loc[idx, "geometry"] = feature_inlet_point

        return feature_inlet_points_gdf

    def derive_headwater_points_with_inlets(self, inlets_attribute="inlet_id", outlet_linestring_index=0):
        """Derives headwater points file given inlets"""

        # get inlet linestring index
        inlet_linestring_index = StreamNetwork.flip_inlet_outlet_linestring_index(outlet_linestring_index)

        inlet_indices = self.loc[:, inlets_attribute] != -1

        inlets = self.loc[inlet_indices, :].reset_index(drop=True)

        headwater_points_gdf = gpd.GeoDataFrame(inlets.copy())

        for idx, row in inlets.iterrows():
            headwater_point = row.geometry.coords[inlet_linestring_index]

            headwater_point = Point(headwater_point)

            headwater_points_gdf.loc[idx, "geometry"] = headwater_point

        return headwater_points_gdf

    def exclude_attribute_values(self, branch_id_attribute=None, values_excluded=None, verbose=False):
        if (branch_id_attribute is not None) and (values_excluded is not None):
            self = StreamNetwork(
                self[~self[branch_id_attribute].isin(values_excluded)],
                branch_id_attribute=branch_id_attribute,
            )

        if verbose:
            print("Number of df rows = " + str(self.shape[0]))

        return self

    def remove_stream_segments_without_catchments(
        self, catchments, reach_id_attribute="ID", reach_id_attribute_in_catchments="ID", verbose=False
    ):
        if verbose:
            print("Removing stream segments without catchments ...")

        # load catchments
        if isinstance(catchments, gpd.GeoDataFrame):
            pass
        elif isinstance(catchments, str):
            catchments = gpd.read_file(catchments)
        else:
            raise TypeError("Catchments needs to be GeoDataFame or path to vector file")

        self = self.merge(
            catchments.loc[:, reach_id_attribute_in_catchments],
            left_on=reach_id_attribute,
            right_on=reach_id_attribute_in_catchments,
            how="inner",
        )

        return self

    def remove_branches_without_catchments(
        self,
        catchments,
        reach_id_attribute="ID",
        branch_id_attribute="branchID",
        reach_id_attribute_in_catchments="ID",
        verbose=False,
    ):
        if verbose:
            print("Removing stream branches without catchments ...")

        # load catchments
        if isinstance(catchments, gpd.GeoDataFrame):
            pass
        elif isinstance(catchments, str):
            catchments = gpd.read_file(catchments)
        else:
            raise TypeError("Catchments needs to be GeoDataFame or path to vector file")

        unique_stream_branches = self.loc[:, branch_id_attribute].unique()
        unique_catchments = set(catchments.loc[:, reach_id_attribute_in_catchments].unique())

        current_index_name = self.index.name
        self = self.set_index(branch_id_attribute, drop=False)

        for usb in unique_stream_branches:
            try:
                reach_ids_in_branch = set(self.loc[usb, reach_id_attribute].unique())
            except AttributeError:
                reach_ids_in_branch = set([self.loc[usb, reach_id_attribute]])

            if len(reach_ids_in_branch & unique_catchments) == 0:
                # print(f'Dropping {usb}')
                self = self.drop(usb)

        if current_index_name is None:
            self = self.reset_index(drop=True)
        else:
            self = self.set_index(current_index_name, drop=True)

        return self

    def trim_branches_in_waterbodies(self, wbd, branch_id_attribute, verbose=False):
        """
        Recursively trims the reaches from the ends of the branches if they are in a
        waterbody (determined by the Lake attribute).
        """

        def find_downstream_reaches_in_waterbodies(tmp_self, tmp_IDs=[]):
            # Find lowest reach(es)
            downstream_IDs = [
                int(x) for x in tmp_self.From_Node[~tmp_self.To_Node.isin(tmp_self.From_Node)]
            ]  # IDs of most downstream reach(es)

            for downstream_ID in downstream_IDs:
                # Stop if lowest reach is not in a lake
                if tmp_self.Lake[tmp_self.From_Node.astype(int) == downstream_ID].iloc[0] == -9999:
                    continue
                else:
                    # Remove reach from tmp_self
                    tmp_IDs.append(downstream_ID)
                    tmp_self = tmp_self.drop(
                        tmp_self[tmp_self.From_Node.astype(int).isin([downstream_ID])].index
                    )
                    # Repeat for next lowest downstream reach
                    if downstream_ID in tmp_self.To_Node.astype(int).values:
                        return find_downstream_reaches_in_waterbodies(tmp_self, tmp_IDs)
            return tmp_IDs

        def find_upstream_reaches_in_waterbodies(tmp_self, tmp_IDs=[]):
            # Find highest reach(es)
            upstream_IDs = [
                int(x) for x in tmp_self.From_Node[~tmp_self.From_Node.isin(tmp_self.To_Node)]
            ]  # IDs of most upstream reach(es)
            nonlake_reaches = [
                int(x) for x in tmp_self.From_Node[tmp_self.Lake == -9999]
            ]  # IDs of most  reach(es) that are not designated as lake reaches

            for upstream_ID in upstream_IDs:
                # Stop if uppermost reach is not in a lake
                if tmp_self.Lake[tmp_self.From_Node.astype(int) == upstream_ID].iloc[0] == -9999:
                    continue
                else:
                    if (
                        int(tmp_self.To_Node[tmp_self.From_Node.astype(int) == upstream_ID].iloc[0])
                        in nonlake_reaches
                    ):
                        continue
                    # Remove reach from tmp_self
                    tmp_IDs.append(upstream_ID)
                    tmp_self = tmp_self.drop(
                        tmp_self[tmp_self.From_Node.astype(int).isin([upstream_ID])].index
                    )
                    # Repeat for next highest upstream reach
                    return find_upstream_reaches_in_waterbodies(tmp_self, tmp_IDs)
            return tmp_IDs

        if verbose:
            print("Trimming stream branches in waterbodies ...")

        for branch in self[branch_id_attribute].astype(int).unique():
            tmp_self = self[self[branch_id_attribute].astype(int) == branch]

            # load waterbodies
            if isinstance(wbd, str):
                wbd = gpd.read_file(wbd)

            # trim only branches in WBD (to prevent outlet from being trimmed)
            if isinstance(wbd, gpd.GeoDataFrame):
                tmp_self = gpd.sjoin(tmp_self, wbd)

            # If entire branch is in waterbody
            if all(tmp_self.Lake.values != -9999):
                tmp_IDs = tmp_self.From_Node.astype(int)

            else:
                # Find bottom up
                tmp_IDs = find_downstream_reaches_in_waterbodies(tmp_self)

                # Find top down
                tmp_IDs = find_upstream_reaches_in_waterbodies(tmp_self, tmp_IDs)

            if len(tmp_IDs) > 0:
                self = self.drop(self[self.From_Node.astype(int).isin(tmp_IDs)].index)

        return self

    def remove_branches_in_waterbodies(
        self, waterbodies, branch_id_attribute, out_vector_files=None, verbose=False
    ):
        """
        Removes branches completely in waterbodies
        """

        if verbose:
            print("Removing branches in waterbodies")

        # load waterbodies
        if isinstance(waterbodies, str) and isfile(waterbodies):
            waterbodies = gpd.read_file(waterbodies)

        if isinstance(waterbodies, gpd.GeoDataFrame):
            waterbodies = waterbodies.drop('OBJECTID', axis=1)

            # Find branches in waterbodies
            self = self.rename(columns={branch_id_attribute: "bids"})
            sjoined = gpd.sjoin(self, waterbodies, predicate="within")
            self = self.drop(sjoined.index)
            self = self.rename(columns={"bids": branch_id_attribute})

            # if out_vector_files is not None:
            #     if verbose:
            #         print("Writing pruned branches ...")

            # self.write(out_vector_files, index=False)

        return self

    def select_branches_intersecting_huc(self, wbd, buffer_wbd_streams, out_vector_files, verbose=False):
        """
        Select branches that intersect the HUC
        """

        if verbose:
            print("Selecting branches that intersect the HUC")

        branch_id_attribute = self.branch_id_attribute
        attribute_excluded = self.attribute_excluded
        values_excluded = self.values_excluded

        # Check if required input files exist
        for filename in [buffer_wbd_streams, wbd]:
            if not isfile(buffer_wbd_streams):
                raise FileNotFoundError(f"{filename} does not exist")

        # load waterbodies
        if isinstance(wbd, str):
            wbd = gpd.read_file(wbd)

        if isinstance(wbd, gpd.GeoDataFrame):
            # Find branches intersecting HUC
            self = self.rename(columns={branch_id_attribute: "bids"})
            sjoined = gpd.sjoin(self, wbd, predicate="intersects")
            self = self.rename(columns={"bids": branch_id_attribute})

            self = self[self.index.isin(sjoined.index.values)]

            self = StreamNetwork(
                self,
                branch_id_attribute=branch_id_attribute,
                attribute_excluded=attribute_excluded,
                values_excluded=values_excluded,
            )

            if out_vector_files is not None:
                if verbose:
                    print("Writing selected branches ...")

                if self.empty:
                    print(
                        "Sorry, no streams exist and processing can not continue. This could be an empty file."
                    )
                    # sys.exit(FIM_exit_codes.UNIT_NO_BRANCHES.value)  # will send a 60 back
                    return self
                    # sys.exit(FIM_exit_codes.NO_BRANCH_LEVELPATHS_EXIST.value)  # will send a 63 back

                self.write(out_vector_files, index=False)

        return self

    def derive_stream_branches(
        self,
        toNode_attribute="ToNode",
        fromNode_attribute="FromNode",
        upstreams=None,
        outlet_attribute="outlet_id",
        branch_id_attribute="branchID",
        reach_id_attribute="ID",
        comparison_attributes="StreamOrde",
        comparison_function=max,
        max_branch_id_digits=6,
        verbose=False,
    ):
        """Derives stream branches"""

        # checks inputs
        allowed_comparison_function = {max, min}
        if comparison_function not in allowed_comparison_function:
            raise ValueError(f"Only {allowed_comparison_function} comparison functions allowed")

        # sets index of stream branches as reach id attribute
        reset_index = False
        if self.index.name != reach_id_attribute:
            self = self.set_index(reach_id_attribute, drop=True)
            reset_index = True

        # make upstream and downstream dictionaries if none are passed
        if upstreams is None:
            upstreams, _ = self.make_up_and_downstream_dictionaries(
                reach_id_attribute=reach_id_attribute,
                toNode_attribute=toNode_attribute,
                fromNode_attribute=fromNode_attribute,
                verbose=verbose,
            )

        # initialize empty queue, visited set, branch attribute column, and all toNodes set
        Q = deque()
        visited = set()
        self[branch_id_attribute] = [-1] * len(self)

        # progress bar
        progress = tqdm(total=len(self), disable=(not verbose), desc="Stream branches")

        outlet_boolean_mask = self[outlet_attribute] >= 0
        outlet_reach_ids = self.index[outlet_boolean_mask].tolist()

        branch_ids = [
            str(h)[0:4] + str(b + 1).zfill(max_branch_id_digits) for b, h in enumerate(outlet_reach_ids)
        ]

        self.loc[outlet_reach_ids, branch_id_attribute] = branch_ids
        Q = deque(outlet_reach_ids)
        visited = set(outlet_reach_ids)
        bid = int(branch_ids[-1][-max_branch_id_digits:].lstrip("0")) + 1
        progress.update(bid - 1)

        # breath-first traversal
        # while queue contains reaches
        while Q:
            # pop current reach id from queue
            current_reach_id = Q.popleft()

            # update progress
            progress.update(1)

            # get current reach stream order and branch id
            # current_reach_comparison_value = self.at[current_reach_id,comparison_attributes]
            current_reach_branch_id = self.at[current_reach_id, branch_id_attribute]

            # get upstream ids
            upstream_ids = upstreams[current_reach_id]

            # identify upstreams by finding if fromNode exists in set of all toNodes
            if upstream_ids:
                # determine if each upstream has been visited or not
                not_visited_upstream_ids = []  # list to save not visited upstreams
                for us in upstream_ids:
                    # if upstream id has not been visited
                    if us not in visited:
                        # add to visited set and to queue
                        visited.add(us)
                        Q.append(us)
                        not_visited_upstream_ids += [us]

                # if upstreams that are not visited exist
                if not_visited_upstream_ids:
                    # find
                    upstream_reaches_compare_values = self.loc[
                        not_visited_upstream_ids, comparison_attributes
                    ]
                    # matching_value = comparison_function(upstream_reaches_compare_values)

                    # ==================================================================================
                    # If the two stream orders aren't the same, then follow the highest order, otherwise use arbolate sum
                    if (
                        upstream_reaches_compare_values.idxmax()["order_"]
                        == upstream_reaches_compare_values.idxmin()["order_"]
                    ):
                        decision_attribute = "arbolate_sum"
                    else:
                        decision_attribute = "order_"
                    # Continue the current branch up the larger stream
                    continue_id = upstream_reaches_compare_values.idxmax()[decision_attribute]
                    self.loc[continue_id, branch_id_attribute] = current_reach_branch_id
                    # Create a new level path for the smaller tributary(ies)
                    if len(not_visited_upstream_ids) == 1:
                        continue  # only create a new branch if there are 2 upstreams
                    new_upstream_branches = upstream_reaches_compare_values.loc[
                        ~upstream_reaches_compare_values.index.isin([continue_id])
                    ]
                    for new_up_id in new_upstream_branches.index:
                        branch_id = str(current_reach_branch_id)[0:4] + str(bid).zfill(max_branch_id_digits)
                        self.loc[new_up_id, branch_id_attribute] = branch_id
                        bid += 1
                    # ==================================================================================
                    """ NOTE: The above logic uses stream order to override arbolate sum.
                        Use the commented section below if this turns out to be a bad idea!"""
                    # matches = 0 # if upstream matches are more than 1, limits to only one match
                    # for usrcv,nvus in zip(upstream_reaches_compare_values,not_visited_upstream_ids):
                    #    if (usrcv == matching_value) & (matches == 0):
                    #        self.at[nvus,branch_id_attribute] = current_reach_branch_id
                    #        matches += 1
                    #    else:
                    #        branch_id = str(current_reach_branch_id)[0:4] + str(bid).zfill(max_branch_id_digits)
                    #        self.at[nvus,branch_id_attribute] = branch_id
                    #        bid += 1

        progress.close()

        if reset_index:
            self = self.reset_index(drop=False)

        return self

    def make_up_and_downstream_dictionaries(
        self, reach_id_attribute="ID", toNode_attribute="ToNode", fromNode_attribute="FromNode", verbose=False
    ):
        # sets index of stream branches as reach id attribute
        # if self.index.name != reach_id_attribute:
        #    self = self.set_index(reach_id_attribute,drop=True)

        # find upstream and downstream dictionaries
        upstreams, downstreams = dict(), dict()

        for _, row in tqdm(
            self.iterrows(),
            disable=(not verbose),
            total=len(self),
            desc="Upstream and downstream dictionaries",
        ):
            reach_id = row[reach_id_attribute]
            downstreams[reach_id] = self.loc[
                self[fromNode_attribute] == row[toNode_attribute], reach_id_attribute
            ].tolist()
            upstreams[reach_id] = self.loc[
                self[toNode_attribute] == row[fromNode_attribute], reach_id_attribute
            ].tolist()

        return (upstreams, downstreams)

    def get_arbolate_sum(
        self,
        arbolate_sum_attribute="arbolate_sum",
        inlets_attribute="inlet_id",
        reach_id_attribute="ID",
        length_conversion_factor_to_km=0.001,
        upstreams=None,
        downstreams=None,
        toNode_attribute="ToNode",
        fromNode_attribute="FromNode",
        verbose=False,
    ):
        # sets index of stream branches as reach id attribute
        reset_index = False
        if self.index.name != reach_id_attribute:
            self = self.set_index(reach_id_attribute, drop=True)
            reset_index = True

        # make upstream and downstream dictionaries if none are passed
        if (upstreams is None) | (downstreams is None):
            upstreams, downstreams = self.make_up_and_downstream_dictionaries(
                reach_id_attribute=reach_id_attribute,
                toNode_attribute=toNode_attribute,
                fromNode_attribute=fromNode_attribute,
                verbose=verbose,
            )

        # initialize queue, visited set, with inlet reach ids
        inlet_reach_ids = self.index[self[inlets_attribute] >= 0].tolist()
        S = deque(inlet_reach_ids)
        visited = set()

        # initialize arbolate sum, make length km column, make all from nodes set
        self[arbolate_sum_attribute] = self.geometry.length * length_conversion_factor_to_km

        # progress bar
        progress = tqdm(total=len(self), disable=(not verbose), desc="Arbolate sums")

        # depth-first traversal
        # while stack contains reaches
        while S:
            # pop current reach id from queue
            current_reach_id = S.pop()

            # current arbolate sum
            current_reach_arbolate_sum = self.at[current_reach_id, arbolate_sum_attribute]

            # if current reach id is not visited mark as visited
            if current_reach_id not in visited:
                visited.add(current_reach_id)
                progress.update(n=1)

            # get downstream ids
            downstream_ids = downstreams[current_reach_id]

            if downstream_ids:
                for ds in downstream_ids:
                    # figure out of all upstream reaches of ds have been visited
                    upstream_of_ds_ids = set(upstreams[ds])
                    all_upstream_ids_of_ds_are_visited = upstream_of_ds_ids.issubset(visited)

                    # append downstream to stack
                    if all_upstream_ids_of_ds_are_visited:
                        S.append(ds)

                    self.loc[ds, arbolate_sum_attribute] += current_reach_arbolate_sum

        progress.close()

        if reset_index:
            self = self.reset_index(drop=False)

        return self

    def dissolve_by_branch(
        self,
        wbd,
        branch_id_attribute="LevelPathI",
        attribute_excluded="StreamOrde",
        values_excluded=[1, 2],
        out_vector_files=None,
        out_extended_vector_files=None,
        verbose=False,
    ):

        def add_outlet_segments(
            self_extended: gpd.GeoDataFrame,
            self_copy: gpd.GeoDataFrame,
            outlet_id: int,
            ds_outlet_tuple: tuple,
            extended_id: int = None,
        ) -> gpd.GeoDataFrame:
            """
            Recursively adds outlet segments to the stream network

            Parameters
            ----------
            self_ : GeoDataFrame
                Dissolved tream network GeoDataFrame
            self_copy : GeoDataFrame
                Stream network GeoDataFrame
            outlet_id : int
                Outlet segment ID
            ds_outlet_tuple : tuple
                Outlet segment tuple

            Returns
            -------
            GeoDataFrame
                Stream network with outlet segments
            """

            if not extended_id:
                extended_id = outlet_id

            # add outlet segment to stream network
            idx = self_extended['bids_temp'] == outlet.levpa_id
            if self_extended.loc[idx].empty:
                idx = self_extended['ID'] == extended_id

            extended_gs = gpd.GeoSeries(
                [self_extended.loc[idx, 'geometry'].item(), ds_outlet_tuple.geometry]
            ).line_merge()

            if isinstance(extended_gs[0], MultiLineString):
                self_extended.loc[idx, 'geometry'] = MultiLineString(
                    [linestring for linestring in extended_gs[0].geoms] + [extended_gs[1]]
                )
            else:
                self_extended.loc[idx, 'geometry'] = MultiLineString(
                    [extended_gs[0].coords, extended_gs[1].coords]
                )

            if ds_outlet_tuple.to in self_copy.ID.values:
                # find the next downstream segment
                outlet_id = ds_outlet_tuple.to
                for ds_outlet_tuple in self_copy[self_copy.ID == ds_outlet_tuple.to].itertuples():

                    # recursively add outlet segments
                    self_extended = add_outlet_segments(
                        self_extended, self_copy, outlet_id, ds_outlet_tuple, extended_id
                    )

            return self_extended

        if verbose:
            print("Dissolving by branch ...")

        # exclude attributes and their values
        if (attribute_excluded is not None) & (values_excluded is not None):
            values_excluded = set(values_excluded)
            exclude_indices = [False if i in values_excluded else True for i in self[attribute_excluded]]
            self = self.loc[exclude_indices, :]

        # dissolve lines
        self["bids_temp"] = self.loc[:, branch_id_attribute].copy()

        wbd = gpd.read_file(wbd)
        wbd = wbd.drop('shape_Length', axis=1)

        # Filter segments that are in the HUC
        self_in_wbd = gpd.sjoin(self, wbd)
        self_in_wbd = self_in_wbd.drop('index_right', axis=1)

        # ensure the new stream order has the order from it's highest child
        max_stream_order = (
            self_in_wbd[[branch_id_attribute, "order_"]].groupby(branch_id_attribute).max()["order_"].copy()
        )

        # Find the HUC outlet(s) -- downstream segments that intersect WBD boundary
        sjoin = gpd.sjoin(self, wbd, predicate='crosses')

        # Get ID of segments downstream of WBD boundary
        s = self[self['ID'].isin(sjoin['to'])]

        # Find downstream segments outside of WBD
        s_in_wbd = gpd.sjoin(s, wbd)
        s_not_in_wbd = s[~s['ID'].isin(s_in_wbd['ID'])]

        self_copy = self.copy(deep=True)

        self = self_in_wbd.dissolve(by=branch_id_attribute)

        self["order_"] = max_stream_order.values

        if not s_not_in_wbd.empty:
            outlets_extended = self.copy(deep=True)

            # For each outlet
            for outlet in s_not_in_wbd.itertuples():
                # Select segments in levelpath
                temp_df = self_copy[self_copy[branch_id_attribute] == outlet.bids_temp]

                # Check if the levelpath outlet is external
                if not len(temp_df.merge(self_in_wbd, left_on='to', right_on='ID')) == len(temp_df):
                    outlet_id = self_in_wbd.loc[self_in_wbd['to'] == outlet.ID, 'ID'].values[0]
                    outlets_extended = add_outlet_segments(outlets_extended, self_copy, outlet_id, outlet)

            # merges each multi-line string to a singular linestring
            for lpid, row in tqdm(
                outlets_extended.iterrows(),
                total=len(outlets_extended),
                disable=(not verbose),
                desc="Merging mult-part geoms",
            ):
                if isinstance(row.geometry, MultiLineString):
                    merged_line = linemerge(row.geometry)

                    # outlets_extended.loc[lpid,'geometry'] = merged_line
                    try:
                        outlets_extended.loc[lpid, "geometry"] = merged_line
                    except ValueError:
                        merged_line = list(merged_line.geoms)[0]
                        outlets_extended.loc[lpid, "geometry"] = merged_line

            # self[branch_id_attribute] = bids
            outlets_extended = StreamNetwork(
                outlets_extended,
                branch_id_attribute=branch_id_attribute,
                attribute_excluded=attribute_excluded,
                values_excluded=values_excluded,
            )

            outlets_extended = outlets_extended.rename(columns={'bids_temp': branch_id_attribute})

        # merges each multi-line string to a singular linestring
        for lpid, row in tqdm(
            self.iterrows(), total=len(self), disable=(not verbose), desc="Merging mult-part geoms"
        ):
            if isinstance(row.geometry, MultiLineString):
                merged_line = linemerge(row.geometry)

                # self.loc[lpid,'geometry'] = merged_line
                try:
                    self.loc[lpid, "geometry"] = merged_line
                except ValueError:
                    merged_line = list(merged_line.geoms)[0]
                    self.loc[lpid, "geometry"] = merged_line

        # self[branch_id_attribute] = bids
        self = StreamNetwork(
            self,
            branch_id_attribute=branch_id_attribute,
            attribute_excluded=attribute_excluded,
            values_excluded=values_excluded,
        )

        if out_vector_files is not None:
            if verbose:
                print("Writing dissolved branches ...")

            # if out_vector_files is not None:
            #     # base_file_path,extension = splitext(out_vector_files)

            #     if verbose:
            #         print("Writing dissolved branches ...")

            # for bid in tqdm(self.loc[:,branch_id_attribute],total=len(self),disable=(not verbose)):
            # out_vector_file = "{}_{}{}".format(base_file_path,bid,extension)

            # bid_indices = self.loc[:,branch_id_attribute] == bid
            # current_stream_network = StreamNetwork(self.loc[bid_indices,:])

            # current_stream_network.write(out_vector_file,index=False)
            self.write(out_vector_files, index=False)

        if out_extended_vector_files is not None and not s_not_in_wbd.empty:
            outlets_extended.write(out_extended_vector_files, index=False)
        else:
            self.write(out_extended_vector_files, index=False)

        return self

    def derive_segments(self, inlets_attribute="inlet_id", reach_id_attribute="ID"):
        pass

    def conflate_branches(
        self,
        target_stream_network,
        branch_id_attribute_left="branch_id",
        branch_id_attribute_right="branch_id",
        left_order_attribute="order_",
        right_order_attribute="order_",
        crosswalk_attribute="crosswalk_id",
        verbose=False,
    ):
        # get unique stream orders
        orders = self.loc[:, right_order_attribute].unique()

        # make a dictionary of STR trees for every stream order
        trees = {o: STRtree(target_stream_network.geometry.tolist()) for o in orders}

        # make the crosswalk id attribute and set index
        self.loc[:, crosswalk_attribute] = [None] * len(self)
        self = self.set_index(branch_id_attribute_left)

        # loop through rows of self
        for idx, row in tqdm(
            self.iterrows(), total=len(self), disable=(not verbose), desc="Conflating branches"
        ):
            g = row["geometry"]
            o = row[left_order_attribute]

            tree = trees[o]

            # find nearest geom in target and its index
            matching_geom = tree.nearest(g)
            match_idx = target_stream_network.geometry == matching_geom

            # get the branch ids
            right_branch_id = int(target_stream_network.loc[match_idx, branch_id_attribute_left])
            left_branch_id = idx

            # save the target matching branch id
            self.loc[left_branch_id, crosswalk_attribute] = right_branch_id

        # reset indices
        self = self.reset_index(drop=False)

        return self

    def explode_to_points(self, reach_id_attribute="ID", sampling_size=None, verbose=False):
        points_gdf = self.copy()
        points_gdf = points_gdf.reset_index(drop=True)

        all_exploded_points = [None] * len(points_gdf)
        for idx, row in tqdm(
            self.iterrows(), total=len(self), disable=(not verbose), desc="Exploding Points"
        ):
            geom = row["geometry"]

            exploded_points = [p for p in iter(geom.coords)]

            if sampling_size is None:
                exploded_points = MultiPoint(exploded_points)
            else:
                try:
                    exploded_points = MultiPoint(sample(exploded_points, sampling_size))
                except ValueError:
                    exploded_points = MultiPoint(exploded_points)

            all_exploded_points[idx] = exploded_points

        points_gdf["geometry"] = all_exploded_points

        points_gdf = points_gdf.explode(index_parts=True)
        points_gdf = points_gdf.reset_index(drop=True)

        return points_gdf

    @staticmethod
    def conflate_points(
        source_points, target_points, source_reach_id_attribute, target_reach_id_attribute, verbose=False
    ):
        tree = STRtree(target_points.geometry.tolist())

        # find matching geometry
        matches_dict = dict.fromkeys(source_points.loc[:, source_reach_id_attribute].astype(int).tolist(), [])
        for idx, row in tqdm(
            source_points.iterrows(),
            total=len(source_points),
            disable=(not verbose),
            desc="Conflating points",
        ):
            geom = row["geometry"]
            nearest_target_point = tree.nearest(geom)
            match_idx = target_points.index[target_points.geometry == nearest_target_point].tolist()

            if len(match_idx) > 1:
                match_idx = match_idx[0]
            else:
                match_idx = match_idx[0]

            matched_id = int(target_points.loc[match_idx, target_reach_id_attribute])
            source_id = int(row[source_reach_id_attribute])
            matches_dict[source_id] = matches_dict[source_id] + [matched_id]

            # if len(matches_dict[source_id])>1:
            #    print(matches_dict[source_id])

        # get mode of matches
        if verbose:
            print("Finding mode of matches ...")

        for source_id, matches in matches_dict.items():
            majority = mode(matches).mode
            matches_dict[source_id] = majority[0]

        # make dataframe
        if verbose:
            print("Generating crosswalk table ...")

        crosswalk_table = pd.DataFrame.from_dict(
            matches_dict, orient="index", columns=[target_reach_id_attribute]
        )
        crosswalk_table.index.name = source_reach_id_attribute

        return crosswalk_table

    def clip(self, mask, keep_geom_type=False, verbose=False):
        if verbose:
            print("Clipping streams to mask ...")

        # load mask
        if isinstance(mask, gpd.GeoDataFrame):
            pass
        elif isinstance(mask, str):
            mask = gpd.read_file(mask)
        else:
            raise TypeError("mask needs to be GeoDataFame or path to vector file")

        branch_id_attribute = self.branch_id_attribute
        attribute_excluded = self.attribute_excluded
        values_excluded = self.values_excluded

        self = StreamNetwork(
            gpd.clip(self, mask, keep_geom_type).reset_index(drop=True),
            branch_id_attribute=branch_id_attribute,
            attribute_excluded=attribute_excluded,
            values_excluded=values_excluded,
        )

        return self


class StreamBranchPolygons(StreamNetwork):
    # branch_id_attribute = None
    # values_excluded = None
    # attribute_excluded = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def buffer_stream_branches(cls, stream_network, buffer_distance, verbose=True):
        """Buffers stream branches by distance"""

        if verbose:
            print("Buffering stream branches to polygons")

        buffer_distance = int(buffer_distance)

        # buffer lines
        new_bids = [None] * len(stream_network)
        new_geoms = new_bids.copy()
        i = 0

        for _, row in tqdm(stream_network.iterrows(), disable=(not verbose), total=len(stream_network)):
            new_geoms[i] = row[stream_network.geometry.name].buffer(buffer_distance)
            new_bids[i] = i + 1
            i += 1

        # create polys gpd
        polys = stream_network.copy()

        # assign to StreamBranchPolys
        # polys[stream_network.branch_id_attribute] = new_bids
        polys[stream_network.geom_name] = new_geoms
        polys.set_geometry(stream_network.geom_name)

        # assign class and attributes
        polys = cls(
            polys,
            branch_id_attribute=stream_network.branch_id_attribute,
            attribute_excluded=stream_network.attribute_excluded,
            values_excluded=stream_network.values_excluded,
        )

        return polys

    @staticmethod
    def query_vectors_by_branch(
        vector, branch_ids, branch_id_attribute, out_filename_template=None, vector_layer=None
    ):
        # load vaas
        if isinstance(vector, str):
            vector_filename = vector
            # vector = gpd.read_file(vector_filename,layer=vector_layer)
            vector = fiona.open(vector_filename, "r", layer=vector_layer)
        elif isinstance(vector, fiona.Collection):
            pass
        else:
            raise TypeError("Pass vector argument as filepath or fiona collection")

        def __find_matching_record(vector, attribute, value, matching="first"):
            if matching not in ("first", "all"):
                raise ValueError("matching needs to be 'first' or 'all'")

            matches = []
            for rec in vector:
                if rec["properties"][attribute] == value:
                    if matching == "first":
                        matches = [rec]
                        break
                    elif matching == "all":
                        matches += [rec]

            return matches

        # get source information
        source_meta = vector.meta

        # out records
        out_records = []

        for bid in branch_ids:
            out_records += __find_matching_record(vector, branch_id_attribute, bid, matching="all")

        if (out_filename_template is not None) & (len(out_records) != 0):
            base, ext = os.path.splitext(out_filename_template)
            out_filename = base + "_{}".format(bid) + ext

            with fiona.open(out_filename, "w", **source_meta) as out_file:
                out_file.writerecords(out_records)

        # close
        vector.close()

        return out_records

    def clip(self, to_clip, out_filename_template=None, branch_id=None, branch_id_attribute=None):
        """Clips a raster or vector to the stream branch polygons"""

        fileType = "raster"  # default

        # load raster or vector file
        if isinstance(to_clip, DatasetReader):  # if already rasterio dataset
            pass
        elif isinstance(to_clip, str):  # if a string
            try:  # tries to open as rasterio first then geopanda
                to_clip = rasterio.open(to_clip, "r")
            except rasterio.errors.RasterioIOError:
                try:
                    to_clip = gpd.read_file(to_clip)
                    fileType = "vector"
                except DriverError:
                    raise IOError("{} file not found".format(to_clip))

        elif isinstance(to_clip, gpd.GeoDataFrame):  # if a geopanda dataframe
            fileType = "vector"
        else:
            raise TypeError(
                "Pass rasterio dataset,geopandas GeoDataFrame, or filepath to raster or vector file"
            )

        # generator to iterate
        if branch_id is not None:
            # print(iter(tuple([0,self.loc[self.loc[:,branch_id_attribute]==branch_id,:].squeeze()])))
            generator_to_iterate = enumerate(
                [self.loc[self.loc[:, branch_id_attribute] == branch_id, :].squeeze()]
            )
        else:
            generator_to_iterate = self.iterrows()

        return_list = []  # list to return rasterio objects or gdf's

        if fileType == "raster":
            buffered_meta = to_clip.meta.copy()
            buffered_meta.update(blockxsize=256, blockysize=256, tiled=True)

            for i, row in generator_to_iterate:
                buffered_array, buffered_transform = mask(to_clip, [row[self.geom_name]], crop=True)

                buffered_meta.update(
                    height=buffered_array.shape[1],
                    width=buffered_array.shape[2],
                    transform=buffered_transform,
                )

                # write out files
                if out_filename_template is not None:
                    branch_id = row[self.branch_id_attribute]

                    base, ext = os.path.splitext(out_filename_template)
                    out_filename = base + "_{}".format(branch_id) + ext

                    with rasterio.open(out_filename, "w", **buffered_meta) as out:
                        out.write(buffered_array)

                # return files in list
                return_list += [out]

            out.close()

        if fileType == "vector":
            for i, row in generator_to_iterate:
                branch_id = row[self.branch_id_attribute]
                out = gpd.clip(to_clip, row[self.geom_name], keep_geom_type=True)
                return_list += [out]

                if (out_filename_template is not None) & (not out.empty):
                    base, ext = os.path.splitext(out_filename_template)
                    out_filename = base + "_{}".format(branch_id) + ext
                    StreamNetwork.write(out, out_filename)

        return return_list

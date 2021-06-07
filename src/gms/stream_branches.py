#!/usr/bin/env python3

import geopandas as gpd
import pandas as pd
import rasterio
from rasterio.mask import mask
from rasterio.io import DatasetReader
from os.path import splitext
from fiona.errors import DriverError
from collections import deque
import numpy as np
from tqdm import tqdm
from shapely.ops import linemerge
from shapely.geometry import MultiLineString, LineString, MultiPoint
from shapely.strtree import STRtree
from random import sample
from scipy.stats import mode

class StreamNetwork(gpd.GeoDataFrame):

    geom_name = 'geometry' # geometry attribute name
    branch_id_attribute = None # branch id attribute name
    values_excluded = None
    attribute_excluded = None

    def __init__(self,*args,**kwargs):
        
        if kwargs:
            branch_id_attribute = kwargs.pop("branch_id_attribute",None)
            values_excluded = kwargs.pop("values_excluded",None)
            attribute_excluded = kwargs.pop("attribute_excluded",None)
        
        super().__init__(*args,**kwargs)
        
        self.branch_id_attribute = branch_id_attribute
        self.values_excluded = values_excluded
        self.attribute_excluded = attribute_excluded


    @classmethod
    def from_file(cls,filename,branch_id_attribute=None,values_excluded=None,attribute_excluded=None, verbose=False,*args,**kwargs):

        """ loads stream network from file to streamnetwork geopandas """

        if kwargs:
            inputs = {  'branch_id_attribute' : kwargs.pop("branch_id_attribute",None) , 
                        'values_excluded' :kwargs.pop("values_excluded",None) ,
                        'attribute_excluded' : kwargs.pop("attribute_excluded",None)      }
            
            verbose = kwargs.pop('verbose',None)
        else:
            inputs = {  'branch_id_attribute' : branch_id_attribute , 
                        'values_excluded' : values_excluded ,
                        'attribute_excluded' : attribute_excluded      }
            
        if verbose: 
            print('Loading file')
        
        return(cls(gpd.read_file(filename,*args,**kwargs),**inputs))


    def write(self,fileName,layer=None,index=True,verbose=False):

        """ Gets driver Name from file extension for Geopandas writing """

        if verbose:
            print("Writing to {}".format(fileName))

        # sets driver
        driverDictionary = {'.gpkg' : 'GPKG','.geojson' : 'GeoJSON','.shp' : 'ESRI Shapefile'}
        driver = driverDictionary[splitext(fileName)[1]]
    
        self.to_file(fileName, driver=driver, layer=layer, index=index)


    def merge(self,*args,**kwargs):
        branch_id_attribute = self.branch_id_attribute
        attribute_excluded = self.attribute_excluded
        values_excluded = self.values_excluded

        self = super().merge(*args,**kwargs)

        self = StreamNetwork(self,branch_id_attribute=branch_id_attribute,
                             attribute_excluded=attribute_excluded,
                             values_excluded=values_excluded)

        return(self)


    def merge_stream_branches(self,stream_branch_dataset,on='NHDPlusID',branch_id_attribute='LevelPathI',attributes='StreamOrde',stream_branch_layer_name=None):

        """ Merges stream branch id attribute from another vector file """

        # load vaas
        if isinstance(stream_branch_dataset,str):
            stream_branch_dataset = gpd.read_file(stream_branch_dataset,layer=stream_branch_layer_name)
        elif isinstance(stream_branch_dataset,gpd.GeoDataFrame):
            pass
        else:
            raise TypeError('Pass stream_branch_dataset argument as filepath or GeoDataframe')
    
        # merge and drop duplicate columns
        if isinstance(attributes,list):
            what = [on] + [branch_id_attribute] + attributes 
        elif isinstance(attributes,str):
            what = [on] + [branch_id_attribute] +[attributes]

        self = self.merge(stream_branch_dataset[what],on=on, how='inner')
        
        # make sure it's the correct object type
        self = StreamNetwork(self,branch_id_attribute=branch_id_attribute)

        return(self)


    def derive_nodes(self,toNode_attribute='ToNode',fromNode_attribute='FromNode',reach_id_attribute='NHDPlusID',
                     outlet_linestring_index=0,node_prefix=None,max_node_digits=8,verbose=False):
        
        if verbose:
            print("Deriving nodes ...")

        # check outlet parameter and set inlet index
        if outlet_linestring_index == 0:
            inlet_linestring_index = -1
        elif outlet_linestring_index == -1:
            inlet_linestring_index = 0
        else:
            raise ValueError("Pass 0 or -1 for outlet_linestring_index argument.")
        
        # set node prefix to string
        if node_prefix is None:
            node_prefix=''
        
        # handle digits and values for node ids
        max_post_node_digits = max_node_digits - len(node_prefix)
        max_node_value = int('9' * max_post_node_digits)

        # sets index of stream branches as reach id attribute
        #if self.index.name != reach_id_attribute:
            #self.set_index(reach_id_attribute,drop=True,inplace=True)
        
        inlet_coordinates, outlet_coordinates = dict(), dict()
        node_coordinates = dict()
        toNodes, fromNodes = [None] * len(self),[None] * len(self)
        current_node_id = '1'.zfill(max_post_node_digits)

        for i,(_,row) in enumerate(self.iterrows()):
            
            reach_id = row[reach_id_attribute]

            # get foss id for node_prefix
            if len(node_prefix) > 0:
                current_node_prefix = node_prefix
            else:
                current_node_prefix = str(reach_id)[0:4]

            reach_coordinates = list(row['geometry'].coords)

            inlet_coordinate = reach_coordinates[inlet_linestring_index]
            outlet_coordinate = reach_coordinates[outlet_linestring_index]
            
            if inlet_coordinate not in node_coordinates:
                current_node_id_with_prefix = current_node_prefix + current_node_id
                node_coordinates[inlet_coordinate] = current_node_id_with_prefix
                fromNodes[i] = current_node_id_with_prefix
                
                current_node_id = int(current_node_id.lstrip('0')) + 1
                if current_node_id > max_node_value:
                    raise ValueError('Current Node ID exceeding max. Look at source code to change.')
                current_node_id = str(current_node_id).zfill(max_post_node_digits)

            else:
                fromNodes[i] = node_coordinates[inlet_coordinate]

            if outlet_coordinate not in node_coordinates:
                current_node_id_with_prefix = current_node_prefix + current_node_id
                node_coordinates[outlet_coordinate] = current_node_id_with_prefix
                toNodes[i] = current_node_id_with_prefix

                current_node_id = int(current_node_id.lstrip('0')) + 1
                if current_node_id > max_node_value:
                    raise ValueError('Current Node ID exceeding max. Look at source code to change.')
                current_node_id = str(current_node_id).zfill(max_post_node_digits)

            else:
                toNodes[i] = node_coordinates[outlet_coordinate]

        self.loc[:,fromNode_attribute] = fromNodes
        self.loc[:,toNode_attribute] = toNodes
        
        
        return(self)


    def derive_outlets(self,toNode_attribute='ToNode',fromNode_attribute='FromNode',outlets_attribute='outlet_id',
                       verbose=False):

        if verbose:
            print("Deriving outlets ...")

        fromNodes = set(i for i in self[fromNode_attribute])

        outlets = [-1] * len(self)
        
        for i,tn in enumerate(self[toNode_attribute]):
            if tn not in fromNodes:
                outlets[i] = i + 1

        self[outlets_attribute] = outlets

        return(self)


    def derive_inlets(self,toNode_attribute='ToNode',fromNode_attribute='FromNode',inlets_attribute='inlet_id',
                      verbose=False):

        if verbose:
            print("Deriving inlets ...")

        toNodes = set(i for i in self[toNode_attribute])

        inlets = [-1] * len(self)

        for i,fn in enumerate(self[fromNode_attribute]):
            if fn not in toNodes:
                inlets[i] = i + 1

        self[inlets_attribute] = inlets

        return(self)


    def derive_stream_branches(self,toNode_attribute='ToNode',
                               fromNode_attribute='FromNode',
                               upstreams=None,
                               outlet_attribute='outlet_id',
                               branch_id_attribute='branchID',
                               reach_id_attribute='NHDPlusID',
                               comparison_attributes='StreamOrde',
                               comparison_function=max,
                               max_branch_id_digits=6,
                               verbose=False):

        """ Derives stream branches """

        # checks inputs
        allowed_comparison_function = {max,min}
        if comparison_function not in allowed_comparison_function:
            raise ValueError(f"Only {allowed_comparison_function} comparison functions allowed")

        # sets index of stream branches as reach id attribute
        reset_index = False
        if self.index.name != reach_id_attribute:
            self.set_index(reach_id_attribute,drop=True,inplace=True)
            reset_index = True

        # make upstream and downstream dictionaries if none are passed
        if upstreams is None:
            upstreams,_ = self.make_up_and_downstream_dictionaries(reach_id_attribute=reach_id_attribute,
                                                                   toNode_attribute=toNode_attribute,
                                                                   fromNode_attribute=fromNode_attribute,
                                                                   verbose=verbose)
        
        # initialize empty queue, visited set, branch attribute column, and all toNodes set
        Q = deque()
        visited = set()
        self[branch_id_attribute] = [-1] * len(self)

        # progress bar
        progress = tqdm(total=len(self),disable=(not verbose),desc='Stream branches')

        """
        # add outlets to queue, visited set. Assign branch id to outlets too
        bid = 1
        for reach_id,row in self.iterrows():

            oa = row[outlet_attribute]

            if oa >= 0:
                self.loc[reach_id,branch_id_attribute] = bid
                bid += 1
                
                Q.append(reach_id)
                visited.add(reach_id)
                progress.update(1)

        # alternative means of adding outlets to queue, to visited, and assigning branch id's to outlets
        """
        outlet_boolean_mask = self[outlet_attribute] >= 0
        outlet_reach_ids = self.index[outlet_boolean_mask].tolist()

        branch_ids = [ str(h)[0:4] + str(b+1).zfill(max_branch_id_digits) for b,h in enumerate(outlet_reach_ids) ]

        self.loc[outlet_reach_ids,branch_id_attribute] = branch_ids
        Q = deque(outlet_reach_ids)
        visited = set(outlet_reach_ids)
        bid = int(branch_ids[-1][-max_branch_id_digits:].lstrip('0')) + 1
        progress.update(bid-1)

        # breath-first traversal
        # while queue contains reaches
        while Q:

            # pop current reach id from queue
            current_reach_id = Q.popleft()
            
            # update progress
            progress.update(1)

            # get current reach stream order and branch id
            current_reach_comparison_value = self.at[current_reach_id,comparison_attributes]
            current_reach_branch_id = self.at[current_reach_id,branch_id_attribute]

            # get upstream ids
            upstream_ids = upstreams[current_reach_id]

            # identify upstreams by finding if fromNode exists in set of all toNodes
            if upstream_ids:
                
                # determine if each upstream has been visited or not
                not_visited_upstream_ids = [] # list to save not visited upstreams
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
                    upstream_reaches_compare_values = self.loc[not_visited_upstream_ids,comparison_attributes]
                    matching_value = comparison_function(upstream_reaches_compare_values)
                    
                    matches = 0 # if upstream matches are more than 1, limits to only one match
                    for usrcv,nvus in zip(upstream_reaches_compare_values,not_visited_upstream_ids):
                        if (usrcv == matching_value) & (matches == 0):
                            self.at[nvus,branch_id_attribute] = current_reach_branch_id
                            matches += 1
                        else:
                            branch_id = str(current_reach_branch_id)[0:4] + str(bid).zfill(max_branch_id_digits)
                            self.at[nvus,branch_id_attribute] = branch_id
                            bid += 1
        
        progress.close()

        if reset_index:
            self.reset_index(drop=False,inplace=True)

        return(self)


    def make_up_and_downstream_dictionaries(self,reach_id_attribute='NHDPlusID',
                                            toNode_attribute='ToNode',
                                            fromNode_attribute='FromNode',
                                            verbose=False):
        
        # sets index of stream branches as reach id attribute
        #if self.index.name != reach_id_attribute:
        #    self.set_index(reach_id_attribute,drop=True,inplace=True)

        # find upstream and downstream dictionaries
        upstreams,downstreams = dict(),dict()
        
        for _, row in tqdm(self.iterrows(),disable=(not verbose),
                           total=len(self), desc='Upstream and downstream dictionaries'):
            
            reach_id = row[reach_id_attribute]
            downstreams[reach_id] = self.loc[ self[fromNode_attribute] == row[toNode_attribute] , reach_id_attribute].tolist() 
            upstreams[reach_id] = self.loc[ self[toNode_attribute] == row[fromNode_attribute] , reach_id_attribute].tolist()
        
        return(upstreams,downstreams)


    def get_arbolate_sum(self,arbolate_sum_attribute='arbolate_sum',inlets_attribute='inlet_id',
                         reach_id_attribute='NHDPlusID',length_conversion_factor_to_km = 0.001,
                         upstreams=None, downstreams=None,
                         toNode_attribute='ToNode',
                         fromNode_attribute='FromNode',
                         verbose=False
                        ):
        
        # sets index of stream branches as reach id attribute
        reset_index = False
        if self.index.name != reach_id_attribute:
            self.set_index(reach_id_attribute,drop=True,inplace=True)
            reset_index = True

        # make upstream and downstream dictionaries if none are passed
        if (upstreams is None) | (downstreams is None):
            upstreams, downstreams = self.make_up_and_downstream_dictionaries(reach_id_attribute=reach_id_attribute,
                                                                              toNode_attribute=toNode_attribute,
                                                                              fromNode_attribute=fromNode_attribute,
                                                                              verbose=verbose)

        # initialize queue, visited set, with inlet reach ids
        inlet_reach_ids = self.index[self[inlets_attribute] >= 0].tolist()
        S = deque(inlet_reach_ids)
        visited = set()

        # initialize arbolate sum, make length km column, make all from nodes set
        self[arbolate_sum_attribute] = self.geometry.length * length_conversion_factor_to_km 
        
        # progress bar
        progress = tqdm(total=len(self),disable=(not verbose), desc= "Arbolate sums")

        # depth-first traversal
        # while stack contains reaches
        while S:
            
            # pop current reach id from queue
            current_reach_id = S.pop()
            
            # current arbolate sum
            current_reach_arbolate_sum = self.at[current_reach_id,arbolate_sum_attribute]

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

                    self.loc[ds,arbolate_sum_attribute] += current_reach_arbolate_sum
        
        progress.close()
    
        if reset_index:
            self.reset_index(drop=False,inplace=True)

        return(self)


    def dissolve_by_branch(self,branch_id_attribute='LevelPathI',attribute_excluded='StreamOrde',
                           values_excluded=[1,2],out_vector_files=None, verbose=False):
        
        if verbose:
            print("Dissolving by branch ...")

        # exclude attributes and their values
        if (attribute_excluded is not None) & (values_excluded is not None):
            values_excluded = set(values_excluded)
            exclude_indices = [False if i in values_excluded else True for i in self[attribute_excluded]]
            self = self.loc[exclude_indices,:]

        # dissolve lines
        self['bids_temp'] = self.loc[:,branch_id_attribute].copy()
        self = self.dissolve(by=branch_id_attribute)
        self.rename(columns={'bids_temp' : branch_id_attribute},inplace=True)
        
        # merges each multi-line string to a sigular linestring
        for lpid,row in tqdm(self.iterrows(),total=len(self),disable=(not verbose),desc="Merging mult-part geoms"):
            if isinstance(row.geometry,MultiLineString):
                self.loc[lpid,'geometry'] = linemerge(self.loc[lpid,'geometry'])
        
        #self[branch_id_attribute] = bids
        self = StreamNetwork(self,branch_id_attribute=branch_id_attribute,
                             attribute_excluded=attribute_excluded,
                             values_excluded=values_excluded)

        if out_vector_files is not None:
            
            base_file_path,extension = splitext(out_vector_files)
            
            if verbose:
                print("Writing dissolved branches ...")
            
            #for bid in tqdm(self.loc[:,branch_id_attribute],total=len(self),disable=(not verbose)):
                #out_vector_file = "{}_{}{}".format(base_file_path,bid,extension)
                
                #bid_indices = self.loc[:,branch_id_attribute] == bid
                #current_stream_network = StreamNetwork(self.loc[bid_indices,:])

                #current_stream_network.write(out_vector_file,index=False)
            self.write(out_vector_files,index=False)

        return(self)

    def derive_segments(self,inlets_attribute='inlet_id', reach_id_attribute='NHDPlusID'):
        pass


    def conflate_branches(self,target_stream_network,branch_id_attribute_left='branch_id',
                          branch_id_attribute_right='branch_id', left_order_attribute='order_',
                          right_order_attribute='order_',
                          crosswalk_attribute='crosswalk_id', verbose=False):
        
        # get unique stream orders
        orders = self.loc[:,right_order_attribute].unique()

        # make a dictionary of STR trees for every stream order
        trees = { o:STRtree(target_stream_network.geometry.tolist()) for o in orders }

        # make the crosswalk id attribute and set index
        self.loc[:,crosswalk_attribute] = [None] * len(self)
        self.set_index(branch_id_attribute_left,inplace=True)

        # loop through rows of self
        for idx,row in tqdm(self.iterrows(),total=len(self),disable=(not verbose),desc="Conflating branches"):

            g = row['geometry']
            o = row[left_order_attribute]
            
            tree = trees[o]

            # find nearest geom in target and its index
            matching_geom = tree.nearest(g)
            match_idx = target_stream_network.geometry == matching_geom
            
            # get the branch ids
            right_branch_id = int(target_stream_network.loc[match_idx,branch_id_attribute_left])
            left_branch_id = idx

            # save the target matching branch id 
            self.loc[left_branch_id,crosswalk_attribute] = right_branch_id

        # reset indices
        self.reset_index(inplace=True,drop=False)

        return(self)
        

    def explode_to_points(self,reach_id_attribute='NHDPlusID', sampling_size=None,
                          verbose=False):
        
        points_gdf = self.copy()
        points_gdf.reset_index(inplace=True,drop=True)

        all_exploded_points = [None] * len(points_gdf)
        for idx,row in tqdm(self.iterrows(),total=len(self),disable=(not verbose),desc='Exploding Points'):
            
            geom = row['geometry']
            
            exploded_points = [p for p in iter(geom.coords)]

            if sampling_size is None:
                exploded_points = MultiPoint(exploded_points)
            else:
                try:
                    exploded_points = MultiPoint( sample(exploded_points,sampling_size) )
                except ValueError:
                    exploded_points = MultiPoint( exploded_points )
            
            all_exploded_points[idx] = exploded_points

        points_gdf['geometry'] = all_exploded_points
        
        points_gdf = points_gdf.explode()
        points_gdf.reset_index(inplace=True,drop=True)

        return(points_gdf)


    @staticmethod
    def conflate_points(source_points,target_points,source_reach_id_attribute,target_reach_id_attribute,verbose=False):

        tree = STRtree(target_points.geometry.tolist())

        # find matching geometry
        matches_dict = dict.fromkeys(source_points.loc[:,source_reach_id_attribute].astype(int).tolist(),[])
        for idx,row in tqdm(source_points.iterrows(),total=len(source_points),disable=(not verbose),desc="Conflating points"):

            geom = row['geometry']
            nearest_target_point = tree.nearest(geom)
            match_idx = target_points.index[target_points.geometry == nearest_target_point].tolist()
            
            if len(match_idx) > 1:
                match_idx = match_idx[0]
            else:
                match_idx = match_idx[0]

            matched_id = int(target_points.loc[match_idx,target_reach_id_attribute])
            source_id = int(row[source_reach_id_attribute])
            matches_dict[source_id] = matches_dict[source_id] + [matched_id]

            #if len(matches_dict[source_id])>1:
            #    print(matches_dict[source_id])

        # get mode of matches
        if verbose:
            print("Finding mode of matches ...")

        for source_id,matches in matches_dict.items():
            majority = mode(matches).mode
            matches_dict[source_id] = majority[0]
        
        
        # make dataframe
        if verbose:
            print("Generating crosswalk table ...")

        crosswalk_table = pd.DataFrame.from_dict(matches_dict,orient='index',
                                                 columns=[target_reach_id_attribute])
        crosswalk_table.index.name = source_reach_id_attribute


        return(crosswalk_table)


    def clip(self,mask,keep_geom_type=False,verbose=False):

        if verbose:
            print("Clipping streams to mask ...")
        
        # load mask
        if isinstance(mask,gpd.GeoDataFrame):
            pass
        elif isinstance(mask,str):
            mask = gpd.read_file(mask)
        else:
            raise TypeError("mask needs to be GeoDataFame or path to vector file")


        branch_id_attribute = self.branch_id_attribute 
        attribute_excluded = self.attribute_excluded
        values_excluded = self.values_excluded
    
        self = StreamNetwork(
                             gpd.clip(self,mask,keep_geom_type).reset_index(drop=True),
                             branch_id_attribute=branch_id_attribute,
                             attribute_excluded=attribute_excluded,
                             values_excluded=values_excluded)

        return(self)


class StreamBranchPolygons(StreamNetwork):

    #branch_id_attribute = None
    #values_excluded = None
    #attribute_excluded = None

    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
     
   
    @classmethod
    def buffer_stream_branches(cls,stream_network,buffer_distance,verbose=True):

        """ Buffers stream branches by distance """

        if verbose:
            print("Buffering stream branches to polygons")

        buffer_distance = int(buffer_distance)

        # buffer lines
        new_bids = [None] *len(stream_network) ; new_geoms = new_bids.copy()
        i=0
   
        for _,row in tqdm(stream_network.iterrows(),disable=(not verbose),total=len(stream_network)):
            new_geoms[i] = row[stream_network.geometry.name].buffer(buffer_distance)
            new_bids[i] = i + 1
            i += 1
        
        # create polys gpd
        polys = stream_network.copy()

        # assign to StreamBranchPolys
        #polys[stream_network.branch_id_attribute] = new_bids
        polys[stream_network.geom_name] = new_geoms
        polys.set_geometry(stream_network.geom_name)
        
        # assign class and attributes
        polys = cls(polys,branch_id_attribute=stream_network.branch_id_attribute,
                    attribute_excluded=stream_network.attribute_excluded,
                    values_excluded=stream_network.values_excluded)

        return(polys)


    def query_vectors_by_branch(self,vector,out_filename_template=None,vector_layer=None):
        
        # load vaas
        if isinstance(vector,str):
            vector_filename = vector
            vector = gpd.read_file(vector_filename,layer=vector_layer)
        elif isinstance(vector,gpd.GeoDataFrame):
            pass
        else:
            raise TypeError('Pass vector argument as filepath or GeoDataframe')

        out_files = [None] * len(self)
        
        for i,bid in enumerate(self.loc[:,self.branch_id_attribute]):
            out_files[i] = vector.loc[vector.loc[:,self.branch_id_attribute] == bid,:]

            if (out_filename_template is not None) & (not out_files[i].empty):
                base,ext = out_filename_template.split('.')
                out_filename = base + "_{}.".format(bid) + ext
                
                StreamNetwork.write(out_files[i],out_filename)

        return(out_files)


    def clip(self,to_clip,out_filename_template=None):

        """ Clips a raster or vector to the stream branch polygons """

        fileType = "raster" #default
        
        # load raster or vector file
        if isinstance(to_clip,DatasetReader): #if already rasterio dataset
            pass
        elif isinstance(to_clip,str): # if a string

            try: # tries to open as rasterio first then geopanda
                to_clip = rasterio.open(to_clip,'r')
            except rasterio.errors.RasterioIOError:
                try:
                    to_clip = gpd.read_file(to_clip)
                    fileType = "vector"
                except DriverError: 
                    raise IOError("{} file not found".format(to_clip))

        elif isinstance(to_clip,gpd.GeoDataFrame): # if a geopanda dataframe
            fileType = "vector"
        else:
            raise TypeError("Pass rasterio dataset,geopandas GeoDataFrame, or filepath to raster or vector file")

        return_list = [] # list to return rasterio objects or gdf's

        if fileType == "raster":
            buffered_meta = to_clip.meta.copy()
            buffered_meta.update(blockxsize=256, blockysize=256, tiled=True)

            for i,row in self.iterrows():
                buffered_array,buffered_transform = mask(to_clip,[row[self.geom_name]],crop=True)

                buffered_meta.update(height = buffered_array.shape[1],
                                     width = buffered_array.shape[2],
                                     transform = buffered_transform
                                    )

                # write out files
                if out_filename_template is not None:
                    branch_id = row[self.branch_id_attribute]

                    base,ext = out_filename_template.split('.')
                    out_filename = base + "_{}.".format(branch_id) + ext
                    
                    with rasterio.open(out_filename,'w',**buffered_meta) as out:
                        out.write(buffered_array)
            
                # return files in list
                return_list += [out]

            out.close()

        if fileType == "vector":
            for i,row in self.iterrows():
                branch_id = row[self.branch_id_attribute]
                out = gpd.clip(to_clip,row[self.geom_name],keep_geom_type=True)
                return_list += [out]
                
                if (out_filename_template is not None) & (not out.empty):
                    base,ext = out_filename_template.split('.')
                    out_filename = base + "_{}.".format(branch_id) + ext
                    StreamNetwork.write(out,out_filename)
        
        return(return_list)

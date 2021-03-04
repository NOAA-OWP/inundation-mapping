#!/usr/bin/env python3

import geopandas as gpd
import rasterio
from rasterio.mask import mask
from rasterio.io import DatasetReader
from os.path import splitext
from fiona.errors import DriverError
from collections import deque
import numpy as np
from tqdm import tqdm

class StreamNetwork(gpd.GeoDataFrame):

    geom_name = 'geometry' # geometry attribute name
    branch_id_attribute = None # branch id attribute name
    values_excluded = None
    attribute_excluded = None

    def __init__(self,*args,**kwargs):

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
                               outlet_attribute='outlet_id',
                               branch_id_attribute='branchID',
                               reach_id_attribute='NHDPlusID',
                               comparison_attributes='StreamOrde',
                               comparison_function=max,
                               verbose=False):

        """ Derives stream branches """
        if verbose:
            print("Deriving stream branches ..")

        # checks inputs
        allowed_comparison_function = {max,min}
        if comparison_function not in allowed_comparison_function:
            raise ValueError(f"Only {allowed_comparison_function} comparison functions allowed")

        # sets index of stream branches as reach id attribute
        if self.index.name != reach_id_attribute:
            self.set_index(reach_id_attribute,drop=True,inplace=True)

        # initialize empty queue, visited set, branch attribute column, and all toNodes set
        Q = deque()
        visited = set()
        self[branch_id_attribute] = [-1] * len(self)
        all_toNodes = set(self[toNode_attribute].unique())

        # add outlets to queue, visited set. Assign branch id to outlets too
        bid = 1
        for reach_id,row in self.iterrows():

            oa = row[outlet_attribute]

            if oa >= 0:
                self.loc[reach_id,branch_id_attribute] = bid
                bid += 1
                
                Q.append(reach_id)
                visited.add(reach_id)

        # alternative means of adding outlets to queue, to visited, and assigning branch id's to outlets
        """
        outlet_boolean_mask = self[outlet_attribue] >= 0
        outlet_reach_ids = self.index[outlet_boolean_mask].tolist()
        self.loc[outlet_reach_ids,branch_id_attribute] = range(1,len(outlet_reach_ids)+1)
        for rid in outlet_reach_ids:
            Q.append(rid)
            visited.add(rid)
        """

        # breath-first traversal
        # while queue contains reaches
        while Q:

            # pop current reach id from queue
            current_reach_id = Q.popleft()

            # get current reach stream order and branch id
            current_reach_comparison_value = self.at[current_reach_id,comparison_attributes]
            current_reach_branch_id = self.at[current_reach_id,branch_id_attribute]

            # get from Node of current reach
            current_fromNode = self.at[current_reach_id,fromNode_attribute]

            # identify upstreams by finding if fromNode exists in set of all toNodes
            if current_fromNode in all_toNodes:
                
                # get boolean mask of upstream reaches and their reach id's
                upstream_bool_mask = self[toNode_attribute] == current_fromNode
                upstream_ids = self.index[upstream_bool_mask].tolist()

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
                    
                    for usrcv,nvus in zip(upstream_reaches_compare_values,not_visited_upstream_ids):
                        if usrcv == matching_value:
                            self.at[nvus,branch_id_attribute] = current_reach_branch_id
                        else:
                            self.at[nvus,branch_id_attribute] = bid
                            bid += 1

        return(self)


    def get_arbolate_sum(self,arbolate_sum_attribute='arbolate_sum',inlets_attribute='inlet_id',
                         reach_id_attribute='NHDPlusID',toNode_attribute='toNodes',
                         fromNode_attribute='fromNodes',length_conversion_factor_to_km = 0.001,
                         verbose=False
                        ):
        
        if verbose:
            print("Deriving arbolate sum ...")

        # sets index of stream branches as reach id attribute
        if self.index.name != reach_id_attribute:
            self.set_index(reach_id_attribute,drop=True,inplace=True)

        # initialize queue, visited set, with inlet reach ids
        inlet_reach_ids = self.index[self[inlets_attribute] >= 0].tolist()
        S = deque(inlet_reach_ids)
        visited = set()

        # initialize arbolate sum, make length km column, make all from nodes set
        self[arbolate_sum_attribute] = self.geometry.length * length_conversion_factor_to_km 
        all_fromNodes = set(self[fromNode_attribute].unique())
        
        # depth-first traversal
        # while stack contains reaches
        while S:

            # pop current reach id from queue
            current_reach_id = S.pop()
            
            # current arbolate sum
            current_reach_arbolate_sum = self.at[current_reach_id,arbolate_sum_attribute]

            # if current reach id is not visited, save arbolate sum, and mark as visited
            if current_reach_id not in visited:
                arbolate_sum_of_last_non_visited_reach = self.loc[current_reach_id,arbolate_sum_attribute]
                visited.add(current_reach_id)

            # get to Node of current reach
            current_toNode = self.at[current_reach_id,toNode_attribute]
            
            # identify downstreams by finding if toNode exists in set of all fromNodes
            if current_toNode in all_fromNodes:
                
                # get boolean mask of downstream reaches and their reach id's
                downstream_bool_mask = self[fromNode_attribute] == current_toNode
                downstream_ids = self.index[downstream_bool_mask].tolist()
                
                for ds in downstream_ids:
                    
                    # append downstream to stack
                    S.append(ds)

                    # if not visited, add current reach arbolate sum, else add the arbolate sum of last non-visited reach
                    if ds not in visited:
                        self.loc[ds,arbolate_sum_attribute] += current_reach_arbolate_sum
                    else:
                        self.loc[ds,arbolate_sum_attribute] += arbolate_sum_of_last_non_visited_reach
                    
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

        # save branch ids
        #print(self.columns)
        #self.sort_values(axis=1,by=branch_id_attribute,inplace=True)
        #bids = self.loc[:,branch_id_attribute].unique().tolist()

        # dissolve lines
        self['bids_temp'] = self.loc[:,branch_id_attribute].copy()
        self = self.dissolve(by=branch_id_attribute)
        self.rename(columns={'bids_temp' : branch_id_attribute},inplace=True)
        
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

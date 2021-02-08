#!/usr/bin/env python3

from stream_branches import StreamNetwork
from stream_branches import StreamBranchPolygons
import argparse


if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Identifies stream branches, generates branch polygons, and clips raster to polygons')
    parser.add_argument('-u','--huc8', help='to-do', required=True)
    #parser.add_argument('-s','--streams', help='to-do', required=False)
    #parser.add_argument('-b','--branches', help='to-do', required=False)
    #parser.add_argument('-d','--buffer-distance', help='to-do', required=False)
    #parser.add_argument('-r','--raster', help='to-do', required=False)
    #parser.add_argument('-v','--verbose', help='to-do', required=False,action='store_true')

    # extract to dictionary
    args = vars(parser.parse_args())

    from os.path import join
    huc8 = args["huc8"] ; huc4 = huc8[0:4]
    input_data_dir = "/data/outputs/slope_1eddd72_dem_all_mannings_6/{}/".format(huc8) #"/data/temp/stream_branches"
    output_data_dir = "/data/temp/stream_branches/large_test/{}".format(huc8)
    streams_file = join(output_data_dir,"NHDPlusBurnLineEvent_subset.gpkg")#'nhdplus_burnlines_subset.gpkg')
    branches_file = join("/data/inputs/nhdplus_vectors/{}/".format(huc4),'NHDPlusFlowLineVAA{}.gpkg'.format(huc4))
    buffer_distance = 1000
    to_clip_raster_file = join(input_data_dir,'dem_meters.tif')
    to_clip_vector_file = join(input_data_dir,'NHDPlusBurnLineEvent_subset.gpkg')

    stream_network_processed_file = join(output_data_dir,'stream_network_processed.gpkg')
    stream_polygons_file = join(output_data_dir,'stream_branch_polygons.gpkg')
    clipped_raster_files = join(output_data_dir,'test_{}.tif')
    clipped_vector_files = join(output_data_dir,'test_{}.gpkg')
    
    # load file
    print('Loading file')
    stream_network = StreamNetwork.from_file(streams_file)

    # merge stream branch attributes
    print('Merge attributes')
    stream_network = stream_network.merge_stream_branches(stream_branch_dataset=branches_file,
                                                          on='NHDPlusID',
                                                          attributes=['StreamOrde','ToNode','FromNode'],
                                                          branch_id_attribute = "LevelPathI",
                                                          stream_branch_layer_name=None
                                                         )
    
    # derive inlets and outlets
    print('Derive inlets and outlets')
    stream_network = stream_network.derive_inlets(toNode_attribute='ToNode',
                                                  fromNode_attribute='FromNode',
                                                  inlets_attribute='inlet_id'
                                                 )
    stream_network = stream_network.derive_outlets(toNode_attribute='ToNode',
                                                   fromNode_attribute='FromNode',
                                                   outlets_attribute='outlet_id'
                                                  )
    
    # derive arbolate sum
    
    print("Calculate arbolate sum")
    stream_network = stream_network.get_arbolate_sum(
                                                 arbolate_sum_attribute='arbolate_sum',inlets_attribute='inlet_id',
                                                 reach_id_attribute='NHDPlusID',toNode_attribute='ToNode',
                                                 fromNode_attribute='FromNode',length_conversion_factor_to_km=0.001
                                                     )

    # derive stream newtork branches
    print("Derive stream branches")
    stream_network = stream_network.derive_stream_branches(
                                        toNode_attribute='ToNode',fromNode_attribute='FromNode',
                                        outlet_attribute='outlet_id',branch_id_attribute='branchID',
                                        reach_id_attribute='NHDPlusID',comparison_attributes='arbolate_sum',
                                        comparison_function=max
                                                          )

    # write stream network with derived stream branches
    print("Write stream branches")
    stream_network.write(stream_network_processed_file)
    
    # dissolving
    print("Dissolve stream network by branch")
    stream_network = stream_network.dissolve_by_branch(branch_id_attribute='branchID',attribute_excluded='StreamOrde',values_excluded=[1,2],out_vector_file_template=None)

    # make stream polygons
    print("Buffer stream branches to polygons")
    stream_polys = StreamBranchPolygons.buffer_stream_branches(stream_network,buffer_distance=buffer_distance)
    
    # write polygons
    print("Write polygons")
    stream_polys.write(stream_polygons_file)

    # clip rasters to polygons
    print("Clip raster to polygons")
    out_rasters = stream_polys.clip(to_clip_raster_file,clipped_raster_files)
    
    # clip rasters to polygons
    print("Clip vector to polygons")
    out_vectors = stream_polys.clip(to_clip_vector_file,clipped_vector_files)

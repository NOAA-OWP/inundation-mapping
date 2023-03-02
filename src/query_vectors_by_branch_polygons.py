#!/usr/bin/env python3


from stream_branches import StreamBranchPolygons
import argparse
from tqdm import tqdm


if __name__ == '__main__':

    # parse arguments
    parser = argparse.ArgumentParser(description='Query vectors by unique attribute values')
    parser.add_argument('-a','--vector-attributes', help='Vector with unique attributs', required=True,default=None)
    parser.add_argument('-d','--branch-ids', help='Branch ID value', required=True,default=None, nargs='+')
    parser.add_argument('-i','--attribute-id', help='Attribute Name', required=True,default=None)
    parser.add_argument('-s','--subset-vectors', help='Vector file names to query by attribute', required=False,default=None,nargs="+")
    parser.add_argument('-o','--out-files', help='Vector filenames to write to after query', required=False,default=None,nargs="+")
    parser.add_argument('-v','--verbose', help='Verbose printing', required=False,default=None,action='store_true')

    # extract to dictionary
    args = vars(parser.parse_args())

    attributes_vector_file, branch_ids ,attribute, subset_vectors, out_files, verbose = args["vector_attributes"], args["branch_ids"],args["attribute_id"], args["subset_vectors"], args["out_files"], args["verbose"]
    
    # load file
    #stream_polys = StreamBranchPolygons.from_file( filename=attributes_vector_file, 
    #                                               branch_id_attribute=attribute,
    #                                               values_excluded=None,attribute_excluded=None, verbose = verbose)
    
    for subset_vector,out_file in tqdm(zip(subset_vectors,out_files),disable=(not verbose),
                                       total=len(subset_vectors),
                                       desc="Query vectors"):

        #if verbose:
            #print("Query \'{}\' by attribute in \'{}\' ...".format(out_file.split('/')[-1].split('.')[0],
            #                                                   attributes_vector_file.split('/')[-1].split('.')[0]))
        StreamBranchPolygons.query_vectors_by_branch(subset_vector,
                                                     branch_ids=branch_ids,
                                                     branch_id_attribute=attribute,
                                                     out_filename_template=out_file)

